"""Unit tests for the NAM Tools MCP server.

Tests tool dispatch, LoadRun state management, _get_config defaults,
_trigger_wiki_load subprocess handling, API helper functions, and all
new query/monitoring/task/document tools — all without cluster access.

Run:
    eval "$(pyenv init -)" && pyenv activate nam && \
    PYTHONPATH=./nam python -m pytest scripts/mcp/tests/test_nam_tools.py -v
"""
from __future__ import annotations

import json
import os
import sys
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock, patch, Mock
from http.client import HTTPResponse
from io import BytesIO

import pytest

# Ensure scripts/ and scripts/mcp/ are importable
SCRIPTS_DIR = str(Path(__file__).resolve().parents[2])
REPO_ROOT = str(Path(__file__).resolve().parents[3])
sys.path.insert(0, SCRIPTS_DIR)
sys.path.insert(0, os.path.join(REPO_ROOT, "nam"))
# Add scripts/mcp to path so we can import nam_tools directly
sys.path.insert(0, os.path.join(SCRIPTS_DIR, "mcp"))

from mcp.types import TextContent

# Import the module under test
import nam_tools
from nam_tools import (
    LoadRun,
    _active_runs,
    _get_config,
    _trigger_wiki_load,
    _get_auth_token,
    _api_get,
    _api_post,
    _json_result,
    _error_result,
    _token_cache,
    call_tool,
    list_tools,
    PIPELINE_TOOLS,
    QUERY_TOOLS,
    MONITORING_TOOLS,
    TASK_TOOLS,
    DOCUMENT_TOOLS,
)
from lib.pipeline_monitor import MonitorConfig, MonitorResult, Sample


# ---------------------------------------------------------------------------
# _get_config
# ---------------------------------------------------------------------------

class TestGetConfig:
    def test_defaults(self):
        config = _get_config()
        assert config.query_url == os.getenv("NAM_QUERY_URL", "http://193.122.141.217:30800")
        assert config.cb_password == os.getenv("NAM_CB_PASSWORD", "password")
        assert config.min_cumulative_rate == 500.0
        assert config.max_head_latency_ms == 10.0
        assert config.warmup_s == 60.0

    def test_explicit_overrides(self):
        config = _get_config(
            query_url="http://localhost:9999",
            cb_password="mypass",
            min_rate=100.0,
            max_head_latency_ms=20.0,
        )
        assert config.query_url == "http://localhost:9999"
        assert config.cb_password == "mypass"
        assert config.min_cumulative_rate == 100.0
        assert config.max_head_latency_ms == 20.0

    def test_pods_split(self):
        config = _get_config(pods="pod-0,pod-1,pod-2")
        assert config.ingestor_pods == ["pod-0", "pod-1", "pod-2"]

    def test_env_fallback(self):
        with patch.dict(os.environ, {"NAM_QUERY_URL": "http://test:1234"}):
            config = _get_config()
            assert config.query_url == "http://test:1234"


# ---------------------------------------------------------------------------
# _trigger_wiki_load
# ---------------------------------------------------------------------------

class TestTriggerWikiLoad:
    @patch("nam_tools.subprocess")
    def test_success(self, mock_subprocess):
        mock_subprocess.run.return_value = MagicMock(
            returncode=0,
            stdout='{"status":"accepted","task_id":"wiki_load_123"}',
            stderr="",
        )
        result = _trigger_wiki_load("http://localhost:30800", "pw", 1000)
        assert result["status"] == "accepted"

    @patch("nam_tools.subprocess")
    def test_kubectl_failure(self, mock_subprocess):
        mock_subprocess.run.return_value = MagicMock(
            returncode=1,
            stdout="",
            stderr="connection refused",
        )
        result = _trigger_wiki_load("http://localhost:30800", "pw", 1000)
        assert "error" in result
        assert "connection refused" in result["error"]

    @patch("nam_tools.subprocess")
    def test_invalid_json(self, mock_subprocess):
        mock_subprocess.run.return_value = MagicMock(
            returncode=0,
            stdout="not json",
            stderr="",
        )
        result = _trigger_wiki_load("http://localhost:30800", "pw", 1000)
        assert "raw" in result
        assert result["raw"] == "not json"


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------

class TestAuthHelpers:
    def setup_method(self):
        """Reset token cache before each test."""
        _token_cache["token"] = None
        _token_cache["expires_at"] = 0

    @patch("nam_tools.urllib.request.urlopen")
    def test_get_auth_token_success(self, mock_urlopen):
        resp_data = json.dumps({"token": "test-jwt-123", "expires_in": 3600}).encode()
        mock_resp = MagicMock()
        mock_resp.read.return_value = resp_data
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        token = _get_auth_token("http://test:30800")
        assert token == "test-jwt-123"
        assert _token_cache["token"] == "test-jwt-123"

    @patch("nam_tools.urllib.request.urlopen")
    def test_get_auth_token_cached(self, mock_urlopen):
        _token_cache["token"] = "cached-token"
        _token_cache["expires_at"] = time.time() + 3600

        token = _get_auth_token("http://test:30800")
        assert token == "cached-token"
        # Should NOT have called urlopen since token is cached
        mock_urlopen.assert_not_called()

    @patch("nam_tools.urllib.request.urlopen")
    def test_get_auth_token_expired_refreshes(self, mock_urlopen):
        _token_cache["token"] = "old-token"
        _token_cache["expires_at"] = time.time() - 100  # expired

        resp_data = json.dumps({"token": "new-token", "expires_in": 3600}).encode()
        mock_resp = MagicMock()
        mock_resp.read.return_value = resp_data
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        token = _get_auth_token("http://test:30800")
        assert token == "new-token"
        mock_urlopen.assert_called_once()

    @patch("nam_tools.urllib.request.urlopen")
    def test_get_auth_token_failure_returns_empty(self, mock_urlopen):
        mock_urlopen.side_effect = Exception("connection refused")
        token = _get_auth_token("http://test:30800")
        assert token == ""


class TestApiHelpers:
    def test_json_result(self):
        result = _json_result({"key": "value"})
        assert len(result) == 1
        assert isinstance(result[0], TextContent)
        data = json.loads(result[0].text)
        assert data["key"] == "value"

    def test_error_result(self):
        result = _error_result("something went wrong")
        assert len(result) == 1
        data = json.loads(result[0].text)
        assert data["error"] == "something went wrong"


# ---------------------------------------------------------------------------
# LoadRun
# ---------------------------------------------------------------------------

def _make_sample(**overrides) -> Sample:
    defaults = dict(
        timestamp=1000.0, elapsed_s=120.0, sample_num=4,
        enqueued=10000, dequeued=10000, errors=0, backlog=0,
        cumulative_rate=600.0, interval_rate=600.0,
        main_items=10000, nam_items=500000,
        head_latencies={"nlp": 3.0, "ontology": 5.0},
        head_counts={"entity": 1000, "attribute": 1000, "affordance": 1000, "context": 1000},
        addressing_rate=600.0, addressing_bundle_ms=2.0, addressing_build_ms=1.5,
        write_queue=0, write_inflight=0, circuit_state="closed",
        ingestor_vbuckets={"nam-ingest-0": 512, "nam-ingest-1": 512},
        ingestor_published={"nam-ingest-0": 5000, "nam-ingest-1": 5000},
        lmdb_entries={"entity": 10000, "attribute": 8000},
    )
    defaults.update(overrides)
    return Sample(**defaults)


class TestLoadRun:
    def test_status_dict_no_result(self):
        config = MonitorConfig(query_url="http://test", cb_password="pw")
        # Bypass __init__ since PipelineMonitor tries to connect
        run = object.__new__(LoadRun)
        run.run_id = "test-1"
        run.config = config
        run.expected_records = 100
        run.stop_event = threading.Event()
        run.result = None
        run.samples = []
        run.started_at = time.time() - 10
        run._thread = None

        status = run.status_dict()
        assert status["run_id"] == "test-1"
        assert status["running"] is False
        assert status["latest"] is None
        assert status["samples_collected"] == 0

    def test_status_dict_with_result(self):
        sample = _make_sample(enqueued=500, dequeued=450, errors=0)
        config = MonitorConfig(query_url="http://test", cb_password="pw")
        run = object.__new__(LoadRun)
        run.run_id = "test-2"
        run.config = config
        run.expected_records = 500
        run.stop_event = threading.Event()
        run.result = MonitorResult(failed=False, samples=[sample])
        run.samples = [sample]
        run.started_at = time.time() - 60
        run._thread = None

        status = run.status_dict()
        assert status["latest"]["enqueued"] == 500
        assert status["latest"]["dequeued"] == 450
        assert status["samples_collected"] == 1
        assert status["failed"] is False

    def test_is_running_false_when_no_thread(self):
        run = object.__new__(LoadRun)
        run._thread = None
        assert run.is_running is False

    def test_latest_sample_returns_last(self):
        s1 = _make_sample(enqueued=100)
        s2 = _make_sample(enqueued=200)
        run = object.__new__(LoadRun)
        run.result = MonitorResult(failed=False, samples=[s1, s2])
        assert run.latest_sample.enqueued == 200


# ---------------------------------------------------------------------------
# call_tool dispatch (async) — pipeline tools
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestCallToolPipeline:
    async def test_unknown_tool(self):
        result = await call_tool("nonexistent_tool", {})
        assert len(result) == 1
        data = json.loads(result[0].text)
        assert "error" in data
        assert "Unknown tool" in data["error"]

    async def test_wiki_load_status_no_active_run(self):
        _active_runs.clear()
        result = await call_tool("wiki_load_status", {"run_id": "doesnt-exist"})
        data = json.loads(result[0].text)
        assert "error" in data
        assert "No active run" in data["error"]

    async def test_wiki_load_stop_no_active_run(self):
        _active_runs.clear()
        result = await call_tool("wiki_load_stop", {})
        data = json.loads(result[0].text)
        assert "error" in data

    async def test_wiki_load_status_returns_latest(self):
        _active_runs.clear()
        sample = _make_sample(enqueued=1000, dequeued=900)

        run = object.__new__(LoadRun)
        run.run_id = "test-status"
        run.config = MonitorConfig(query_url="http://test", cb_password="pw")
        run.expected_records = 1000
        run.stop_event = threading.Event()
        run.result = MonitorResult(failed=False, samples=[sample])
        run.samples = [sample]
        run.started_at = time.time() - 30
        run._thread = None

        _active_runs["test-status"] = run

        result = await call_tool("wiki_load_status", {})
        data = json.loads(result[0].text)
        assert data["run_id"] == "test-status"
        assert data["latest"]["enqueued"] == 1000
        assert data["latest"]["dequeued"] == 900

        _active_runs.clear()

    async def test_wiki_load_stop_cleans_up(self):
        _active_runs.clear()
        sample = _make_sample()

        run = object.__new__(LoadRun)
        run.run_id = "test-stop"
        run.config = MonitorConfig(query_url="http://test", cb_password="pw")
        run.expected_records = 100
        run.stop_event = threading.Event()
        run.result = MonitorResult(failed=False, samples=[sample])
        run.samples = [sample]
        run.started_at = time.time() - 10
        run._thread = None

        _active_runs["test-stop"] = run

        result = await call_tool("wiki_load_stop", {"run_id": "test-stop"})
        data = json.loads(result[0].text)
        assert data["status"] == "stopped"
        assert data["final_metrics"]["enqueued"] == 10000
        assert "test-stop" not in _active_runs

    @patch("nam_tools.PipelineMonitor")
    async def test_preflight_check_clean(self, MockMonitor):
        mock_instance = MockMonitor.return_value
        mock_instance.preflight_check.return_value = []

        result = await call_tool("preflight_check", {
            "query_url": "http://test:30800",
            "cb_password": "pw",
        })
        data = json.loads(result[0].text)
        assert data["clean"] is True
        assert data["warnings"] == []

    @patch("nam_tools.PipelineMonitor")
    async def test_pipeline_metrics_success(self, MockMonitor):
        mock_instance = MockMonitor.return_value
        mock_instance._start_time = time.monotonic() - 1
        mock_instance.sample.return_value = _make_sample()
        MockMonitor.format_sample.return_value = "formatted line"

        result = await call_tool("pipeline_metrics", {
            "query_url": "http://test:30800",
        })
        data = json.loads(result[0].text)
        assert data["enqueued"] == 10000
        assert data["formatted"] == "formatted line"

    @patch("nam_tools.PipelineMonitor")
    async def test_pipeline_metrics_error(self, MockMonitor):
        mock_instance = MockMonitor.return_value
        mock_instance._start_time = time.monotonic() - 1
        mock_instance.sample.side_effect = Exception("connection refused")

        result = await call_tool("pipeline_metrics", {})
        data = json.loads(result[0].text)
        assert "error" in data
        assert "connection refused" in data["error"]


# ---------------------------------------------------------------------------
# call_tool dispatch — query tools (mocked HTTP)
# ---------------------------------------------------------------------------

def _mock_api_response(data: dict):
    """Create a mock urlopen context manager that returns JSON data."""
    mock_resp = MagicMock()
    mock_resp.read.return_value = json.dumps(data).encode()
    mock_resp.__enter__ = lambda s: s
    mock_resp.__exit__ = MagicMock(return_value=False)
    return mock_resp


@pytest.mark.asyncio
class TestCallToolQuery:
    @patch("nam_tools._api_post")
    async def test_query(self, mock_post):
        mock_post.return_value = {
            "ok": True,
            "mode": "EXPLORATORY",
            "affordance": None,
            "results": [{"title": "Earth", "id": "doc1"}],
            "addresses": [{"record_id": "doc1", "address_meta": {"entity": "abc123"}}],
            "truncated": False,
            "errors": [],
        }
        result = await call_tool("query", {"query": "earth", "fan_out": 2})
        data = json.loads(result[0].text)
        assert data["ok"] is True
        assert data["mode"] == "EXPLORATORY"
        assert data["result_count"] == 1
        mock_post.assert_called_once_with("/v1/query", {"query": "earth", "fan_out": 2})

    @patch("nam_tools._api_post")
    async def test_graph_walk(self, mock_post):
        mock_post.return_value = {
            "ok": True,
            "result_count": 50,
            "hops_used": 2,
            "edges_followed": 120,
            "entities_discovered": 8,
            "seed_keys": ["k1", "k2"],
            "keys": ["k1", "k2", "k3"],
            "nodes": [{"address_key": "k1", "entity_name": "math"}],
            "documents": {},
        }
        result = await call_tool("graph_walk", {"entity": "mathematic", "hop_depth": 2})
        data = json.loads(result[0].text)
        assert data["ok"] is True
        assert data["result_count"] == 50
        assert data["hops_used"] == 2
        mock_post.assert_called_once_with("/v1/graph/walk", {
            "entity": "mathematic",
            "hop_depth": 2,
        })

    @patch("nam_tools._api_post")
    async def test_graph_walk_with_resolve(self, mock_post):
        mock_post.return_value = {
            "ok": True, "result_count": 5, "hops_used": 1,
            "edges_followed": 10, "entities_discovered": 3,
            "seed_keys": [], "keys": [], "nodes": [],
            "documents": {"dp1": {"title": "Test"}},
        }
        result = await call_tool("graph_walk", {
            "entity": "dog",
            "resolve": True,
        })
        data = json.loads(result[0].text)
        assert data["documents"] == {"dp1": {"title": "Test"}}
        call_body = mock_post.call_args[0][1]
        assert call_body["resolve"] is True

    @patch("nam_tools._api_post")
    async def test_record_walk(self, mock_post):
        mock_post.return_value = {
            "ok": True,
            "record_count": 10,
            "entity_count": 3,
            "hops_used": 2,
            "seed_entity": "abc123",
            "records": ["dp1", "dp2"],
            "entities": [{"id": "abc123", "name": "math"}],
            "record_entity_map": {"dp1": ["abc123"]},
        }
        result = await call_tool("record_walk", {"entity": "math", "max_records": 25})
        data = json.loads(result[0].text)
        assert data["record_count"] == 10
        assert data["entity_count"] == 3
        mock_post.assert_called_once_with("/v1/graph/record-walk", {
            "entity": "math",
            "max_records": 25,
        })

    @patch("nam_tools._api_post")
    async def test_entity_documents(self, mock_post):
        mock_post.return_value = {
            "ok": True,
            "documents": {"dp1": {"title": "Math"}},
            "record_entity_map": {"dp1": ["abc123"]},
            "entity_count": 1,
            "document_count": 1,
        }
        result = await call_tool("entity_documents", {
            "entities": ["abc123"],
            "max_records": 10,
        })
        data = json.loads(result[0].text)
        assert data["ok"] is True
        assert data["document_count"] == 1

    @patch("nam_tools._api_get")
    async def test_graph_stats(self, mock_get):
        mock_get.return_value = {"ok": True, "total_keys": 1000}
        result = await call_tool("graph_stats", {})
        data = json.loads(result[0].text)
        assert data["total_keys"] == 1000
        mock_get.assert_called_once_with("/v1/graph/stats")


# ---------------------------------------------------------------------------
# call_tool dispatch — monitoring tools (mocked HTTP)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestCallToolMonitoring:
    @patch("nam_tools._api_get")
    async def test_system_status(self, mock_get):
        mock_get.return_value = {
            "timestamp": 1234567890,
            "buckets": [{"name": "main", "item_count": 500000}],
        }
        result = await call_tool("system_status", {})
        data = json.loads(result[0].text)
        assert data["timestamp"] == 1234567890
        mock_get.assert_called_once_with("/v1/admin/system-status")

    @patch("nam_tools._api_get")
    async def test_bucket_stats(self, mock_get):
        mock_get.return_value = {
            "timestamp": 1234567890,
            "buckets": [
                {"name": "main", "item_count": 500000, "disk_used": 300000000},
                {"name": "nam", "item_count": 13000000, "disk_used": 420000000},
            ],
        }
        result = await call_tool("bucket_stats", {})
        data = json.loads(result[0].text)
        assert len(data["buckets"]) == 2
        assert data["buckets"][0]["name"] == "main"

    @patch("nam_tools._api_get")
    async def test_encoder_heads(self, mock_get):
        mock_get.return_value = {
            "timestamp": 1234567890,
            "heads": {
                "entity": {"active_pods": 4, "avg_encode_ms": 5.2},
                "nlp": {"active_pods": 2, "avg_encode_ms": 2.6},
            },
        }
        result = await call_tool("encoder_heads", {})
        data = json.loads(result[0].text)
        assert "entity" in data["heads"]
        assert data["heads"]["nlp"]["avg_encode_ms"] == 2.6

    @patch("nam_tools._api_get")
    async def test_pipeline_errors(self, mock_get):
        mock_get.return_value = {
            "timestamp": 1234567890,
            "summary": {"total_errors": 0},
            "stages": {},
        }
        result = await call_tool("pipeline_errors", {})
        data = json.loads(result[0].text)
        assert data["summary"]["total_errors"] == 0

    @patch("nam_tools._api_get")
    async def test_time_series(self, mock_get):
        mock_get.return_value = [{"ts": 1234567890, "bkt_main_items": 500000}]
        result = await call_tool("time_series", {})
        data = json.loads(result[0].text)
        assert isinstance(data, list)
        assert data[0]["ts"] == 1234567890

    @patch("nam_tools._api_get")
    async def test_dcp_metrics(self, mock_get):
        mock_get.return_value = {
            "enqueued_total": 487536,
            "dequeued_total": 485936,
            "errors_total": 0,
            "nam_item_count": 13284322,
        }
        result = await call_tool("dcp_metrics", {})
        data = json.loads(result[0].text)
        assert data["enqueued_total"] == 487536
        assert data["errors_total"] == 0

    @patch("nam_tools._api_get")
    async def test_storage_stats(self, mock_get):
        mock_get.return_value = {"rocksdb": {"nam": {"sst_files": 42}}}
        result = await call_tool("storage_stats", {})
        data = json.loads(result[0].text)
        assert data["rocksdb"]["nam"]["sst_files"] == 42


# ---------------------------------------------------------------------------
# call_tool dispatch — task tools (mocked HTTP)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestCallToolTasks:
    @patch("nam_tools._api_get")
    async def test_task_list(self, mock_get):
        mock_get.return_value = {
            "wiki_load": {"status": "idle"},
            "flush": {"status": "idle"},
            "compaction": {"status": "idle"},
        }
        result = await call_tool("task_list", {})
        data = json.loads(result[0].text)
        assert "wiki_load" in data
        mock_get.assert_called_once_with("/v1/admin/tasks")

    @patch("nam_tools._api_get")
    async def test_task_status(self, mock_get):
        mock_get.return_value = {
            "task_type": "wiki_load",
            "status": "running",
            "config": {"limit": 500000},
        }
        result = await call_tool("task_status", {"task_type": "wiki_load"})
        data = json.loads(result[0].text)
        assert data["status"] == "running"
        mock_get.assert_called_once_with("/v1/admin/tasks/wiki_load")

    @patch("nam_tools._api_post")
    async def test_task_trigger(self, mock_post):
        mock_post.return_value = {"status": "triggered", "task_id": "wiki_load_123"}
        result = await call_tool("task_trigger", {
            "task_type": "wiki_load",
            "config": {"limit": 1000},
        })
        data = json.loads(result[0].text)
        assert data["status"] == "triggered"
        mock_post.assert_called_once_with("/v1/admin/tasks/wiki_load", {"limit": 1000})

    @patch("nam_tools._api_post")
    async def test_task_trigger_no_config(self, mock_post):
        mock_post.return_value = {"status": "triggered"}
        result = await call_tool("task_trigger", {"task_type": "flush"})
        data = json.loads(result[0].text)
        assert data["status"] == "triggered"
        mock_post.assert_called_once_with("/v1/admin/tasks/flush", {})

    @patch("nam_tools._api_get")
    async def test_task_history(self, mock_get):
        mock_get.return_value = {
            "task_type": "wiki_load",
            "history": [
                {"started_at": "2026-03-22T10:00:00", "status": "completed"},
            ],
        }
        result = await call_tool("task_history", {"task_type": "wiki_load"})
        data = json.loads(result[0].text)
        assert len(data["history"]) == 1
        mock_get.assert_called_once_with("/v1/admin/tasks/wiki_load/history")


# ---------------------------------------------------------------------------
# call_tool dispatch — document tools (mocked HTTP)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestCallToolDocuments:
    @patch("nam_tools._api_get")
    async def test_doc_list_default(self, mock_get):
        mock_get.return_value = {
            "rows": [{"id": "doc1", "preview": "{title, body}"}],
            "total_count": 500000,
            "bucket": "main",
        }
        result = await call_tool("doc_list", {"bucket": "main"})
        data = json.loads(result[0].text)
        assert data["total_count"] == 500000
        assert len(data["rows"]) == 1
        # Check path includes bucket and default limit
        call_path = mock_get.call_args[0][0]
        assert "/v1/admin/docs/main" in call_path
        assert "limit=20" in call_path

    @patch("nam_tools._api_get")
    async def test_doc_list_with_params(self, mock_get):
        mock_get.return_value = {"rows": [], "total_count": 0, "bucket": "nam"}
        result = await call_tool("doc_list", {
            "bucket": "nam",
            "skip": 10,
            "limit": 50,
            "start_key": "lca:",
        })
        data = json.loads(result[0].text)
        call_path = mock_get.call_args[0][0]
        assert "skip=10" in call_path
        assert "limit=50" in call_path
        assert "start_key=lca%3A" in call_path

    @patch("nam_tools._api_get")
    async def test_doc_list_limit_capped(self, mock_get):
        mock_get.return_value = {"rows": [], "total_count": 0}
        await call_tool("doc_list", {"bucket": "main", "limit": 500})
        call_path = mock_get.call_args[0][0]
        assert "limit=200" in call_path  # capped at 200

    @patch("nam_tools._api_get")
    async def test_doc_get(self, mock_get):
        mock_get.return_value = {
            "key": "doc1",
            "doc": {"title": "Earth", "body": "Third planet"},
        }
        result = await call_tool("doc_get", {"bucket": "main", "key": "doc1"})
        data = json.loads(result[0].text)
        assert data["doc"]["title"] == "Earth"
        mock_get.assert_called_once_with("/v1/admin/docs/main/doc1")


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestErrorHandling:
    @patch("nam_tools._api_get")
    async def test_http_error(self, mock_get):
        import urllib.error
        mock_get.side_effect = urllib.error.HTTPError(
            "http://test", 401, "Unauthorized", {}, BytesIO(b'{"detail":"bad token"}')
        )
        result = await call_tool("system_status", {})
        data = json.loads(result[0].text)
        assert "error" in data
        assert "401" in data["error"]

    @patch("nam_tools._api_get")
    async def test_connection_error(self, mock_get):
        import urllib.error
        mock_get.side_effect = urllib.error.URLError("Connection refused")
        result = await call_tool("system_status", {})
        data = json.loads(result[0].text)
        assert "error" in data
        assert "Connection failed" in data["error"]

    @patch("nam_tools._api_post")
    async def test_query_error(self, mock_post):
        mock_post.side_effect = Exception("timeout")
        result = await call_tool("query", {"query": "test"})
        data = json.loads(result[0].text)
        assert "error" in data
        assert "timeout" in data["error"]


# ---------------------------------------------------------------------------
# list_tools
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestListTools:
    async def test_returns_all_tools(self):
        tools = await list_tools()
        expected_count = len(PIPELINE_TOOLS) + len(QUERY_TOOLS) + len(MONITORING_TOOLS) + len(TASK_TOOLS) + len(DOCUMENT_TOOLS)
        assert len(tools) == expected_count

    async def test_tool_names_complete(self):
        tools = await list_tools()
        names = {t.name for t in tools}
        expected = {
            # Pipeline
            "wiki_load_start", "wiki_load_status", "wiki_load_stop",
            "pipeline_metrics", "preflight_check",
            # Query
            "query", "graph_walk", "record_walk", "entity_documents", "graph_stats",
            # Monitoring
            "system_status", "bucket_stats", "encoder_heads", "pipeline_errors",
            "time_series", "dcp_metrics", "storage_stats",
            # Tasks
            "task_list", "task_status", "task_trigger", "task_history",
            # Documents
            "doc_list", "doc_get",
        }
        assert names == expected

    async def test_all_tools_have_descriptions(self):
        tools = await list_tools()
        for t in tools:
            assert t.description, f"{t.name} has empty description"

    async def test_all_tools_have_input_schema(self):
        tools = await list_tools()
        for t in tools:
            assert t.inputSchema["type"] == "object"

    async def test_tool_count_by_category(self):
        assert len(PIPELINE_TOOLS) == 5
        assert len(QUERY_TOOLS) == 5
        assert len(MONITORING_TOOLS) == 7
        assert len(TASK_TOOLS) == 4
        assert len(DOCUMENT_TOOLS) == 2
