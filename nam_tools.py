#!/usr/bin/env python3
"""NAM Tools MCP Server.

Exposes the full NAM API surface to Claude Code via MCP tools.
Runs as a local stdio MCP server — started/managed by Claude Code.

Tool categories:
    Pipeline monitoring:  wiki_load_start, wiki_load_status, wiki_load_stop,
                         pipeline_metrics, preflight_check
    Query:               query, graph_walk, record_walk, entity_documents
    System monitoring:   system_status, bucket_stats, encoder_heads,
                         pipeline_errors, time_series
    Tasks:               task_list, task_status, task_trigger, task_history
    Documents:           doc_list, doc_get

Standalone usage (pip install mcp):
    python nam_tools.py

Registration in .mcp.json:
    "mcpServers": {
        "nam-tools": {
            "command": "python",
            "args": ["nam_tools.py"],
            "cwd": "<path-to-nam-mcp>"
        }
    }

Pipeline monitoring tools (wiki_load_start/status/stop, preflight_check) require
the full NAM repo's pipeline_monitor library. They are disabled when running
standalone. All other tools (18 of 23) work with just NAM_QUERY_URL.
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request
import urllib.error
from dataclasses import asdict
from typing import Any, Optional

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import TextContent, Tool

# Pipeline monitor is optional — available when running from within the full NAM repo.
# Without it, the 5 pipeline tools (wiki_load_*, preflight_check) are disabled;
# the other 18 tools work standalone.
_HAS_PIPELINE_MONITOR = False
try:
    # When installed as a submodule inside the NAM repo, scripts/lib is on the path
    _repo_root = os.environ.get(
        "NAM_REPO_ROOT",
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    )
    for _p in [os.path.join(_repo_root, "scripts"), os.path.join(_repo_root, "nam")]:
        if _p not in sys.path:
            sys.path.insert(0, _p)
    from lib.pipeline_monitor import (  # type: ignore[import-not-found]
        MonitorConfig,
        MonitorResult,
        PipelineMonitor,
        Sample,
    )
    _HAS_PIPELINE_MONITOR = True
except ImportError:
    MonitorConfig = None  # type: ignore[assignment,misc]
    MonitorResult = None  # type: ignore[assignment,misc]
    PipelineMonitor = None  # type: ignore[assignment,misc]
    Sample = None  # type: ignore[assignment,misc]

# All logging to stderr (stdout is reserved for MCP protocol)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("nam-tools")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

QUERY_URL = os.getenv("NAM_QUERY_URL", "http://193.122.141.217:30800")
QUERY_USERNAME = os.getenv("NAM_QUERY_USERNAME", "admin")
QUERY_PASSWORD = os.getenv("NAM_QUERY_PASSWORD", os.getenv("NAM_CB_PASSWORD", "password"))

# ---------------------------------------------------------------------------
# Auth helper — JWT token cache
# ---------------------------------------------------------------------------

_token_cache: dict[str, Any] = {"token": None, "expires_at": 0}
_token_lock = threading.Lock()


def _get_auth_token(query_url: str = "") -> str:
    """Get a valid JWT token, refreshing if expired."""
    url = query_url or QUERY_URL
    with _token_lock:
        if _token_cache["token"] and time.time() < _token_cache["expires_at"] - 30:
            return _token_cache["token"]

        login_url = f"{url}/v1/auth/login"
        payload = json.dumps({
            "username": QUERY_USERNAME,
            "password": QUERY_PASSWORD,
        }).encode()
        req = urllib.request.Request(
            login_url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read())
            _token_cache["token"] = data["token"]
            _token_cache["expires_at"] = time.time() + data.get("expires_in", 3600)
            return _token_cache["token"]
        except Exception as e:
            logger.warning("auth.login_failed: %s (continuing without auth)", e)
            return ""


def _api_get(path: str, query_url: str = "", timeout: int = 15) -> dict:
    """Make an authenticated GET request to the query service."""
    url = (query_url or QUERY_URL) + path
    token = _get_auth_token(query_url)
    req = urllib.request.Request(url, method="GET")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


def _api_post(path: str, body: dict | None = None, query_url: str = "",
              timeout: int = 15) -> dict:
    """Make an authenticated POST request to the query service."""
    url = (query_url or QUERY_URL) + path
    token = _get_auth_token(query_url)
    payload = json.dumps(body or {}).encode()
    req = urllib.request.Request(
        url, data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


def _api_delete(path: str, query_url: str = "", timeout: int = 15) -> dict:
    """Make an authenticated DELETE request to the query service."""
    url = (query_url or QUERY_URL) + path
    token = _get_auth_token(query_url)
    req = urllib.request.Request(url, method="DELETE")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
        return json.loads(raw) if raw else {"ok": True}


# ---------------------------------------------------------------------------
# Global state for async wiki load runs
# ---------------------------------------------------------------------------

class LoadRun:
    """Tracks an in-progress monitored wiki load.

    Requires pipeline_monitor (available when running from within the NAM repo).
    """

    def __init__(self, run_id: str, config: "MonitorConfig", expected_records: int):  # type: ignore[name-defined]
        self.run_id = run_id
        self.config = config
        self.expected_records = expected_records
        self.monitor = PipelineMonitor(config)  # type: ignore[misc]
        self.stop_event = threading.Event()
        self.result: MonitorResult | None = None
        self.samples: list[Sample] = []
        self.started_at = time.time()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, daemon=True, name=f"load-{self.run_id}")
        self._thread.start()

    def _run(self) -> None:
        try:
            self.result = self.monitor.run(self.stop_event, expected_records=self.expected_records)
            self.samples = self.result.samples
        except Exception as e:
            logger.error("load_run.failed run=%s: %s", self.run_id, e)
            self.result = MonitorResult(failed=True, failure_reason=str(e))

    def stop(self) -> None:
        self.stop_event.set()
        if self._thread:
            self._thread.join(timeout=10)

    @property
    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    @property
    def latest_sample(self) -> Sample | None:
        if self.result and self.result.samples:
            return self.result.samples[-1]
        return None

    def status_dict(self) -> dict:
        latest = self.latest_sample
        return {
            "run_id": self.run_id,
            "running": self.is_running,
            "elapsed_s": round(time.time() - self.started_at, 1),
            "expected_records": self.expected_records,
            "samples_collected": len(self.result.samples) if self.result else 0,
            "failed": self.result.failed if self.result else False,
            "failure_reason": self.result.failure_reason if self.result else "",
            "violations": len(self.result.violations) if self.result else 0,
            "latest": {
                "enqueued": latest.enqueued,
                "dequeued": latest.dequeued,
                "errors": latest.errors,
                "backlog": latest.backlog,
                "cumulative_rate": round(latest.cumulative_rate, 1),
                "interval_rate": round(latest.interval_rate, 1),
                "main_items": latest.main_items,
                "nam_items": latest.nam_items,
                "head_latencies": latest.head_latencies,
                "head_counts": latest.head_counts,
                "addressing_rate": round(latest.addressing_rate, 1),
                "circuit_state": latest.circuit_state,
                "ingestor_vbuckets": latest.ingestor_vbuckets,
                "lmdb_entries": latest.lmdb_entries,
            } if latest else None,
        }


# Active runs (keyed by run_id)
_active_runs: dict[str, LoadRun] = {}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_config(
    query_url: str = "",
    cb_password: str = "",
    kubeconfig: str = "",
    namespace: str = "nam",
    pods: str = "nam-ingest-0,nam-ingest-1",
    warmup_s: float = 60.0,
    min_rate: float = 500.0,
    max_head_latency_ms: float = 10.0,
) -> MonitorConfig:
    return MonitorConfig(
        query_url=query_url or os.getenv("NAM_QUERY_URL", "http://193.122.141.217:30800"),
        cb_password=cb_password or os.getenv("NAM_CB_PASSWORD", "password"),
        kubeconfig=kubeconfig or os.getenv("KUBECONFIG", os.path.expanduser("~/.kube/nam-config")),
        namespace=namespace,
        ingestor_pods=pods.split(","),
        warmup_s=warmup_s,
        min_cumulative_rate=min_rate,
        max_head_latency_ms=max_head_latency_ms,
    )


def _trigger_wiki_load(query_url: str, password: str, records: int) -> dict:
    """Trigger wiki load via supervisor task API."""
    kubeconfig = os.getenv("KUBECONFIG", os.path.expanduser("~/.kube/nam-config"))
    cmd = ["kubectl"]
    if kubeconfig:
        cmd.extend(["--kubeconfig", kubeconfig])
    cmd.extend([
        "-n", "nam", "exec", "deploy/nam-supervisor", "--",
        "curl", "-s", "--max-time", "10",
        "-X", "POST", "http://localhost:8080/v1/admin/tasks/wiki_load",
        "-H", "Content-Type: application/json",
        "-d", json.dumps({"limit": records}),
    ])
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        return {"error": f"kubectl failed: {result.stderr}"}
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        return {"raw": result.stdout, "stderr": result.stderr}


def _json_result(data: Any) -> list[TextContent]:
    """Wrap data as MCP TextContent JSON response."""
    return [TextContent(type="text", text=json.dumps(data, indent=2, default=str))]


def _error_result(msg: str) -> list[TextContent]:
    """Wrap error as MCP TextContent JSON response."""
    return [TextContent(type="text", text=json.dumps({"error": msg}))]


# ---------------------------------------------------------------------------
# Tool definitions
# ---------------------------------------------------------------------------

# -- Pipeline monitoring tools --
PIPELINE_TOOLS = [
    Tool(
        name="wiki_load_start",
        description=(
            "Start a monitored wiki data load. Triggers wiki_load via supervisor "
            "and starts pipeline monitoring in the background. Returns a run_id "
            "for tracking. Use wiki_load_status to check progress."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "records": {"type": "integer", "description": "Number of wiki records to load", "default": 50000},
                "query_url": {"type": "string", "description": "Query service URL"},
                "cb_password": {"type": "string", "description": "Data service password"},
                "kubeconfig": {"type": "string", "description": "Path to kubeconfig"},
                "min_rate": {"type": "number", "description": "Minimum cumulative rec/s threshold", "default": 500.0},
                "max_head_latency_ms": {"type": "number", "description": "Maximum head latency ms", "default": 10.0},
                "skip_load_trigger": {"type": "boolean", "description": "Only monitor (load already triggered)", "default": False},
            },
        },
    ),
    Tool(
        name="wiki_load_status",
        description="Check the status of a running wiki load with all metrics.",
        inputSchema={
            "type": "object",
            "properties": {
                "run_id": {"type": "string", "description": "Run ID (omit for latest)"},
            },
        },
    ),
    Tool(
        name="wiki_load_stop",
        description="Stop a running wiki load monitor and get the final report.",
        inputSchema={
            "type": "object",
            "properties": {
                "run_id": {"type": "string", "description": "Run ID (omit for latest)"},
            },
        },
    ),
    Tool(
        name="pipeline_metrics",
        description=(
            "One-shot pipeline snapshot: DCP enqueued/dequeued/errors, bucket counts, "
            "head latencies, addressing rate, ingestor balance, LMDB counts."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "query_url": {"type": "string"},
                "cb_password": {"type": "string"},
                "kubeconfig": {"type": "string"},
            },
        },
    ),
    Tool(
        name="preflight_check",
        description="Check if buckets are clean before a wiki load.",
        inputSchema={
            "type": "object",
            "properties": {
                "query_url": {"type": "string"},
                "cb_password": {"type": "string"},
                "kubeconfig": {"type": "string"},
            },
        },
    ),
]

# -- Query tools --
QUERY_TOOLS = [
    Tool(
        name="query",
        description=(
            "Run a semantic query against NAM. Returns addressed results with "
            "entity resolution, mode detection (EXPLORATORY/AFFORDANCE), and payloads."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "The search query text"},
                "fan_out": {"type": "integer", "description": "Fan-out level 0-3 (broader=more results)", "default": 3},
                "limit": {"type": "integer", "description": "Max results to return"},
            },
            "required": ["query"],
        },
    ),
    Tool(
        name="graph_walk",
        description=(
            "Walk the graph index from a seed entity. Returns connected entities "
            "and address keys at configurable hop depth. Entity can be a name or 12-char hash."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "entity": {"type": "string", "description": "Seed entity name or 12-char hash"},
                "hop_depth": {"type": "integer", "description": "Number of hops (default: 2)", "default": 2},
                "max_results": {"type": "integer", "description": "Max keys to return", "default": 100},
                "resolve": {"type": "boolean", "description": "Include document payloads from main bucket", "default": False},
            },
            "required": ["entity"],
        },
    ),
    Tool(
        name="record_walk",
        description=(
            "Record-based graph walk from a seed entity. Returns document record IDs "
            "and their associated entities at configurable hop depth."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "entity": {"type": "string", "description": "Seed entity name or 12-char hash"},
                "hop_depth": {"type": "integer", "description": "Number of hops (default: 2)", "default": 2},
                "max_records": {"type": "integer", "description": "Max records to return", "default": 50},
            },
            "required": ["entity"],
        },
    ),
    Tool(
        name="entity_documents",
        description="Given entity hashes, find their records and return document payloads.",
        inputSchema={
            "type": "object",
            "properties": {
                "entities": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Entity hashes to look up",
                },
                "max_records": {"type": "integer", "description": "Max records per entity", "default": 50},
            },
            "required": ["entities"],
        },
    ),
    Tool(
        name="graph_stats",
        description="Get graph index statistics (key counts, entity counts, etc.).",
        inputSchema={"type": "object", "properties": {}},
    ),
]

# -- System monitoring tools --
MONITORING_TOOLS = [
    Tool(
        name="system_status",
        description=(
            "Aggregated system status: bucket stats, ingest pod health, DCP metrics, "
            "pipeline throughput, LMDB sync state. Full dashboard data."
        ),
        inputSchema={"type": "object", "properties": {}},
    ),
    Tool(
        name="bucket_stats",
        description=(
            "Detailed stats for all buckets: item counts, memory, disk usage, "
            "ops/sec, quota. Sourced from data service REST API via query proxy."
        ),
        inputSchema={"type": "object", "properties": {}},
    ),
    Tool(
        name="encoder_heads",
        description=(
            "Encoder head inventory: active pods, records processed, avg latency, "
            "errors, and per-instance metrics for all pipeline stages."
        ),
        inputSchema={"type": "object", "properties": {}},
    ),
    Tool(
        name="pipeline_errors",
        description="Detailed pipeline error breakdown per pod per stage with recent error details.",
        inputSchema={"type": "object", "properties": {}},
    ),
    Tool(
        name="time_series",
        description="Return stored time-series dashboard samples (up to 1 hour of history).",
        inputSchema={"type": "object", "properties": {}},
    ),
    Tool(
        name="dcp_metrics",
        description=(
            "DCP pipeline metrics: enqueued/dequeued/errors totals, nam item count, "
            "ops/sec, lease ownership per ingestor pod."
        ),
        inputSchema={"type": "object", "properties": {}},
    ),
    Tool(
        name="storage_stats",
        description="Storage stats including local RocksDB and S3 data. Proxied from supervisor.",
        inputSchema={"type": "object", "properties": {}},
    ),
]

# -- Task tools --
TASK_TOOLS = [
    Tool(
        name="task_list",
        description="List all registered task types and their current status.",
        inputSchema={"type": "object", "properties": {}},
    ),
    Tool(
        name="task_status",
        description="Get status and metrics for a specific task type.",
        inputSchema={
            "type": "object",
            "properties": {
                "task_type": {
                    "type": "string",
                    "description": "Task type: wiki_load, flush, compaction, cluster_reset, session_seed, address_heal, s3_clear, s3_compaction",
                },
            },
            "required": ["task_type"],
        },
    ),
    Tool(
        name="task_trigger",
        description=(
            "Manually trigger a task. Available types: wiki_load, flush, compaction, "
            "cluster_reset, session_seed, address_heal, s3_clear, s3_compaction. "
            "Optional config overrides (e.g. {\"limit\": 1000} for wiki_load)."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "task_type": {"type": "string", "description": "Task type to trigger"},
                "config": {
                    "type": "object",
                    "description": "Optional config overrides for the task",
                },
            },
            "required": ["task_type"],
        },
    ),
    Tool(
        name="task_history",
        description="Get execution history for a specific task type.",
        inputSchema={
            "type": "object",
            "properties": {
                "task_type": {"type": "string", "description": "Task type"},
            },
            "required": ["task_type"],
        },
    ),
]

# -- Document browsing tools --
DOCUMENT_TOOLS = [
    Tool(
        name="doc_list",
        description=(
            "List documents in a bucket. Uses RocksDB key scan for fast enumeration. "
            "Buckets: main, nam, graph, session, metrics."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "bucket": {"type": "string", "description": "Bucket name", "default": "main"},
                "skip": {"type": "integer", "description": "Number of docs to skip", "default": 0},
                "limit": {"type": "integer", "description": "Max docs to return (max 200)", "default": 20},
                "start_key": {"type": "string", "description": "Start scanning from this key prefix"},
            },
        },
    ),
    Tool(
        name="doc_get",
        description="Get a single document by key from any bucket.",
        inputSchema={
            "type": "object",
            "properties": {
                "bucket": {"type": "string", "description": "Bucket name"},
                "key": {"type": "string", "description": "Document key"},
            },
            "required": ["bucket", "key"],
        },
    ),
]


# ---------------------------------------------------------------------------
# MCP Server
# ---------------------------------------------------------------------------

server = Server("nam-tools")


@server.list_tools()
async def list_tools() -> list[Tool]:
    tools = QUERY_TOOLS + MONITORING_TOOLS + TASK_TOOLS + DOCUMENT_TOOLS
    if _HAS_PIPELINE_MONITOR:
        tools = PIPELINE_TOOLS + tools
    return tools


@server.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
    global _active_runs

    try:
        # ---------------------------------------------------------------
        # Pipeline monitoring tools (require pipeline_monitor from NAM repo)
        # ---------------------------------------------------------------
        if name in ("wiki_load_start", "wiki_load_status", "wiki_load_stop",
                     "pipeline_metrics", "preflight_check") and not _HAS_PIPELINE_MONITOR:
            return [TextContent(
                type="text",
                text=json.dumps({
                    "error": f"Tool '{name}' requires the NAM repo's pipeline_monitor library. "
                             "Install nam-mcp as a submodule in the NAM repo, or set NAM_REPO_ROOT.",
                }),
            )]

        if name == "wiki_load_start":
            records = arguments.get("records", 50000)
            config = _get_config(
                query_url=arguments.get("query_url", ""),
                cb_password=arguments.get("cb_password", ""),
                kubeconfig=arguments.get("kubeconfig", ""),
                min_rate=arguments.get("min_rate", 500.0),
                max_head_latency_ms=arguments.get("max_head_latency_ms", 10.0),
            )
            run_id = f"load-{int(time.time())}"
            monitor = PipelineMonitor(config)
            warnings = monitor.preflight_check()
            warning_msgs = [w.message for w in warnings]

            load_result = {}
            if not arguments.get("skip_load_trigger", False):
                load_result = _trigger_wiki_load(config.query_url, config.cb_password, records)

            run = LoadRun(run_id, config, records)
            run.start()
            _active_runs[run_id] = run

            return _json_result({
                "run_id": run_id,
                "status": "started",
                "records": records,
                "preflight_warnings": warning_msgs,
                "load_trigger_result": load_result,
                "thresholds": {
                    "min_rate": config.min_cumulative_rate,
                    "max_head_latency_ms": config.max_head_latency_ms,
                    "warmup_s": config.warmup_s,
                    "sample_interval_s": config.sample_interval_s,
                },
                "next_step": "Call wiki_load_status to check progress (every 30-60s)",
            })

        elif name == "wiki_load_status":
            run_id = arguments.get("run_id", "")
            if not run_id and _active_runs:
                run_id = max(_active_runs.keys())
            if run_id not in _active_runs:
                return _json_result({
                    "error": f"No active run found (run_id={run_id!r})",
                    "active_runs": list(_active_runs.keys()),
                })
            run = _active_runs[run_id]
            status = run.status_dict()
            if run.result and run.result.violations:
                status["violation_details"] = [
                    {"severity": v.severity, "metric": v.metric, "message": v.message}
                    for v in run.result.violations
                ]
            if run.result and run.result.failed:
                status["diagnostics"] = run.result.diagnostics[:5000]
            return _json_result(status)

        elif name == "wiki_load_stop":
            run_id = arguments.get("run_id", "")
            if not run_id and _active_runs:
                run_id = max(_active_runs.keys())
            if run_id not in _active_runs:
                return _json_result({"error": f"No active run found (run_id={run_id!r})"})

            run = _active_runs[run_id]
            run.stop()
            report = {
                "run_id": run_id,
                "status": "stopped",
                "elapsed_s": round(time.time() - run.started_at, 1),
                "samples_collected": len(run.result.samples) if run.result else 0,
                "failed": run.result.failed if run.result else False,
                "failure_reason": run.result.failure_reason if run.result else "",
                "total_violations": len(run.result.violations) if run.result else 0,
            }
            if run.result and run.result.samples:
                last = run.result.samples[-1]
                report["final_metrics"] = {
                    "enqueued": last.enqueued,
                    "dequeued": last.dequeued,
                    "errors": last.errors,
                    "main_items": last.main_items,
                    "nam_items": last.nam_items,
                    "cumulative_rate": round(last.cumulative_rate, 1),
                    "head_latencies": last.head_latencies,
                    "head_counts": last.head_counts,
                    "addressing_rate": round(last.addressing_rate, 1),
                    "ingestor_vbuckets": last.ingestor_vbuckets,
                    "lmdb_entries": last.lmdb_entries,
                }
            if run.result and run.result.failed:
                report["diagnostics"] = run.result.diagnostics[:5000]
            del _active_runs[run_id]
            return _json_result(report)

        elif name == "pipeline_metrics":
            config = _get_config(
                query_url=arguments.get("query_url", ""),
                cb_password=arguments.get("cb_password", ""),
                kubeconfig=arguments.get("kubeconfig", ""),
            )
            monitor = PipelineMonitor(config)
            monitor._start_time = time.monotonic() - 1
            s = monitor.sample(1)
            return _json_result({
                "enqueued": s.enqueued,
                "dequeued": s.dequeued,
                "errors": s.errors,
                "backlog": s.backlog,
                "main_items": s.main_items,
                "nam_items": s.nam_items,
                "head_latencies": s.head_latencies,
                "head_counts": s.head_counts,
                "addressing_rate": round(s.addressing_rate, 1),
                "addressing_bundle_ms": round(s.addressing_bundle_ms, 1),
                "addressing_build_ms": round(s.addressing_build_ms, 1),
                "write_queue": s.write_queue,
                "write_inflight": s.write_inflight,
                "circuit_state": s.circuit_state,
                "ingestor_vbuckets": s.ingestor_vbuckets,
                "ingestor_published": s.ingestor_published,
                "lmdb_entries": s.lmdb_entries,
                "formatted": PipelineMonitor.format_sample(s),
            })

        elif name == "preflight_check":
            config = _get_config(
                query_url=arguments.get("query_url", ""),
                cb_password=arguments.get("cb_password", ""),
                kubeconfig=arguments.get("kubeconfig", ""),
            )
            monitor = PipelineMonitor(config)
            warnings = monitor.preflight_check()
            return _json_result({
                "clean": len(warnings) == 0,
                "warnings": [
                    {"metric": w.metric, "message": w.message, "value": w.value}
                    for w in warnings
                ],
            })

        # ---------------------------------------------------------------
        # Query tools (via query service REST API)
        # ---------------------------------------------------------------
        elif name == "query":
            body = {"query": arguments["query"]}
            if "fan_out" in arguments:
                body["fan_out"] = arguments["fan_out"]
            if "limit" in arguments:
                body["limit"] = arguments["limit"]
            result = _api_post("/v1/query", body)
            # Compact the response for readability
            return _json_result({
                "ok": result.get("ok"),
                "mode": result.get("mode"),
                "affordance": result.get("affordance"),
                "result_count": len(result.get("results", [])),
                "results": result.get("results", [])[:25],
                "addresses": result.get("addresses", [])[:25],
                "truncated": result.get("truncated"),
                "errors": result.get("errors"),
            })

        elif name == "graph_walk":
            body = {"entity": arguments["entity"]}
            if "hop_depth" in arguments:
                body["hop_depth"] = arguments["hop_depth"]
            if "max_results" in arguments:
                body["max_results"] = arguments["max_results"]
            if arguments.get("resolve"):
                body["resolve"] = True
            result = _api_post("/v1/graph/walk", body)
            return _json_result({
                "ok": result.get("ok"),
                "result_count": result.get("result_count"),
                "hops_used": result.get("hops_used"),
                "edges_followed": result.get("edges_followed"),
                "entities_discovered": result.get("entities_discovered"),
                "seed_keys": result.get("seed_keys", [])[:10],
                "nodes": result.get("nodes", [])[:25],
                "keys": result.get("keys", [])[:25],
                "documents": result.get("documents", {}),
            })

        elif name == "record_walk":
            body = {"entity": arguments["entity"]}
            if "hop_depth" in arguments:
                body["hop_depth"] = arguments["hop_depth"]
            if "max_records" in arguments:
                body["max_records"] = arguments["max_records"]
            result = _api_post("/v1/graph/record-walk", body)
            return _json_result({
                "ok": result.get("ok"),
                "record_count": result.get("record_count"),
                "entity_count": result.get("entity_count"),
                "hops_used": result.get("hops_used"),
                "seed_entity": result.get("seed_entity"),
                "records": result.get("records", [])[:25],
                "entities": result.get("entities", [])[:25],
                "record_entity_map": dict(list(result.get("record_entity_map", {}).items())[:25]),
            })

        elif name == "entity_documents":
            body = {"entities": arguments["entities"]}
            if "max_records" in arguments:
                body["max_records"] = arguments["max_records"]
            result = _api_post("/v1/entities/documents", body)
            return _json_result(result)

        elif name == "graph_stats":
            return _json_result(_api_get("/v1/graph/stats"))

        # ---------------------------------------------------------------
        # System monitoring tools
        # ---------------------------------------------------------------
        elif name == "system_status":
            return _json_result(_api_get("/v1/admin/system-status"))

        elif name == "bucket_stats":
            # Extract just bucket data from system-status
            status = _api_get("/v1/admin/system-status")
            return _json_result({
                "buckets": status.get("buckets", []),
                "timestamp": status.get("timestamp"),
            })

        elif name == "encoder_heads":
            return _json_result(_api_get("/v1/admin/encoder-heads"))

        elif name == "pipeline_errors":
            return _json_result(_api_get("/v1/admin/pipeline-errors"))

        elif name == "time_series":
            return _json_result(_api_get("/v1/admin/time-series"))

        elif name == "dcp_metrics":
            return _json_result(_api_get("/v1/admin/dcp-metrics"))

        elif name == "storage_stats":
            return _json_result(_api_get("/v1/admin/storage-stats"))

        # ---------------------------------------------------------------
        # Task tools
        # ---------------------------------------------------------------
        elif name == "task_list":
            return _json_result(_api_get("/v1/admin/tasks"))

        elif name == "task_status":
            task_type = arguments["task_type"]
            return _json_result(_api_get(f"/v1/admin/tasks/{task_type}"))

        elif name == "task_trigger":
            task_type = arguments["task_type"]
            task_config = arguments.get("config")
            body = task_config if task_config else {}
            return _json_result(_api_post(f"/v1/admin/tasks/{task_type}", body))

        elif name == "task_history":
            task_type = arguments["task_type"]
            return _json_result(_api_get(f"/v1/admin/tasks/{task_type}/history"))

        # ---------------------------------------------------------------
        # Document browsing tools
        # ---------------------------------------------------------------
        elif name == "doc_list":
            bucket = arguments.get("bucket", "main")
            params = []
            if "skip" in arguments:
                params.append(f"skip={arguments['skip']}")
            limit = min(arguments.get("limit", 20), 200)
            params.append(f"limit={limit}")
            if arguments.get("start_key"):
                params.append(f"start_key={urllib.request.quote(arguments['start_key'])}")
            qs = "&".join(params)
            path = f"/v1/admin/docs/{bucket}?{qs}" if qs else f"/v1/admin/docs/{bucket}"
            return _json_result(_api_get(path))

        elif name == "doc_get":
            bucket = arguments["bucket"]
            key = arguments["key"]
            # One path segment: cluster-layout keys contain "#" and "|", and an
            # unencoded "#" ends the URL path (everything after it was dropped).
            return _json_result(_api_get(
                f"/v1/admin/docs/{bucket}/{urllib.parse.quote(key, safe='')}"))

        else:
            return _error_result(f"Unknown tool: {name}")

    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode()[:500]
        except Exception:
            pass
        return _error_result(f"HTTP {e.code}: {e.reason}. {body}")
    except urllib.error.URLError as e:
        return _error_result(f"Connection failed: {e.reason}")
    except Exception as e:
        logger.exception("tool.%s failed", name)
        return _error_result(str(e))


async def main():
    all_tools = QUERY_TOOLS + MONITORING_TOOLS + TASK_TOOLS + DOCUMENT_TOOLS
    if _HAS_PIPELINE_MONITOR:
        all_tools = PIPELINE_TOOLS + all_tools
    logger.info("nam-tools MCP server starting (%d tools, pipeline_monitor=%s)",
                len(all_tools), _HAS_PIPELINE_MONITOR)
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
