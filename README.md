# NAM MCP Server

[Model Context Protocol](https://modelcontextprotocol.io/) server for [NAM (Neural Addressed Memory)](https://github.com/tdenton8772/nam-documentation). Exposes NAM's query, monitoring, task management, and document APIs as MCP tools for use with Claude Code and other MCP clients.

## Tools

23 tools across 5 categories:

| Category | Tools | Description |
|----------|-------|-------------|
| **Query** | `query`, `graph_walk`, `record_walk`, `entity_documents` | Semantic queries, graph traversal, entity document retrieval |
| **Monitoring** | `system_status`, `bucket_stats`, `encoder_heads`, `pipeline_errors`, `time_series`, `dcp_metrics`, `storage_stats` | System health, bucket statistics, pipeline metrics |
| **Tasks** | `task_list`, `task_status`, `task_trigger`, `task_history` | Manage supervisor tasks (compaction, flush, wiki_load, etc.) |
| **Documents** | `doc_list`, `doc_get` | Browse and retrieve documents from any bucket |
| **Pipeline** | `wiki_load_start`, `wiki_load_status`, `wiki_load_stop`, `pipeline_metrics`, `preflight_check` | Monitored data loads with threshold enforcement (requires NAM repo) |

The first 18 tools (Query, Monitoring, Tasks, Documents) work standalone with just a NAM deployment URL. The 5 Pipeline tools require the full NAM repo's `pipeline_monitor` library.

## Setup

### Requirements

- Python 3.11+
- `mcp` package: `pip install mcp`
- A running NAM deployment with the query service accessible

### Configuration

Set these environment variables:

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `NAM_QUERY_URL` | Yes | `http://193.122.141.217:30800` | Query service URL |
| `NAM_QUERY_PASSWORD` | Yes | — | Admin password for JWT auth |
| `NAM_QUERY_USERNAME` | No | `admin` | Username for JWT auth |

### Claude Code (`.mcp.json`)

Add to your project's `.mcp.json`:

```json
{
  "mcpServers": {
    "nam-tools": {
      "command": "python",
      "args": ["path/to/nam_tools.py"],
      "env": {
        "NAM_QUERY_URL": "http://your-nam-host:30800",
        "NAM_QUERY_PASSWORD": "your-password"
      }
    }
  }
}
```

### As a submodule in the NAM repo

When installed as a git submodule inside the NAM repo, the 5 pipeline monitoring tools are also available:

```bash
cd your-nam-repo
git submodule add git@github.com:tdenton8772/nam-mcp.git scripts/mcp
```

Then in `.mcp.json`:

```json
{
  "mcpServers": {
    "nam-tools": {
      "command": "python",
      "args": ["scripts/mcp/nam_tools.py"],
      "cwd": "/path/to/nam-repo",
      "env": {
        "NAM_QUERY_URL": "http://your-nam-host:30800",
        "NAM_QUERY_PASSWORD": "your-password"
      }
    }
  }
}
```

## Architecture

The MCP server runs as a local stdio process — Claude Code starts and manages it. It authenticates to NAM's query service via JWT (auto-refreshing tokens) and routes all tool calls through the query service REST API. No direct cluster access is needed.

```
Claude Code <--stdio--> nam_tools.py <--HTTP/JWT--> NAM Query Service (port 30800)
```

## Tests

```bash
pip install mcp
python -m pytest tests/ -v
```

55 tests covering all tool handlers, auth caching, error handling, and tool registration.

## License

MIT
