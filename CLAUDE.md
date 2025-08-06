# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

deltaapply is a Python package for Change Data Capture (CDC) with automatic application of inserts, updates, and deletes. The project is in early development (version 0.0.1.dev0).

## Project Structure

- **Package**: Located in `src/deltaapply/`
- **Build System**: Uses modern Python packaging with `pyproject.toml`
- **Python Version**: Requires Python >=3.12
- **License**: MIT

## Development Commands

**Add dependencies:**
```bash
uv add <package-name>
```

**Install in development mode:**
```bash
uv sync --dev
```

**Build package:**
```bash
uv build
```

**Run tests:**
```bash
uv run pytest
```

**Run tests with coverage:**
```bash
uv run pytest --cov=src/deltaapply --cov-report=html
```

## Architecture Notes

- **Package Structure**: Standard Python `src/` layout
- **Core Module**: `deltaapply.core.DeltaApply` - main CDC orchestration class
- **Data Sources**: `deltaapply.data_sources.DataSource` - handles CSV/DataFrame/DB table inputs uniformly
- **CDC Logic**: `deltaapply.cdc_operations.CDCOperations` - detects inserts/updates/deletes
- **Target Writers**: `deltaapply.target_writers.TargetWriter` - applies changes to different output formats
- **Type System**: Polars DataFrames used internally, maintains input/output type consistency
- **Key Dependencies**: polars (>=1.32.0), pandas (>=2.3.1), sqlalchemy (>=2.0.42)

## Coding Standards

- All code documentation should be in English (including inline comments)
- Use Google-style docstrings for all functions
- Use type hints everywhere
- Maximum line length is 120 characters
- Place imports at the beginning of modules