"""Pytest configuration and fixtures for deltaapply tests."""

import tempfile
import pandas as pd
import polars as pl
import pytest
from pathlib import Path
from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, String
from sqlalchemy.engine import Engine


@pytest.fixture
def sample_source_df() -> pd.DataFrame:
    """Sample source DataFrame for testing."""
    return pd.DataFrame({
        'id': [1, 2, 3, 4],
        'name': ['Alice', 'Bob Updated', 'Charlie', 'David'],
        'value': [10, 25, 30, 40]
    })


@pytest.fixture
def sample_target_df() -> pd.DataFrame:
    """Sample target DataFrame for testing."""
    return pd.DataFrame({
        'id': [1, 2, 5],
        'name': ['Alice', 'Bob', 'Eve'],
        'value': [10, 20, 50]
    })


@pytest.fixture
def sample_source_polars() -> pl.DataFrame:
    """Sample source Polars DataFrame for testing."""
    return pl.DataFrame({
        'id': [1, 2, 3, 4],
        'name': ['Alice', 'Bob Updated', 'Charlie', 'David'],
        'value': [10, 25, 30, 40]
    })


@pytest.fixture
def sample_target_polars() -> pl.DataFrame:
    """Sample target Polars DataFrame for testing."""
    return pl.DataFrame({
        'id': [1, 2, 5],
        'name': ['Alice', 'Bob', 'Eve'],
        'value': [10, 20, 50]
    })


@pytest.fixture
def temp_csv_files(sample_source_df, sample_target_df):
    """Create temporary CSV files for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        source_path = temp_path / "source.csv"
        target_path = temp_path / "target.csv"
        
        sample_source_df.to_csv(source_path, index=False)
        sample_target_df.to_csv(target_path, index=False)
        
        yield {
            'source': str(source_path),
            'target': str(target_path),
            'dir': str(temp_path)
        }


@pytest.fixture
def sqlite_engine():
    """Create in-memory SQLite engine for testing."""
    engine = create_engine("sqlite:///:memory:")
    
    # Create test tables
    with engine.begin() as conn:
        # Source table
        conn.execute(text("""
            CREATE TABLE source_table (
                id INTEGER PRIMARY KEY,
                name TEXT,
                value INTEGER
            )
        """))
        
        # Target table
        conn.execute(text("""
            CREATE TABLE target_table (
                id INTEGER PRIMARY KEY,
                name TEXT,
                value INTEGER
            )
        """))
        
        # Insert test data into source
        conn.execute(text("""
            INSERT INTO source_table (id, name, value) VALUES 
            (1, 'Alice', 10),
            (2, 'Bob Updated', 25),
            (3, 'Charlie', 30),
            (4, 'David', 40)
        """))
        
        # Insert test data into target
        conn.execute(text("""
            INSERT INTO target_table (id, name, value) VALUES 
            (1, 'Alice', 10),
            (2, 'Bob', 20),
            (5, 'Eve', 50)
        """))
    
    yield engine
    engine.dispose()


@pytest.fixture
def key_columns():
    """Standard key columns for testing."""
    return ['id']


@pytest.fixture
def expected_changes():
    """Expected changes for standard test data."""
    return {
        'inserts': pd.DataFrame({
            'id': [3, 4],
            'name': ['Charlie', 'David'],
            'value': [30, 40]
        }),
        'updates': pd.DataFrame({
            'id': [2],
            'name': ['Bob Updated'],
            'value': [25]
        }),
        'deletes': pd.DataFrame({
            'id': [5],
            'name': ['Eve'],
            'value': [50]
        }),
        'unchanged': pd.DataFrame({
            'id': [1],
            'name': ['Alice'],
            'value': [10]
        })
    }