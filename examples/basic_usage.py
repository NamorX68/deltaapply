"""Basic usage examples for DeltaApply CDC operations."""

import pandas as pd
import polars as pl
from deltaapply import DeltaApply

def example_with_dataframes():
    """Example using pandas and polars DataFrames."""
    print("=== DataFrame Example ===")
    
    # Create sample source data (new state)
    source_data = pd.DataFrame({
        'id': [1, 2, 3, 4],
        'name': ['Alice', 'Bob Updated', 'Charlie', 'David'],
        'value': [10, 25, 30, 40]
    })
    
    # Create sample target data (current state) 
    target_data = pd.DataFrame({
        'id': [1, 2, 5],
        'name': ['Alice', 'Bob', 'Eve'],
        'value': [10, 20, 50]
    })
    
    print("Source data:")
    print(source_data)
    print("\nTarget data:")
    print(target_data)
    
    # Initialize CDC
    cdc = DeltaApply(
        source=source_data,
        target=target_data,
        key_columns=['id']
    )
    
    # Get summary of changes
    summary = cdc.get_summary()
    print(f"\nChanges summary: {summary}")
    
    # Dry run to see what would change
    result = cdc.apply(dry_run=True)
    print(f"\nInserts ({len(result.inserts)}):")
    if not result.inserts.is_empty():
        print(result.inserts.to_pandas())
    
    print(f"\nUpdates ({len(result.updates)}):")
    if not result.updates.is_empty():
        print(result.updates.to_pandas())
    
    print(f"\nDeletes ({len(result.deletes)}):")
    if not result.deletes.is_empty():
        print(result.deletes.to_pandas())
    
    # Apply all changes
    final_data = cdc.apply()
    print(f"\nFinal synchronized data:")
    print(final_data)


def example_with_csv():
    """Example using CSV files."""
    print("\n=== CSV File Example ===")
    
    # Create sample CSV files
    source_df = pd.DataFrame({
        'product_id': [1, 2, 3, 4],
        'product_name': ['Laptop', 'Mouse Updated', 'Keyboard', 'Monitor'],
        'price': [999.99, 25.99, 79.99, 299.99]
    })
    source_df.to_csv('source.csv', index=False)
    
    target_df = pd.DataFrame({
        'product_id': [1, 2, 5],
        'product_name': ['Laptop', 'Mouse', 'Headphones'],
        'price': [999.99, 19.99, 149.99]
    })
    target_df.to_csv('target.csv', index=False)
    
    # Apply CDC to CSV files
    cdc = DeltaApply(
        source='source.csv',
        target='target.csv', 
        key_columns=['product_id']
    )
    
    # Apply only inserts and updates (keep deletes)
    result_path = cdc.apply(operations=['insert', 'update'])
    print(f"Updated target saved to: {result_path}")
    
    # Read and display final result
    final_data = pd.read_csv('target.csv')
    print("Final CSV content:")
    print(final_data)


def example_with_polars():
    """Example using Polars DataFrames exclusively."""
    print("\n=== Polars DataFrame Example ===")
    
    # Create Polars DataFrames
    source = pl.DataFrame({
        'customer_id': [1, 2, 3, 4],
        'customer_name': ['John', 'Jane Updated', 'Bob', 'Alice'],
        'status': ['active', 'inactive', 'active', 'pending']
    })
    
    target = pl.DataFrame({
        'customer_id': [1, 2, 5],
        'customer_name': ['John', 'Jane', 'Charlie'],
        'status': ['active', 'active', 'inactive']
    })
    
    print("Source (Polars):")
    print(source)
    print("\nTarget (Polars):")
    print(target)
    
    # Apply CDC
    cdc = DeltaApply(
        source=source,
        target=target,
        key_columns=['customer_id']
    )
    
    # Apply only inserts
    result = cdc.apply_inserts_only()
    print(f"\nAfter applying inserts only:")
    print(result)
    
    # Apply all operations
    result_all = cdc.apply()
    print(f"\nAfter applying all operations:")
    print(result_all)


def example_selective_operations():
    """Example showing selective operation application."""
    print("\n=== Selective Operations Example ===")
    
    source = pd.DataFrame({
        'id': [1, 2, 3],
        'value': [10, 20, 30]
    })
    
    target = pd.DataFrame({
        'id': [1, 2, 4], 
        'value': [10, 15, 40]
    })
    
    cdc = DeltaApply(source, target, key_columns=['id'])
    
    print("Original target:")
    print(target)
    
    # Apply different operation combinations
    print("\n1. Only inserts:")
    result1 = cdc.apply(operations=['insert'])
    print(result1)
    
    print("\n2. Only updates:")
    result2 = cdc.apply(operations=['update'])  
    print(result2)
    
    print("\n3. Only deletes:")
    result3 = cdc.apply(operations=['delete'])
    print(result3)
    
    print("\n4. Inserts and updates (no deletes):")
    result4 = cdc.apply(operations=['insert', 'update'])
    print(result4)


if __name__ == "__main__":
    # Run examples
    example_with_dataframes()
    example_with_csv()
    example_with_polars()
    example_selective_operations()
    
    # Clean up CSV files
    import os
    for file in ['source.csv', 'target.csv']:
        if os.path.exists(file):
            os.remove(file)