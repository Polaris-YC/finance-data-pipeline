### Function 1: Create Database
```python
def create_database(self, db_name: str = 'astock.db', db_path: str = './') -> bool:
```

**Args:**  
`db_name`: Database file name  
`db_path`: Database storage path, defaults to the current directory  

**Returns:**  
`bool`: Whether creation was successful  

### Function 2: Download historical data for a single stock
```python
def download_stock_data(
self,
ts_code: str,
start_date: Optional[str] = None,
end_date: Optional[str] = None,
adjust: str = 'qfq',
force_redownload: bool = False
) -> Tuple[bool, str, int]:
```

**Args:**  
`ts_code`: Stock code, format such as '000001.SZ' or '000001' (suffix will be auto-completed)  
`start_date`: Start date, format 'YYYYMMDD', defaults to one year ago  
`end_date`: End date, format 'YYYYMMDD', defaults to today  
`adjust`: Adjustment type, 'qfq' for forward-adjusted, 'hfq' for backward-adjusted, '' for no adjustment  
`force_redownload`: Whether to force re-download (overwrite existing data)  

**Returns:**  
`Tuple[bool, str, int]`: (success flag, message, number of records downloaded)  

### Function 3: Batch download data
```python
def batch_download_data(
self,
ts_code_list: List[str],
start_date: Optional[str] = None,
end_date: Optional[str] = None,
adjust: str = 'qfq',
batch_size: int = 10,
delay_seconds: float = 1.0,
retry_failed: bool = True
) -> dict:
```

**Args:**  
`ts_code_list`: List of stock codes  
`start_date`: Start date  
`end_date`: End date  
`adjust`: Adjustment type  
`batch_size`: Number processed per batch  
`delay_seconds`: Delay in seconds between batches  
`retry_failed`: Whether to retry failed downloads  

**Returns:**
`dict`: Statistics result

### Function 4: Update data (incremental update)

***Incremental update for a single stock***

```python
def update_stock_data(
self,
ts_code: str,
end_date: Optional[str] = None,
adjust: str = 'qfq',
max_days: int = 365 * 2 # At most update 2 years of data to prevent anomalies
) -> Tuple[bool, str, int]:
```

**Args:**  
`ts_code`: Stock code  
`end_date`: End date, defaults to today  
`adjust`: Adjustment type  
`max_days`: Maximum number of days to update (safety limit)  

**Returns:**  
`Tuple[bool, str, int]`: (success flag, message, number of new records)  

***Batch incremental update of stock data***

```python
def batch_update_data(
self,
ts_code_list: List[str],
batch_size: int = 20,
delay_seconds: float = 2.0,
**kwargs # Capture all other keyword arguments
) -> dict:
```

**Args:**  
`ts_code_list`: List of stock codes  
`batch_size`: Batch size  
`delay_seconds`: Delay between batches  
`**kwargs`: Other parameters passed to update_stock_data,  
*for example: end_date='20241231', adjust='hfq', max_days=100*  

**Returns:**  
`dict`: Update statistics result  
