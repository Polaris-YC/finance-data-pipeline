### 函数1: 创建数据库
```python    
def create_database(self, db_name: str = 'astock.db', db_path: str = './') -> bool:
```

**Args:**  
    `db_name`: 数据库文件名  
    `db_path`: 数据库存放路径，默认当前目录  
**Returns:**  
    `bool`: 创建是否成功

### 函数2: 下载单只股票历史数据 
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
    `ts_code`: 股票代码，格式如 '000001.SZ' 或 '000001'（自动补全后缀） 
    `start_date`: 开始日期，格式 'YYYYMMDD'，默认一年前  
    `end_date`: 结束日期，格式 'YYYYMMDD'，默认今天  
    `adjust`: 复权类型，'qfq'前复权，'hfq'后复权，''不复权  
    `force_redownload`: 是否强制重新下载（覆盖已有数据） 
    
**Returns:**  
    `Tuple[bool, str, int]`: (是否成功, 消息, 下载的记录数)

### 函数3: 批量下载数据 
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
    `ts_code_list`: 股票代码列表  
    `start_date`: 开始日期  
    `end_date`: 结束日期  
    `adjust`: 复权类型  
    `batch_size`: 每批处理数量  
    `delay_seconds`: 批次间延迟秒数  
    `retry_failed`: 是否重试失败的下载  
    
**Returns:**  
    `dict`: 统计结果  

###  函数4: 更新数据（增量更新）
***增量更新单只股票数据***

```python
def update_stock_data(
    self,
    ts_code: str,
    end_date: Optional[str] = None,
    adjust: str = 'qfq',
    max_days: int = 365 * 2  # 最多更新2年数据，防止异常
) -> Tuple[bool, str, int]:
```

**Args:**  
    `ts_code`: 股票代码  
    `end_date`: 结束日期，默认今天  
    `adjust`: 复权类型  
    `max_days`: 最大更新天数（安全限制）  
    
**Returns:**
    `Tuple[bool, str, int]`: (是否成功, 消息, 新增记录数)  

***批量增量更新股票数据***

```python
    def batch_update_data(
        self,
        ts_code_list: List[str],
        batch_size: int = 20,
        delay_seconds: float = 2.0,
        **kwargs  # 捕获所有其他关键字参数
    ) -> dict:
```

**Args:**  
    `ts_code_list`: 股票代码列表  
    `batch_size`: 批次大小  
    `delay_seconds`: 批次间延迟  
    `**kwargs`: 传递给 update_stock_data 的其他参数，  
    *例如：end_date='20241231', adjust='hfq', max_days=100*
            
**Returns:**  
    `dict`: 更新统计结果  


