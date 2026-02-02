import akshare as ak
import pandas as pd
import sqlite3
import os
import time
import logging
from datetime import datetime, timedelta
from typing import List, Optional, Tuple
from contextlib import contextmanager

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DatabaseManager:
    """A股数据库管理类"""
    
    COLUMN_MAPPING = {
        '日期': 'trade_date',
        '开盘': 'open',
        '最高': 'high', 
        '最低': 'low',
        '收盘': 'close',
        '成交量': 'volume',
        '成交额': 'amount',
        '涨跌幅': 'pct_chg',
        '涨跌额': 'change',
        '换手率': 'turnover',
        '振幅': 'amplitude'
    }
    
    def __init__(self, db_path: str = './astock.db'):
        self.db_path = db_path
        self.conn = None
        logger.info(f"数据库管理器初始化，路径: {db_path}")
    
    @contextmanager
    def _get_connection(self):
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row  
        
        try:
            yield self.conn
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logger.error(f"数据库操作失败: {e}")
            raise
    
    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None
            logger.info("数据库连接已关闭")
    
    # ==================== 函数1: 创建数据库 ====================
    def create_database(self, db_name: str = 'astock.db', db_path: str = './') -> bool:

        full_path = os.path.join(db_path, db_name)
        self.db_path = full_path
        
        try:
            os.makedirs(os.path.dirname(full_path), exist_ok=True)
            
            with self._get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS stock_basic (
                        ts_code TEXT PRIMARY KEY,
                        symbol TEXT NOT NULL,
                        name TEXT NOT NULL,
                        industry TEXT,
                        area TEXT,
                        list_date TEXT,
                        is_st BOOLEAN DEFAULT 0,
                        update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS daily_price (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        ts_code TEXT NOT NULL,
                        trade_date TEXT NOT NULL,
                        open REAL,
                        high REAL,
                        low REAL,
                        close REAL,
                        volume REAL,
                        amount REAL,
                        pct_chg REAL,
                        change REAL,
                        turnover REAL,
                        amplitude REAL,
                        update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(ts_code, trade_date),
                        FOREIGN KEY (ts_code) REFERENCES stock_basic(ts_code)
                    )
                ''')
                
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_daily_code ON daily_price(ts_code)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_daily_date ON daily_price(trade_date)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_daily_code_date ON daily_price(ts_code, trade_date)')
                
                logger.info(f"数据库创建成功: {full_path}")
                logger.info("已创建表: stock_basic, daily_price")
                return True
                
        except Exception as e:
            logger.error(f"创建数据库失败: {e}")
            return False
    
    # ==================== 函数2: 下载单只股票历史数据 ====================
    def download_stock_data(
        self, 
        ts_code: str, 
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        adjust: str = 'qfq',
        force_redownload: bool = False
    ) -> Tuple[bool, str, int]:
        if '.' not in ts_code:
            ts_code = self._format_ts_code(ts_code)
        
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
        
        symbol = ts_code.split('.')[0]  
        
        try:
            logger.info(f"开始下载 {ts_code} 数据: {start_date} 至 {end_date}")
            
            df = ak.stock_zh_a_hist(
                symbol=symbol,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust=adjust
            )
            
            if df.empty:
                msg = f"未获取到 {ts_code} 的数据"
                logger.warning(msg)
                return False, msg, 0
            
            df_processed = self._process_dataframe(df, ts_code)
            
            records_saved = self._save_to_database(df_processed, ts_code, force_redownload)
            
            msg = f"成功下载 {ts_code}: {records_saved} 条记录"
            logger.info(msg)
            return True, msg, records_saved
            
        except Exception as e:
            msg = f"下载 {ts_code} 失败: {str(e)}"
            logger.error(msg)
            return False, msg, 0
    
    def _format_ts_code(self, code: str) -> str:
        if code.startswith('6'):
            return f"{code}.SH"
        elif code.startswith('0') or code.startswith('3'):
            return f"{code}.SZ"
        elif code.startswith('4') or code.startswith('8'):
            return f"{code}.BJ"
        else:
            return f"{code}.SH" 
            
    def _process_dataframe(self, df: pd.DataFrame, ts_code: str) -> pd.DataFrame:
        df = df.rename(columns=self.COLUMN_MAPPING)
        
        columns_to_drop = ['股票代码', '序号', 'code', '名称']  
        for col in columns_to_drop:
            if col in df.columns:
                df = df.drop(columns=[col])
        
        df['ts_code'] = ts_code
        
        required_columns = [
            'ts_code', 'trade_date', 'open', 'high', 'low', 'close',
            'volume', 'amount', 'pct_chg', 'change', 'turnover', 'amplitude'
        ]
        
        existing_columns = [col for col in required_columns if col in df.columns]
        df = df[existing_columns]
        
        numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount', 
                        'pct_chg', 'change', 'turnover', 'amplitude']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        if 'trade_date' in df.columns:
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y%m%d')
        
        print(f"处理后的DataFrame列名: {list(df.columns)}")
        
        return df
    
    def _save_to_database(self, df: pd.DataFrame, ts_code: str, force_redownload: bool) -> int:
        """将DataFrame保存到数据库"""
        if df.empty:
            return 0
        
        with self._get_connection() as conn:
            if force_redownload:
                cursor = conn.cursor()
                date_range = (df['trade_date'].min(), df['trade_date'].max())
                cursor.execute(
                    "DELETE FROM daily_price WHERE ts_code = ? AND trade_date BETWEEN ? AND ?",
                    (ts_code, date_range[0], date_range[1])
                )
                logger.info(f"已清理 {ts_code} 在 {date_range[0]} 至 {date_range[1]} 的数据")
            
            records_before = self._count_records(conn, ts_code)
            
            df.to_sql(
                'daily_price',
                conn,
                if_exists='append',
                index=False,
                method='multi'  
            )
            
            records_after = self._count_records(conn, ts_code)
            records_added = records_after - records_before
            
            return records_added
    
    def _count_records(self, conn, ts_code: str) -> int:
        """计算表中已有记录数"""
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM daily_price WHERE ts_code = ?", (ts_code,))
        return cursor.fetchone()[0]
    
    # ==================== 函数3: 批量下载数据 ====================
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
        total = len(ts_code_list)
        results = {
            'success': [],
            'failed': [],
            'total_downloaded': 0,
            'start_time': datetime.now()
        }
        
        logger.info(f"开始批量下载 {total} 只股票数据，批次大小: {batch_size}")
        
        for i in range(0, total, batch_size):
            batch = ts_code_list[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (total + batch_size - 1) // batch_size
            
            logger.info(f"处理批次 {batch_num}/{total_batches}，共 {len(batch)} 只股票")
            
            for j, ts_code in enumerate(batch):
                try:
                    success, msg, records = self.download_stock_data(
                        ts_code=ts_code,
                        start_date=start_date,
                        end_date=end_date,
                        adjust=adjust,
                        force_redownload=False
                    )
                    
                    if success:
                        results['success'].append({
                            'ts_code': ts_code,
                            'records': records,
                            'message': msg
                        })
                        results['total_downloaded'] += records
                        logger.info(f"  [{j+1}/{len(batch)}] ✓ {ts_code}: {records} 条")
                    else:
                        results['failed'].append({
                            'ts_code': ts_code,
                            'error': msg
                        })
                        logger.warning(f"  [{j+1}/{len(batch)}] ✗ {ts_code}: {msg}")
                    
                    if j < len(batch) - 1:
                        time.sleep(0.2)
                        
                except Exception as e:
                    error_msg = f"处理 {ts_code} 时发生异常: {str(e)}"
                    results['failed'].append({
                        'ts_code': ts_code,
                        'error': error_msg
                    })
                    logger.error(f"  [{j+1}/{len(batch)}] ✗ {ts_code}: {error_msg}")
            
            if i + batch_size < total:
                logger.info(f"批次 {batch_num} 完成，等待 {delay_seconds} 秒...")
                time.sleep(delay_seconds)
        
        if retry_failed and results['failed']:
            logger.info(f"开始重试 {len(results['failed'])} 只失败股票...")
            failed_codes = [item['ts_code'] for item in results['failed']]
            retry_results = self.batch_download_data(
                ts_code_list=failed_codes,
                start_date=start_date,
                end_date=end_date,
                adjust=adjust,
                batch_size=5,  
                delay_seconds=delay_seconds * 2,  
                retry_failed=False 
            )
            
            results['success'].extend(retry_results['success'])
            results['total_downloaded'] += retry_results['total_downloaded']
            results['failed'] = retry_results['failed']
        
        results['end_time'] = datetime.now()
        results['duration'] = (results['end_time'] - results['start_time']).total_seconds()
        results['success_count'] = len(results['success'])
        results['failed_count'] = len(results['failed'])
        
        logger.info(f"批量下载完成！成功: {results['success_count']}, 失败: {results['failed_count']}, "
                   f"总记录: {results['total_downloaded']}, 耗时: {results['duration']:.1f}秒")
        
        return results
    
    # ==================== 函数4: 更新数据（增量更新） ====================
    def update_stock_data(
        self,
        ts_code: str,
        end_date: Optional[str] = None,
        adjust: str = 'qfq',
        max_days: int = 365 * 2  
    ) -> Tuple[bool, str, int]:

        if '.' not in ts_code:
            ts_code = self._format_ts_code(ts_code)
        
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')
        
        try:
            latest_date = self._get_latest_trade_date(ts_code)
            
            if latest_date:
                latest_dt = datetime.strptime(latest_date, '%Y%m%d')
                start_dt = latest_dt + timedelta(days=1)
                start_date = start_dt.strftime('%Y%m%d')
                
                if start_date > end_date:
                    msg = f"{ts_code} 数据已是最新（最新日期: {latest_date}）"
                    logger.info(msg)
                    return True, msg, 0
                
                days_diff = (datetime.strptime(end_date, '%Y%m%d') - start_dt).days
                if days_diff > max_days:
                    start_dt = datetime.strptime(end_date, '%Y%m%d') - timedelta(days=max_days)
                    start_date = start_dt.strftime('%Y%m%d')
                    logger.warning(f"{ts_code} 更新天数超过限制，仅更新最近 {max_days} 天数据")
            else:
                start_dt = datetime.strptime(end_date, '%Y%m%d') - timedelta(days=max_days)
                start_date = start_dt.strftime('%Y%m%d')
                logger.info(f"{ts_code} 本地无数据，下载最近 {max_days} 天数据")
            
            logger.info(f"增量更新 {ts_code}: {start_date} 至 {end_date}")
            return self.download_stock_data(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date,
                adjust=adjust,
                force_redownload=False 
            )
            
        except Exception as e:
            msg = f"更新 {ts_code} 失败: {str(e)}"
            logger.error(msg)
            return False, msg, 0
    
    def _get_latest_trade_date(self, ts_code: str) -> Optional[str]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT MAX(trade_date) FROM daily_price WHERE ts_code = ?",
                (ts_code,)
            )
            result = cursor.fetchone()[0]
            return result if result else None
    
    def batch_update_data(
        self,
        ts_code_list: List[str],
        batch_size: int = 20,
        delay_seconds: float = 2.0,
        **kwargs 
    ) -> dict:

        logger.info(f"开始批量增量更新 {len(ts_code_list)} 只股票，参数: {kwargs}")
        
        results = {
            'updated': [],
            'skipped': [],
            'failed': [],
            'total_new_records': 0,
            'start_time': datetime.now()
        }
        
        for i, ts_code in enumerate(ts_code_list, 1):
            try:
                success, msg, new_records = self.update_stock_data(ts_code, **kwargs)
                
                if success:
                    if new_records > 0:
                        results['updated'].append({
                            'ts_code': ts_code,
                            'new_records': new_records,
                            'message': msg
                        })
                        results['total_new_records'] += new_records
                        logger.info(f"[{i}/{len(ts_code_list)}] ✓ {ts_code}: 新增 {new_records} 条")
                    else:
                        results['skipped'].append({
                            'ts_code': ts_code,
                            'message': msg
                        })
                        logger.info(f"[{i}/{len(ts_code_list)}] - {ts_code}: {msg}")
                else:
                    results['failed'].append({
                        'ts_code': ts_code,
                        'error': msg
                    })
                    logger.warning(f"[{i}/{len(ts_code_list)}] ✗ {ts_code}: {msg}")
                
                if i % batch_size == 0 and i < len(ts_code_list):
                    logger.info(f"已处理 {i} 只股票，等待 {delay_seconds} 秒...")
                    time.sleep(delay_seconds)
                    
            except Exception as e:
                error_msg = f"更新 {ts_code} 时异常: {str(e)}"
                results['failed'].append({
                    'ts_code': ts_code,
                    'error': error_msg
                })
                logger.error(f"[{i}/{len(ts_code_list)}] ✗ {ts_code}: {error_msg}")
        
        results['end_time'] = datetime.now()
        results['duration'] = (results['end_time'] - results['start_time']).total_seconds()
        
        logger.info(f"批量更新完成！更新: {len(results['updated'])}, 跳过: {len(results['skipped'])}, "
                   f"失败: {len(results['failed'])}, 新增记录: {results['total_new_records']}")
        
        return results
