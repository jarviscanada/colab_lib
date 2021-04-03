import pandas as pd
from google.colab import data_table
import glob
from datetime import timedelta
from datetime import datetime
from pandas.tseries.offsets import MonthEnd
from google.colab import auth
import numpy as np
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import pytz
from pytz import timezone

#convert `$` to float
def currency2float(df, fields):
  for field in fields:
    if (df[field].dtype != np.float64):
      df[field] = df[field].str.replace(',', '').str.replace('$', '')
      df[field] = df[field].astype('float')


def convertPercentage2float(df, fields):
  for field in fields:
    if (df[field].dtype != np.float64):
      df[field] = df[field].str.replace(',', '').str.replace('%', '')
      df[field] = df[field].astype('float') / 100

def ws2df_currency(sheet, ws_str, dtype_map, currency_fields):
  df = ws2df(sheet, ws_str, dtype_map)
  currency2float(df, currency_fields)
  return df


def ws2df(sheet, ws_str, dtype_map):
  ws = sheet.worksheet(ws_str)
  df = (
      pd.DataFrame
      .from_records(ws.get_all_values()[1:], columns=ws.get_all_values()[0])
      ).replace(r'^\s*$', np.nan, regex=True).dropna(thresh=1)
  return df.astype(dtype_map)


def df2ws(sheet, ws_name, df, is_new_ws=False, include_index=False, resize=False):
  try:
    ws = sheet.worksheet(ws_name)
  except:
    ws = sheet.add_worksheet(ws_name, 1, 1, index=None)
  ws.clear()
  set_with_dataframe(ws, df, include_index=include_index, resize=resize)

def new_ws(sheet, ws_name):
  ws = sheet.add_worksheet(ws_name, 1, 1, index=None)

#logger
class LEVELS:
  INFO = "INFO"
  WARN = "WARN"
  ERROR = "ERROR"


def clear_log(sheet):
  #clear Log worksheet
  empty_log = pd.DataFrame(columns=["timestamp",	"level",	"worksheet_name",	"message", "record"])
  df2ws(sheet, "Log", empty_log, resize=False)

def writeLog(sheet, level: LEVELS, ws_name, message, records):
  sh = sheet
  ws = "Log"
  eastern = timezone('US/Eastern')
  ts = str(datetime.now(eastern))

  log_header = ["timestamp", "level", "worksheet_name", "message", "record"]

  record_df = pd.DataFrame()
  
  for record in records:
    record_csv = ",".join([str(r) for r in record])
    record_df = record_df.append(pd.DataFrame([[ts, level, ws_name, message, record_csv]], columns=log_header))

  print(record_df.to_string())
  log_df = ws2df(sh, ws, {})
  log_df = log_df.append(record_df)
  df2ws(sh, ws, log_df)
  
def is_subset(list_a, list_b):
  '''check if list_b is a subset of list_a'''
  return all([item in list_a for item in list_b])

def add_gp_col(df):
  required_header = ['hours' ,'bill_rate' ,'pay_rate', 'vendor_fee_rate' ,'payroll_fee_rate']
  check_required_header(df, required_header)

  df["bill_amount"] = df.hours * df.bill_rate
  df["pay_amount"] = df.hours * df.pay_rate
  df["vendor_fee"] = df.bill_amount * df.vendor_fee_rate
  df["payroll_fee"] = df.pay_amount * df.payroll_fee_rate
  df["gross_profit"] = df.bill_amount - df.pay_amount - df.vendor_fee - df.payroll_fee
  return df

def add_commission_col(df):
  required_header = ['gross_profit', 'commission_rate']
  check_required_header(df, required_header)
  df["commission_amount"] = df.gross_profit * df.commission_rate
  return df

#handle commission
def is_subset(list_a, list_b):
  '''check if list_b is a subset of list_a'''
  return all([item in list_a for item in list_b])


def add_commission_col(df):
  required_header = ['gross_profit', 'commission_rate']
  check_required_header(df, required_header)
  df["commission_amount"] = df.gross_profit * df.commission_rate
  return df


def get_commission_df(commission_sh, ws_name):
  dtype_map = {
      'timestamp': 'datetime64',
      'week_ending': 'datetime64',
      'hours': 'float64',
      'commission_rate': 'float64',
  }

  df = ws2df(commission_sh, ws_name, dtype_map)
  currency2float(df, ['bill_rate', 'pay_rate', 'bill_amount', 'pay_amount', 
                           'vendor_fee', 'payroll_fee', 'gross_profit', 'commission_amount'])
  df = df.dropna(subset=["id", "week_ending"])
  return df

def check_required_header(df, required_header):
  df_cols = df.columns.tolist()
  if not is_subset(df_cols, required_header):
    missing_fields = [item for item in required_header if item not in df_cols]
    msg = f'Missing headers: {",".join(missing_fields)}'
    raise RuntimeError(msg)


def get_new_records(df_a, df_b, keys):
  '''Return new records (by keys) that are not in b'''
  return (df_a
          .merge(df_b[keys], how='left', on=keys, indicator=True)
          .query("_merge == 'left_only'"))

# def append_new_records(df_a, df_b, keys, fields):
#   '''append new records (by keys) from a into b'''
#   check_required_header(df_a, fields)
#   check_required_header(df_b, fields)

#   df_a_new = get_new_records(df_a, df_b, keys)
#   return pd.concat([df_a_new[fields], df_b])


def write_commission_to_gsheet(commission_df, commission_sh):  
  commission_df = commission_df.copy()
  ts_now = datetime.now(timezone('US/Eastern'))
  commission_df['timestamp'] = ts_now.strftime("%Y-%m-%d %H:%M:%S")
  commission_df['commission_paid'] = ""


  commission_header = ['employee_email', 'id', 'name' ,'role' ,'week_ending' ,'hours', 
                      'client', 'timestamp', 'bill_rate','pay_rate' ,'bill_amount' 
                      ,'pay_amount' ,'vendor_fee' ,'payroll_fee' ,'gross_profit' 
                      ,'commission_rate' ,'commission_amount']

  #Append new items in too `all` worksheet
  keys = ['employee_email', 'id', 'role', 'week_ending']

  commission_all_df = get_commission_df(commission_sh, "all")
  check_required_header(commission_df, commission_header)
  new_commission_df = get_new_records(commission_df, commission_all_df, keys)
  union_all_df = pd.concat([new_commission_df[commission_header], commission_all_df])
  union_all_df = union_all_df.sort_values(by=["week_ending", "employee_email", "id", "role"])
  df2ws(commission_sh, 'all', union_all_df)

  #Append new items to `unpaid` worksheet
  commission_unpaid_df = get_commission_df(commission_sh, "unpaid")
  check_required_header(commission_unpaid_df, commission_header)
  union_unpaid_df = pd.concat([new_commission_df[commission_header], commission_unpaid_df])
  union_unpaid_df = union_unpaid_df.sort_values(by=["week_ending", "employee_email", "id", "role"])
  df2ws(commission_sh, 'unpaid', union_unpaid_df)
