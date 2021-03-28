import pandas as pd
from google.colab import data_table
import glob
from datetime import timedelta
from datetime import datetime
from pandas.tseries.offsets import MonthEnd
from google.colab import auth
import gspread
from oauth2client.client import GoogleCredentials
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


def df2ws(sheet, ws_name, df, is_new_ws=False, include_index=False, resize=True):
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

def writeLog(level: LEVELS, ws_name, message, records):
  sh = main_sheet
  ws = "Log"
  eastern = timezone('US/Eastern')
  ts = str(datetime.now(eastern))

  log_header = ["timestamp", "level", "worksheet_name", "message", "record"]

  record_df = pd.DataFrame()
  
  for record in records:
    record_csv = ",".join([str(r) for r in record])
    record_df = record_df.append(pd.DataFrame([[ts, level, ws_name, message, record_csv]], columns=log_header))

  log_df = ws2df(sh, ws, {})
  log_df = log_df.append(record_df)
  df2ws(main_sheet, ws, log_df)