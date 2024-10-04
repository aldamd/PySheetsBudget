import os
from glob import glob
import pandas as pd
from numpy import vectorize
from io import StringIO
import datetime as dt
from dateutil.relativedelta import relativedelta

# %%

class DataParse:
  def __init__(self):
    self.transaction_df = None
    self.settings = None
  
  def set_settings_file(self, settings):
    self.settings = settings

  def categorize(self, description, amount):
    description = description.lower()
    for category, items in self.settings["categories"].items():
      for item in items:
          if item in description: return (category, amount)
    
    return ("UNK", amount)

  def __normalize_df(self, df):
    #drop invalid rows
    df = df.dropna()

    #create cumulative count column to maintain local duplicates when global duplicates will be removed later
    df["Count"] = df.groupby(["Date", "Description", "Amount"]).cumcount()

    #converts amount column values to float and ignores zero cost items
    if df["Amount"].dtype != "float64":
      df["Amount"] = df["Amount"].str.replace(",", "")
      df["Amount"] = pd.to_numeric(df["Amount"])
    df = df[df["Amount"] != 0]

    if self.settings and "categories" in self.settings:
      #apply existing categories to purchases, if cannot be categorized, returns "UNK"  
      df["Category"], df["Amount"] = vectorize(self.categorize)(df["Description"], df["Amount"])
      #if credit card category and the number is negative, remove it from the dataframe (ignore credit card costs from bank statements, assumes that the credit card csv will also be provided)
      df = df[df["Category"] != "Credit Card"]

    #convert date column to datetime objects
    df["Date"] = pd.to_datetime(df["Date"])

    #if csv file has more negative numbers than positive, flip them (costs are positive)
    negative_nums = df["Amount"].lt(0).sum()
    positive_nums = df["Amount"].gt(0).sum()
    if (negative_nums > positive_nums):
      df["Amount"] = df["Amount"].apply(lambda x: x * -1)

    return df

  def get_transaction_df(self):
    #grab all csv files in the current directory
    print("Collecting .csv files...")
    filedir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "csv_files")
    csv_files = glob(os.path.join(filedir, "*.csv"))
    if not csv_files:
      input(f"No .csv files provided in {filedir}\nPress Enter to exit\n>> ")
      exit()

    df_bin = []
    #iterate through each csv and scrape data
    for file in csv_files:
      with open(file, "r") as r:
        content = r.readlines()
      if content[0] == "Trans. Date,Post Date,Description,Amount,Category\n":
        print("Compiling Discover card expenses...")
        temp_df = self.__discover_parse(content)
      elif content[0] == "Date,Description,Original Description,Category,Amount,Status\n":
        print("Compiling USAA Bank expenses...")
        temp_df = self.__usaa_parse(content)
      elif "1," in content[0] and "/" in content[0]:
        print("Compiling M&T Bank expenses...")
        temp_df = self.__mtb_parse(content)
      elif "Date,Description,Amount,Running Bal.\n" in content[:7]:
        print("Compiling Bank of America expenses...")
        temp_df = self.__boa_parse(content)
      else:
        print(f"Unrecognized CSV detected: {file}")
        input("Press Enter to exit\n>> ")
        exit()

      #append temporary dataframe to df_bin for later concatenation
      df_bin.append(temp_df)
    
    #remove global duplicate entries, sort the values by date, and do some cleanup
    self.transaction_df = pd.concat(df_bin).drop_duplicates().sort_values(by="Date").reset_index(drop=True).drop(["Count"], axis=1)

  def __boa_parse(self, content):
    #iterate through BOA csv file until the header is found
    for idx, line in enumerate(content):
      if line == "Date,Description,Amount,Running Bal.\n":
        break
    content = content[idx:]
    
    temp_df = pd.read_csv(StringIO("".join(content)), header=0)
    temp_df = temp_df.drop(columns="Running Bal.", axis=1)
    temp_df = self.__normalize_df(temp_df.copy())

    return temp_df

  def __discover_parse(self, content):
    temp_df = pd.read_csv(StringIO("".join(content)), header=0)
    temp_df = temp_df.drop(columns="Post Date", axis=1)

    #universalize the Date column
    temp_df.rename(columns={"Trans. Date": "Date"}, inplace=True)

    temp_df = self.__normalize_df(temp_df.copy())

    return temp_df
  
  def __usaa_parse(self, content):
    temp_df = pd.read_csv(StringIO("".join(content)), header=0)
    temp_df = temp_df.drop(columns=["Original Description", "Status"], axis=1)
    temp_df = self.__normalize_df(temp_df.copy())

    return temp_df
  
  def __mtb_parse(self, content):
    temp_df = pd.read_csv(StringIO("".join(content)), names=["Index", "Date", "Description", "Amount", "UNK", "Running Balance"])
    temp_df.drop(columns=["Index", "UNK", "Running Balance"], axis=1)
    temp_df = self.__normalize_df(temp_df.copy())

  def get_mapped_df(self):
    #find beginning and end dates for data sample
    first_date = self.transaction_df["Date"].iloc[0]
    last_date = self.transaction_df["Date"].iloc[-1]
    first_date = dt.datetime(first_date.year, first_date.month, 1)
    last_date = dt.datetime(last_date.year, last_date.month, 1) + relativedelta(months=1)

    #sort dataframes by month and store in hashmap
    date_map = {}
    current_date = first_date
    while (current_date < last_date):
      date_tag = current_date.strftime("%b %Y")
      temp_df = self.transaction_df[self.transaction_df["Date"] >= current_date]
      temp_df = temp_df[temp_df["Date"] < current_date + relativedelta(months=1)]

      temp_df["Date"] = temp_df["Date"].dt.strftime("%m/%d/%Y")
      date_map[date_tag] = temp_df
      current_date = current_date + relativedelta(months=1)
    
    self.date_map = date_map
    self.first_date = first_date
    self.last_date = last_date

if __name__ == "__main__":
  dataparse = DataParse()
  dataparse.get_mapped_df()
  transaction_df = dataparse.date_map

  print()

#TODO create first time setup, check for most frequently occurring items and save off categorization into json file
