import os
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import datetime as dt
from dateutil.relativedelta import relativedelta

# %%

class SheetAPI:
  def __init__(self, sheet_id, DataParse):
    filedir = os.path.dirname(os.path.abspath(__file__))
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(os.path.join(filedir, "credentials.json"), scopes=scopes)
    client = gspread.authorize(creds)

    self.sheet_id = sheet_id
    self.workbook = client.open_by_key(self.sheet_id)

    #get all the current worksheet titles and ids and store them in a map
    print("Fetching worksheets...")
    self.worksheets = {i.title: i for i in self.workbook.worksheets()}

    #assign the monthly and yearly formatted worksheets
    self.monthly_formatted_sheet = None
    self.yearly_formatted_sheet = None
    for worksheet in self.worksheets:
      if "monthly_format" in worksheet:
        self.monthly_formatted_sheet = self.workbook.worksheet(worksheet)
      if "yearly_format" in worksheet:
        self.yearly_formatted_sheet = self.workbook.worksheet(worksheet)
    if not self.monthly_formatted_sheet:
      print("monthly_format worksheet not found in the given Google sheet. Please run the script again when you've got both a monthly_format worksheet and a yearly_format worksheet in the given Google Sheet.")
      input("Press Enter to exit the script\n>> ")
      exit()
    if not self.yearly_formatted_sheet:
      print("yearly_format worksheet not found in the given Google sheet. Please run the script again when you've got both a monthly_format worksheet and a yearly_format worksheet in the given Google Sheet.")
      input("Press Enter to exit the script\n>> ")
      exit()

    self.DataParse = DataParse
  
  def __create_monthly_sheets(self):
    """Create the month by month budget worksheets"""
    #iterate through the monthly dataframes and populate their corresponding worksheets
    print("Sorting expenses by month...")
    self.DataParse.get_mapped_df()
    for month, df in self.DataParse.date_map.items():
      #if this month already exists, check if the dataframe has more entries than the worksheet. If so, update the worksheet and move on
      if month in self.worksheets:
        current_sheet = self.worksheets[month]
        #if the first date in the statements comes before the first date in the worksheet, or the last date in the statements comes after the last date in the worksheets, then the worksheet is missing some entries
        worksheet_dates = current_sheet.col_values(1)
        worksheet_dates = [dt.datetime.strptime(i, "%m/%d/%Y") for i in [worksheet_dates[1], worksheet_dates[-1]]]
        if dt.datetime.strptime(df["Date"].iloc[0], "%m/%d/%Y") < worksheet_dates[0] or dt.datetime.strptime(df["Date"].iloc[-1], "%m/%d/%Y") > worksheet_dates[-1]:
          print(f"Creating {dt.datetime.strptime(df['Date'].iloc[0], "%m/%d/%Y").strftime("%b %Y")}")
          #extract the worksheet data into its own dataframe
          ws_df = pd.DataFrame(current_sheet.get_values("A2:D1000"), columns=["Date", "Description", "Amount", "Category"])
          #convert worksheet string values to float
          ws_df["Amount"] = ws_df["Amount"].str.replace("$", "").str.replace(",", "").astype(float)
          #combine the transaction dataframe with the worksheet dataframe and keep the existing worksheet entries (more accurate category)
          df = pd.concat([df, ws_df]).drop_duplicates(subset=["Date", "Description", "Amount"], keep="last").sort_values(by="Date").reset_index(drop=True)
          print(f"rewriting {month}")
          #overwrite the worksheet
          current_sheet.update(df.values.tolist(), "A2:D1000")
        continue

      print(f"Creating {dt.datetime.strptime(df['Date'].iloc[0], "%m/%d/%Y").strftime("%b %Y")}")
      #duplicate the monthly formatted sheet for each month
      current_sheet = self.monthly_formatted_sheet.duplicate(new_sheet_name=month)
      #make the duplicated worksheet visible
      current_sheet.show()
      #populate the worksheet with the dataframe's values
      current_sheet.update(df.values.tolist(), "A2:D1000")
      self.worksheets[month] = current_sheet
  
  def __create_yearly_summary(self):
    """Create the yearly summary budget worksheets using the existing month by month sheets"""
    #find all the different budget categories and store them in a map with their corresponding acell sum locations
    categories = [i for i in self.yearly_formatted_sheet.col_values(1) if i]
    category_locations = [i[i.find("G"):] for i in self.yearly_formatted_sheet.col_values(2, value_render_option="FORMULA")[1:len(categories)+1]]
    categories = dict(zip(categories, category_locations))

    #initialize storage containers
    month_headers = []
    cell_template = []
    content = []

    #find the cutoff years for the summary worksheet(s)
    start_year = dt.datetime(self.DataParse.first_date.year, 1, 1)
    end_year = dt.datetime(self.DataParse.last_date.year + 1, 1, 1)
    current_date = start_year
    while (current_date < end_year):
      current_year = current_date.year
      header = current_date.strftime("%b %Y")
      #if the month is in the worksheet, then we'll populate the summary worksheet with data from that month
      if header in self.worksheets:
        ws_id = self.worksheets[header].id
        month_headers.append(f'=HYPERLINK("#gid={ws_id}", "{header}")')
        cell_template.append(f"='{header}'!G2") #G2 is arbitrary, just need something for later replacement
      #otherwise add the year header accompanied with blank data
      else:
        month_headers.append(header)
        cell_template.append("")

      #add a month to the current date to keep iterating through the while loop
      current_date += relativedelta(months=1)

      #if we just went from Dec to Jan or this is the last date before the while loop would end1
      if ((current_year - current_date.year) or current_date >= end_year):
        #store the month headers and all corresponding month data in the container for easy cell updating later
        content.append(month_headers)
        #using the cell template, we can dynamically store all the budget categories we need with the category map
        for cell_loc in categories.values():
          content.append([i.replace("G2", cell_loc) for i in cell_template])
        
        #overwrite the year summary worksheet regardless if its already in the workbook (it's not computationally intensive)
        ws_name = f"{current_year} Summary"
        if ws_name in self.worksheets:
          current_sheet = self.worksheets[ws_name]
        else:
          current_sheet = self.yearly_formatted_sheet.duplicate(new_sheet_name=ws_name)
          current_sheet.show()
          self.worksheets[ws_name] = current_sheet

        print(f"Creating summary for {current_year}...")
        current_sheet.update(content, f"B1:M{len(categories) + 1}", raw=False)

        #reset the containers for the next year iteration
        month_headers = []
        cell_template = []
        content = []
  
  def __cleanup(self):
    """Cleans up the worksheet order and hides copious monthly worksheets"""
    print("Cleaning up Google Sheet...")
    
    #TODO fix this
    if "Sheet" in self.worksheets:
      for worksheet in self.worksheets:
        if "Sheet" in worksheet: self.workbook.del_worksheet(self.worksheets[worksheet])

    #Isolate and order the month by month budget worksheets
    ordered_sheets_monthly = sorted([dt.datetime.strptime(i, "%b %Y") for i in self.worksheets.keys() if "_" not in i and "Summary" not in i and "Sheet" not in i])
    ordered_sheets_monthly = [self.worksheets[i.strftime("%b %Y")] for i in ordered_sheets_monthly]

    #Isolate and order the yearly summary budget worksheets
    ordered_sheets_yearly = [self.worksheets[j] for j in sorted([i for i in self.worksheets if "Summary" in i])]

    #Hide the formatting worksheets and the month by month budget worksheets
    ordered_sheets_master = [self.worksheets["monthly_format"], self.worksheets["yearly_format"]] 
    for sheet in ordered_sheets_master + ordered_sheets_monthly:
      if not sheet.isSheetHidden: sheet.hide()

    #Reorder the worksheets such that its hidden, formatted sheets -> summary sheets -> hidden, monthly sheets
    ordered_sheets_master += ordered_sheets_yearly + ordered_sheets_monthly
    self.workbook.reorder_worksheets(ordered_sheets_master)
  
  def execute(self):
    """Create monthly sheets, the corresponding yearly summary, and clean up the work sheet order and visibility"""
    self.__create_monthly_sheets()
    self.__create_yearly_summary()
    self.__cleanup()



if __name__ == "__main__":
  from data_parse import DataParse

  SHEET_ID = "1n-C8Ki2la4qPq72eGRSYHOi_0Rr7ccL_DBoJLvTZ7rI"

  sheet_api = SheetAPI(SHEET_ID, DataParse())
  sheet_api.execute()

  print()