from data_parse import DataParse
from sheet_api import SheetAPI
from gspread.exceptions import APIError
import json
from numpy import vectorize
import os
from glob import glob
from time import sleep

# %%

class CLI:
  def __init__(self):
    self.splash = r"""
     ____          _          _  _  _  _   
    |  _ \        | |        | |(_)(_)| |  
    | |_) | _   _ | |__    __| | _  _ | |_ 
    |  _ < | | | || '_ \  / _` || || || __|
    | |_) || |_| || | | || (_| || || || |_ 
    |____/  \__,_||_| |_| \__,_|| ||_| \__|
                               _/ |        
                              |__/         """
    self.categories = ["Housing", "Car", "Food", "Media", "Personal", "Travel", "Gift", "Misc", "Credit"]

    self.clear_str = None
    self.__get_os_clear_str()

    self.DataParse = DataParse()

    self.settings_file = None
    self.settings = {}
    self.__get_settings()
    if not self.settings:
      self.__first_time_setup()

    self.DataParse.get_transaction_df()
    sleep(1)

    self.__title()

    self.SheetAPI = SheetAPI(self.settings["sheet_id"], self.DataParse)
    while True:
      try:
        self.SheetAPI.execute()
        break
      except APIError:
        print("Google's write request quota reached, waiting 1 minute before proceeding...")
        sleep(60)
    print("Done! Please Venmo Dan $5 for running the script")
    sleep(2)
    exit()


  def __get_os_clear_str(self):
    if os.name == "nt":
      self.clear_str = "cls"
    else:
      self.clear_str = "clear"

  def __get_settings(self):
    filedir = os.path.dirname(os.path.abspath(__file__))
    self.settings_file = os.path.join(filedir, "settings.conf")
    files = glob(os.path.join(filedir, "*"))
    for file in files:
      if "settings.conf" in file:
        try:
          with open(file, "r") as r:
            self.settings = json.load(r)
        except:
          print("ERROR: Invalid settings.conf formatting detected. Please exit the script and attempt to repair or press Enter to begin first time setup.")
          input(">> ")
          return
        self.settings_file = file
        self.DataParse.set_settings_file(self.settings)
        break
    

  def __write_settings(self):
    with open(self.settings_file, "w") as w:
      json.dump(self.settings, w, indent=1)

  def __reinitialize_category_df(self):
    for category in self.categories:
      self.uncat_df = self.uncat_df[self.uncat_df["Category"] != category]
    self.df_set = self.uncat_df["Description"].value_counts().to_frame("Count")

  def __categorize_df(self, description, orig_category):
    description = description.lower()
    if self.expense in description:
      self.log_bin.append(description)
      return self.category.capitalize()
    
    return orig_category
    
  def __category_option(self):
    os.system(self.clear_str)

    if "categories" not in self.settings:
      category_dict = {key : [] for key in self.categories}
    else:
      category_dict = self.settings["categories"]

    self.uncat_df = self.DataParse.transaction_df.copy()
    for category in self.categories + ["Paycheck"]:
      self.uncat_df = self.uncat_df[self.uncat_df["Category"] != category]
    self.rollback_df = self.uncat_df.copy()

    self.df_set = self.uncat_df["Description"].value_counts().to_frame("Count")

    input_count = 0
    last_category = ""
    undone = False
    rows_changed = None
    set_size = self.df_set.shape[0]
    while set_size > 0:
      os.system(self.clear_str)
      print(self.df_set.head(50).to_string(header=False))
      print(f"\nPlease sort the above expenses in the following categories:\n\t{', '.join(self.categories)}\nIf the expense is a Credit Card payment or deposit, please categorize it as Credit.\nFor example:\n\tamzn : personal\n\tsunoco: car\n\tdiscover des :credit\n")
      if set_size < 100 or input_count >= 20:
        print("When you're satisfied with the sorting, input \"exit\"\n")
      if rows_changed != None:
        print(f"{rows_changed} expenses categorized")

      user_input = input()

      if user_input.strip().lower() in ["exit", "quit", "0"]:
        os.system(self.clear_str)
        break

      if user_input.strip().lower() in ["undo", "back", "rollback"]:
        if undone:
          input("Only one undo operation is available. Press Enter to continue\n")
          continue
        if not last_category:
          input("There's nothing to undo you dingus. Press Enter to continue\n")
          continue
        input_count -= 1
        undone = True
        self.uncat_df = self.rollback_df.copy()
        category_dict[last_category].pop()
        self.__reinitialize_category_df()
        continue
      else:
        undone = False
      
      if ":" not in user_input:
        input("Invalid input. Please follow the format of {EXPENSE}:{CATEGORY}. Press Enter to continue\n")
        continue
      
      self.rollback_df = self.uncat_df.copy()
      expense, category = [i.strip() for i in user_input.split(":")]
      category = category.capitalize()
      expense = expense.lower()
      self.category = category
      self.expense = expense

      if not expense:
        input("Invalid input. Please follow the format of {EXPENSE}:{CATEGORY}. Press Enter to continue\n")
        continue
      if category.capitalize() not in self.categories:
        input(f"Invalid category. Please only select from: {', '.join(self.categories)}. Press Enter to continue\n")
        continue

      self.log_bin = []
      self.rollback_df["Category"] = vectorize(self.__categorize_df)(self.uncat_df["Description"], self.uncat_df["Category"])
      rows_changed = len(self.log_bin)
      if rows_changed > 0:
        input_count += 1
        last_category = category
        self.rollback_df, self.uncat_df = self.uncat_df, self.rollback_df
        category_dict[category].append(expense)
        self.__reinitialize_category_df()
        set_size = self.df_set.shape[0]

    self.settings["categories"] = category_dict
    self.DataParse.transaction_df["Category"], self.DataParse.transaction_df["Amount"] = vectorize(self.DataParse.categorize)(self.DataParse.transaction_df["Description"], self.DataParse.transaction_df["Amount"])
    self.__write_settings()

  def __url_option(self):
    if not self.settings:
      print("Welcome to the first time setup!\n")
    print("Please enter the URL of the google spreadsheet you'd like to be using (make sure the spreadsheet already has the monthly and yearly templates!):")
    while True:
      try:
        url = input(">> ").strip()
        sheet_id = url.split("/")
        sheet_id = sheet_id[sheet_id.index("d") + 1]
        self.settings["sheet_id"] = sheet_id
        self.__write_settings()
        return
      except ValueError:
        print("ERROR: invalid URL. Please try again")
        continue

  def __first_time_setup(self):
    print(self.splash + "\n")
    if "sheet_id" not in self.settings:
      self.__url_option()
    print("\nBefore we continue, please make sure that you've shared your Google Sheet with your service account email (with editor privileges)!")
    input("Press Enter to continue\n>> ")

  def __title(self):
    while True:
      os.system(self.clear_str)
      print(self.splash)
      print("\n")
      print("1. Create Budget")
      print("2. Categorize Expenses")
      print("3. Change Google Sheet URL")
      print("4. Exit\n")

      print(f"current sheet id: {self.settings['sheet_id']}")

      transaction_df = self.DataParse.transaction_df
      if "Category" not in transaction_df:
        transaction_df["Category"] = "UNK"

      uncategorized_expenses = transaction_df[transaction_df["Category"] == "UNK"].shape[0]
      print(f"{uncategorized_expenses} uncategorized expenses")
      print()

      while True:
        user_selection = input(">> ").strip()
        if user_selection in ["1", "2", "3", "4"]:
          break
        else:
          print("Invalid selection, please try again")
      
      if user_selection == "1":
        os.system(self.clear_str)
        print("Creating budget...")
        break
      elif user_selection == "2":
        self.__category_option()
        continue
      elif user_selection == "3":
        self.__url_option()
        print("Link successfully changed!")
        sleep(1)
        continue
      elif user_selection == "4":
        exit()


if __name__ == "__main__":
  CLI()

#TODO show the name of the currently set spreadsheet
#TODO show number of uncategorized expenses