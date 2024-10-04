# PySheetsBudget
## Description
My bank &amp; credit card account statement parser utilizing the google sheets API to seamlessly organize and populate my yearly and monthly budget.

Load in .csv files and interactively categorize expenses under Food, Housing, Personal, Media, Travel, Gift, Misc. Can incorporate as many as the user needs.

![outfile](https://github.com/user-attachments/assets/fd981143-c09e-4436-a33d-bb1056f22480)


Categories are remembered and repeat purchases are automatically categorized.

The script is self-organizing, and self-sorting -- it will populate totals without the need for user input. If the user needs to correct, the sheet is dynamically formatted to account for any changes.

![outfile2](https://github.com/user-attachments/assets/59d23424-06f8-4fd7-bdc6-02befdcfdd7f)

## Requirements
- A Google [Service Account](https://docs.gspread.org/en/latest/oauth2.html#service-account). Follow the linked instructions and put the generated credentials.json into the root directory with the batch file.
- Add the attached template.xlsx spreadsheet to your Google Drive and share said sheet with your service account email (with editor privileges)

