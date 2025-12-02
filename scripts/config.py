# Edit these after preview step to match your actual columns
SALES_COLS = ["Store", "Dept", "Date", "Weekly_Sales", "IsHoliday"]  # replace if different
FEATURES_COLS = ["Store", "Date", "Temperature", "Fuel_Price", "MarkDown1", "MarkDown2", "MarkDown3", "MarkDown4", "MarkDown5", "CPI", "Unemployment", "IsHoliday"]  # replace if different
STORES_COLS = ["Store", "Type", "Size"]  # matches the file you uploaded

# Date parsing
DATE_COL = "Date"
DATE_FORMAT = None  # set like "%Y-%m-%d" if needed
