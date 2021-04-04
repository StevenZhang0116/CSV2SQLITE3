# CSV2SQLITE3

Automatic conversion tool between CSV file to SQLite database. The information would be easily manageable and accessible through workspaces like SQLiteStudio. Line 71 in csv2sqlite.py should be customized depending on the delimiter used in CSV (commonly is comma). Line 75-100 is to extract contents from string format and disregard the comma within so that a single cell would not be separated as two columns and lead to an inconsistent structure. 
