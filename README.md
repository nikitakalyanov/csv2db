# csv2db

A simple and light-weight script to trasfer .csv file (calls to police in San Francisco) to a custom SQLite database (very portable format). Creates database from `make_db_structure.sql` and records all valid rows in a .csv to it. The script outputs the number of inserted records and elapsed time. Extra info and error messages are in `csv2db.log` file. Has no dependencies (apart from standard library).

## Usage

Edit `PATH_TO_CSV` and `BUFF_SIZE` (if neccessary) in `csv2db.py` and launch it. The resulting file is `police_db`.
