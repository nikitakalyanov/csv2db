import sqlite3
import csv
import sys
import time

PATH_TO_CSV = '/home/nik/Downloads/Police/police-department-calls-for-service.csv'
BUFF_SIZE = 10000       # number of SQLite transactions to buffer before a commit


# function to output `curr_time` to the `log` file in a pretty way
def log_time(curr_time, log):
    t_struct = time.gmtime(curr_time)
    log.write(str(t_struct.tm_year) + '/')
    log.write(f"{t_struct.tm_mon:02d}" + '/')
    log.write(f"{t_struct.tm_mday:02d}" + ' ')
    log.write(f"{t_struct.tm_hour:02d}" + ':')
    log.write(f"{t_struct.tm_min:02d}" + ':')
    log.write(f"{t_struct.tm_sec:02d}")

try:
    log = open("csv2db.log", "w")
except OSError as error:
    print("Error while opening log-file")
    print(error)
    exit()

start_time = time.time()
log_time(start_time, log)
log.write("\tExecution started")
log.write('\n')

conn = sqlite3.connect('police_db', detect_types=sqlite3.PARSE_DECLTYPES)
c = conn.cursor()

try:
    fd = open('make_db_structure.sql', 'r')
except OSError as error:
    print("Error while opening SQL-script, see log")
    log_time(time.time())
    log.write("\tError while opening SQL-script\t")
    log.write(str(error) + '\n')
    exit()


# build db from SQL-script
script = fd.read()
c.execute(script)
conn.commit()
fd.close()

try:
    csv_file = open(PATH_TO_CSV, 'r')
    lines = csv.reader(csv_file)
except OSError as error:
    print("Error while opening CSV-file, see log")
    log_time(time.time(), log)
    log.write("\tError while opening CSV-file\t")
    log.write(str(error) + '\n')
    exit()

sys.stdout.write("counting lines ...\n")
line_count = sum(1 for line in lines)       # count lines to correctly evaluate progress
sys.stdout.write(f"done\n")
sys.stdout.write(f"\rProgress: {0:.7f}%")
sys.stdout.flush()

csv_file.seek(0)        # return file offset to beginning

i = 0
records_written = 0
for line in lines:
    if i == 0:				# skip first row with header
        i += 1
        c.execute('BEGIN TRANSACTION')
        continue        				

    if i % BUFF_SIZE == 0:      # commit every `BUFF_SIZE` rows
        try:
            c.execute('COMMIT')
        except sqlite3.Error as error:
            conn.rollback()
            log_time(time.time(), log)
            log.write(f"\tError while commiting to db at row {i}, buffer number {i / BUFF_SIZE}\t")
            log.write(str(error) + '\n')
            records_written -= BUFF_SIZE
        c.execute('BEGIN TRANSACTION')

    try:					
        c.execute('INSERT INTO police_requests VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', tuple(line))
        records_written += 1
    except sqlite3.Error as error:
        log_time(time.time(), log)
        log.write(f"\tError while writing to db at row {i}\t")
        log.write(str(error) + '\n')

    if i % 10 == 0:					# update progress every 10 lines
        sys.stdout.write(f"\rProgress: {(i / line_count*100):.7f}%")    
        sys.stdout.flush()
    i += 1

try:
    c.execute('COMMIT')     # commit remaining rows
except sqlite3.Error as error:
    conn.rollback()
    log_time(time.time(), log)
    log.write(f"\tError while commiting to db at row {i}, buffer number {i / BUFF_SIZE} (last buffer)\t")
    log.write(str(error) + '\n')
    records_written -= BUFF_SIZE

sys.stdout.write(f"\rProgress: {100:.7f}%")
sys.stdout.flush()

end_time = time.time()
t_struct = time.gmtime(end_time - start_time)

log_time(end_time, log)
log.write(f"\tWriting finished, {records_written} records written\t")
log.write(f"Time elapsed: ")
log.write(f"{t_struct.tm_hour:02d} hours, {t_struct.tm_min:02d} minutes, {t_struct.tm_sec:02d} seconds\n")


sys.stdout.write("\n")
sys.stdout.write("sucess\n")
sys.stdout.write(f"{records_written} records written\n")
sys.stdout.write(f"Time elapsed: ")
sys.stdout.write(f"{t_struct.tm_hour:02d} hours, {t_struct.tm_min:02d} minutes, {t_struct.tm_sec:02d} seconds\n")

c.close()
conn.close()
csv_file.close()
log.close()