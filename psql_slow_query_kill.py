import psycopg2
import datetime
from datetime import datetime as dt

today = datetime.date.today()

now = dt.now()
date_format_str_db = '%Y-%m-%d %H:%M:%S.%f+06:00'
date_format_str_now = '%Y-%m-%d %H:%M:%S.%f'
yesterday = today - datetime.timedelta(days=1)
print('yesterday = ',yesterday)

conn = psycopg2.connect(
    host="hostname",
    database="postgres",
    user="postgres",
    password="")
cur = conn.cursor()
if conn:
    print("Connected with postgres database.")

query_to_select_pid = """SELECT pid, xact_start FROM pg_stat_activity ORDER BY xact_start ASC;"""

cur.execute(query_to_select_pid)
fetched_data = cur.fetchall()

#stoping all pid older than today
for row in fetched_data:
    if row[1] is not None and str(row[1]).split()[0]<str(today):
        print(str(row[0]), str(row[1]).split()[0])
        kill_pid_query = "SELECT pg_cancel_backend(" + str(row[0]) + ");"
        print(kill_pid_query)
        cur.execute(kill_pid_query)
    
    # kill 6 hours older queries
    elif str(now) > str(row[1]):
        #print("now = ",str(now), " row = ",str(row[1]))
        start = datetime.datetime.strptime( str(row[1]), date_format_str_db)
        end =   datetime.datetime.strptime( str(now) , date_format_str_now)
        # Get interval between two timstamps as timedelta object
        diff = end - start
        # Get interval between two timstamps in hours
        diff_in_hours = diff.total_seconds() / 3600
        print('Difference between two datetimes in hours: ',diff_in_hours)
        if diff_in_hours > 6:
            kill_pid_query = "SELECT pg_cancel_backend(" + str(row[0]) + ");"
            print(kill_pid_query)
            cur.execute(kill_pid_query)


if conn:
    cur.close()
    conn.close()
