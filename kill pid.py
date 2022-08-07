import psycopg2
import datetime
from datetime import date
today = date.today()

yesterday = today - datetime.timedelta(days=1)
#print('yesterday = ',yesterday)

conn = psycopg2.connect(
    host="server_ip",
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
        

if conn:
    cur.close()
    conn.close()
