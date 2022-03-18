import csv
import os
import pyodbc 
import datetime
from datetime import datetime, date, timedelta
from time import sleep
server = 'tcp:###' 
username = '###' 
password = '###'

cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';UID='+username+';PWD='+ password)

cursor = cnxn.cursor()

def get_fcr_data(start_date, end_date):
    """
    gets data from db
    start_date, end_date = YYYY-MM-DD HH:MM:SS
    """
    cursor.execute("""SELECT
                DATEPART(DAYOFYEAR,d.finishdatetime) as dzien_roku
                , d.Numer
                FROM
                    (
                SELECT
                cdr.[CallTimestamp]
                , s.finishdatetime
                , CASE 
                WHEN REPLACE(cdr.OriginatingNumber, 'sip:', '') LIKE '%@#' THEN REPLACE(REPLACE(cdr.OriginatingNumber, 'sip:', ''),'@#','')
                WHEN REPLACE(cdr.OriginatingNumber, 'sip:', '') LIKE '%@#' THEN REPLACE(REPLACE(cdr.OriginatingNumber, 'sip:', ''),'@#','')
                ELSE REPLACE(REPLACE(cdr.OriginatingNumber, 'sip:', ''),'@###','')
                END AS Numer
                , s.[status]

                    FROM #.dbo.CDR #
                        JOIN #.dbo.# as s
                        on #.SessionID = s.wave
                    WHERE CallTimestamp BETWEEN ? AND ?
                        AND ApplicationName IN ('###', '###')
                        and s.status IN ('Closed', 'Timeout')
                    ) as d

                WHERE d.Numer NOT IN ('#')
                ORDER by d.finishdatetime ASC;
                            """,start_date,end_date)
    rows = cursor.fetchall()
    return rows

def days(start_day, end_day):
    """
    function returns list_d off numbers of days in the year?
    start_day, end_day = int 
    """
    days_list_d = list(range(start_day, end_day+1))
    return days_list_d

def count_contacts(data_fcr,list_d,s_day=0):
   
    days_count = []
    for list_d[s_day] in list_d:
        i = 0 
        print("Counting contacs in day {day}".format(day=list_d[s_day]))
        day_count = []
        try:
            for i in range(0,len(data_fcr)):
                if list_d[s_day] == int(data_fcr[i][0]):
                    day_count.append(data_fcr[i][1])
                    i += 1
                else:
                    i += 1 
            days_count.append(day_count)
            s_day += 1
        except IndexError: 
            break        
    return days_count

def count_duplicates(data_fcr, list_d, counted, days=7, s_day=0):
    """
    function counts duplicates in given data between given days 
    """
    count_dup = []
    set_list_d = set(list_d)
    try:
        while list_d[s_day] in set_list_d:
            print("Counting duplicates in day {day}".format(day=list_d[s_day]))
            num = [] 
            try:
                i = 0        
                for number in data_fcr:
                    number = data_fcr[i][1]
                    if data_fcr[i][0] in list_d[s_day:s_day+days]:
                        num.append(number)  
                    else:
                        pass
                    i += 1
                z = 0
                number_z = num[z]
                dup_day = set()
                for number_z in num:
                    if number_z in counted[s_day] and num.count(number_z) > 1:
                        dup_day.add(number_z)
                    else:
                        pass
                    z += 1
            except IndexError:
                if days > 0:
                    days -= 1
                    continue
                else:
                    break
            else:
                s_day += 1
                list_d_set_dup = list(dup_day)
                count_dup.append(list_d_set_dup)
    except IndexError:
        pass
   
    return count_dup

def fcr_values(days, counted_contacts, counted_duplicates):
    i = 0
    print('dzien ', 'liczba kontaktów', 'duplikatów', 'procent' )
    for i in range(0,len(days)):
        b = format(float(1-(len(counted_duplicates[i])/len(counted_contacts[i])),), '.2f')
        print(days[i],len(counted_contacts[i]),len(counted_duplicates[i]), b)
        print('-'*30)
        i += 1
  
def fcr_list(days, counted_contacts, counted_duplicates, file):

    list_fcr = []
    i = 0
    for i in range(0,len(days)):
        try:
            list_day = []
            list_day.append(str(days[i]))
            list_day.append(str(len(counted_contacts[i])))
            list_day.append(str(len(counted_duplicates[i])))
            list_fcr.append(list_day)
            i += 1
        except IndexError:
            break
    with open(file, 'r') as csv_file:
        csv_file = csv.reader(csv_file)
        i = 0
        csv_lines = []
        for lines in csv_file:

            csv_lines.append(lines)
        c = []
        g = 1
        for g in range(len(csv_lines)):
            try:
                c.append(csv_lines[g][0])
                g += 1
            except IndexError:
                del g
                break
        if csv_lines == []:
            i = 0
            for i in range(len(list_fcr)):
                    csv_lines.append(list_fcr[i])
        else:
            for i in range(0, len(list_fcr)):
                u = 0
                
                if list_fcr[i][0] in c:
                    for u in range(0,len(csv_lines)):
                        a = list_fcr[i][0]
                        b = csv_lines[u][0]
                        if a == b:
                            csv_lines[u] = list_fcr[i]
                        else:
                            pass
                        u += 1
                else:
                    csv_lines.append(list_fcr[i])
                i += 1
    return csv_lines

def write_to_csv(file, csv_lines):
    fields = ['day', 'counted_cs', 'counted_ds']
    with open(file, 'w+', newline='') as file_to_write:
        writer = csv.writer(file_to_write)
        # writer.writerow(fields)
        writer.writerows(csv_lines)



def main():
    today = datetime.today()
    yesterday = today - timedelta(days=1)
    one_week_ago = today - timedelta(days=8)
    data1 = datetime.strftime(one_week_ago, '%Y-%m-%d')
    data2 = datetime.strftime(yesterday, '%Y-%m-%d')
    yesterday = yesterday.timetuple().tm_yday
    one_week_ago = one_week_ago.timetuple().tm_yday

    data1 = data1 + ' 00:00:00'
    data2 = data2 + ' 23:59:59'
    
    fcr_data = get_fcr_data(data1, data2)


    d = days(one_week_ago,yesterday)
    cc = count_contacts(fcr_data, d)
    cd = count_duplicates(fcr_data, d, cc)

    path_to_f = r'#'
    write_to_csv(path_to_f, fcr_list(d,cc,cd, path_to_f))

if __name__ =='__main__':
    main()

    print('Adding fcr values is done, you can close the window')