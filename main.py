import jdatetime as jd
import sqlite3
import pandas as pd
import re
import tkinter as tk
import threading
import os

directory = os.path.dirname(os.path.abspath(__file__))

def updatedb():
    
    updatedb.connection = sqlite3.connect(directory + '\\data.db')
    cursor = updatedb.connection.cursor()

    try:
        cursor.execute("CREATE TABLE records (time INTEGER, deposit INTEGER, withdrawal INTEGER, source TEXT, destination TEXT, comment TEXT)")
    except: 
        pass

    datecol = int(main.datefield.get('1.0', 'end-1c')) - 1
    timecol = int(main.timefield.get('1.0', 'end-1c')) - 1
    depositcol = int(main.depositfield.get('1.0', 'end-1c')) - 1
    withdrawalcol = int(main.withdrawalfield.get('1.0', 'end-1c')) - 1
    feecol = int(main.feefield.get('1.0', 'end-1c')) - 1
    sourcecol = int(main.sourcefield.get('1.0', 'end-1c')) - 1
    destinationcol = int(main.destinationfield.get('1.0', 'end-1c')) - 1
    commentcol = int(main.commentfield.get('1.0', 'end-1c')) - 1

    try:
        cursor.execute("DROP TABLE columns")
    except: 
        pass

    cursor.execute("CREATE TABLE columns (date, time, deposit, withdrawal, fee, source, destination, comment)")
    cursor.execute("INSERT INTO columns VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (datecol + 1, timecol + 1, depositcol + 1, withdrawalcol + 1, feecol + 1, sourcecol + 1, destinationcol + 1, commentcol + 1))
    
    try:
        df = pd.read_excel(directory + '\\records.xlsx', index_col= None, header= None)
        df[withdrawalcol] = df[withdrawalcol].fillna(0)
        df[depositcol] =  df[depositcol].fillna(0)
        if feecol != -1:
            df[feecol] = df[feecol].fillna(0)
        
        for i in range(1, len(df)):
            main.updatebuttontext.set(str(i) + '/' + str(len(df)))
            date = str(df[datecol][i])
            if timecol != -1:
                time = str(df[timecol][i])
            else:
                time = "00:00:00"
            
            year = int(re.findall("\d+", date)[0])
            month = int(re.findall("\d+", date)[1])
            day = int(re.findall("\d+", date)[2])
            
            if datecol != timecol:
                hour = int(re.findall("\d+", time)[0])
                minute = int(re.findall("\d+", time)[1])
                second = int(re.findall("\d+", time)[2])
            else:
                hour = int(re.findall("\d+", time)[3])
                minute = int(re.findall("\d+", time)[4])
                second = int(re.findall("\d+", time)[5])
            
            datetime = jd.datetime(year, month, day, hour, minute, second)
            timestamp = datetime.timestamp()
            
            deposit = int(df[depositcol][i])
        
            withdrawal = int(df[withdrawalcol][i])
                    
            if feecol != -1:
                fee = int(df[feecol][i])   
            else:
                fee = 0

            if deposit == 0:
                withdrawal += fee
            elif withdrawal == 0:
                deposit -= fee

            source = str(df[sourcecol][i])
            destination = str(df[destinationcol][i])
            comment = str(df[commentcol][i])
            
            select = cursor.execute("SELECT * FROM records WHERE (time = ? AND deposit = ? AND withdrawal = ? AND source = ? AND destination = ?)", (timestamp, deposit, withdrawal, source, destination)).fetchall()
            if len(select) == 0:
                cursor.execute("INSERT INTO records (time, deposit, withdrawal, source, destination, comment) VALUES (?, ?, ?, ?, ?, ?)", (timestamp, deposit, withdrawal, source, destination, comment))
                updatedb.connection.commit()
    except:
        pass
    try:
        depositsum = cursor.execute("SELECT SUM(deposit) FROM records").fetchall()[0][0]
        withdrawalsum = cursor.execute("SELECT SUM(withdrawal) FROM records").fetchall()[0][0]
        startinghold = int(main.holdfield.get('1.0', 'end-1c'))
        available = depositsum + startinghold - withdrawalsum
        main.availablemoney.set(available)
    except:
        main.availablemoney.set(999999)
    
    updatedb.connection.close()
    main.updatebuttontext.set('در حال ایجاد اکسل')
    recordstoexcel()
    main.updatebutton["state"] = tk.NORMAL
    main.querybutton['state'] = tk.NORMAL
    main.updatebuttontext.set('آپدیت')

def recordstoexcel():
    connection = sqlite3.connect(directory + '\\data.db')
    cursor = connection.cursor()
    sortedbytime = cursor.execute("SELECT * FROM records ORDER BY time ASC").fetchall()
    df = pd.DataFrame(columns= ['time', 'deposit', 'withdrawal', 'source', 'destination', 'comment'])
    for row in sortedbytime:
        time = jd.datetime.fromtimestamp(row[0])
        deposit = row[1]
        withdrawal = row[2]
        source = row[3]
        destination = row[4]
        comment = row[5]
        toappend = {'time': str(time), 'deposit': deposit, 'withdrawal': withdrawal, 'source': source, 'destination': destination, 'comment': comment}
        df = df.append(toappend, ignore_index = True)
    df.to_excel(directory + '\\results.xlsx', 'sortedrecords', index= False)
    connection.close()

def destinations():
    y1 = int(query.y1txt.get('1.0', 'end-1c'))
    m1 = int(query.m1txt.get('1.0', 'end-1c'))
    d1 = int(query.d1txt.get('1.0', 'end-1c'))
    y2 = int(query.y2txt.get('1.0', 'end-1c'))
    m2 = int(query.m2txt.get('1.0', 'end-1c'))
    d2 = int(query.d2txt.get('1.0', 'end-1c'))
    datetime1 = jd.datetime(y1, m1, d1)
    datetime2 = jd.datetime(y2, m2, d2)
    
    dt1 = datetime1.timestamp()
    dt2 = datetime2.timestamp()
    connection = sqlite3.connect(directory + '\\data.db')
    cursor = connection.cursor()
    distinctdestinations = cursor.execute("SELECT DISTINCT destination FROM records WHERE (withdrawal > 0 AND time > ? AND time < ?)", (dt1, dt2)).fetchall()
    try:
        cursor.execute("DROP TABLE destinations")
    except:
        pass
    try:
        cursor.execute("CREATE TABLE destinations (name, sum)")
    except:
        pass
    for dest in distinctdestinations:
        destination = dest[0]
        select = cursor.execute("SELECT SUM(withdrawal) FROM records WHERE (destination = ? AND time > ? AND time < ?)", (destination, dt1, dt2)).fetchall()
        sum = select[0][0]
        cursor.execute("INSERT INTO destinations (name, sum) VALUES (? , ?)", (destination, sum))
    connection.commit()
    connection.close()
    destinationstoexcel()

def destinationstoexcel():
    connection = sqlite3.connect(directory + '\\data.db')
    cursor = connection.cursor()
    sortedbysum = cursor.execute("SELECT * FROM destinations ORDER BY sum DESC").fetchall()
    df = pd.DataFrame(columns= ['name', 'sum'])
    for row in sortedbysum:
        name = row[0]
        sum = row[1]
        toappend = {'name': name, 'sum': sum}
        df = df.append(toappend, ignore_index = True)
    df.to_excel(directory + '\\destinations.xlsx', 'sorteddestinations', index= False)
    connection.close()

def profit():
    y1 = int(query.y1txt.get('1.0', 'end-1c'))
    m1 = int(query.m1txt.get('1.0', 'end-1c'))
    d1 = int(query.d1txt.get('1.0', 'end-1c'))
    y2 = int(query.y2txt.get('1.0', 'end-1c'))
    m2 = int(query.m2txt.get('1.0', 'end-1c'))
    d2 = int(query.d2txt.get('1.0', 'end-1c'))
    datetime1 = jd.datetime(y1, m1, d1)
    datetime2 = jd.datetime(y2, m2, d2)
    dt1 = datetime1.timestamp()
    dt2 = datetime2.timestamp()
    connection = sqlite3.connect(directory + '\\data.db')
    cursor = connection.cursor()
    sumofdeposit = cursor.execute("SELECT SUM(deposit) FROM records WHERE (time > ? AND time < ?)", (dt1, dt2)).fetchall()
    sumofwithdrawal = cursor.execute("SELECT SUM(withdrawal) FROM records WHERE (time > ? AND time < ?)", (dt1, dt2)).fetchall()
    try:
        pl = sumofdeposit[0][0] - sumofwithdrawal[0][0]
    except:
        pl = 0
    query.profit.set(pl)

def estimation():
    connection = sqlite3.connect(directory + '\\data.db')
    cursor = connection.cursor()
    try:
        cursor.execute("DROP TABLE probableexpenses")
    except:
        pass
    cursor.execute("CREATE TABLE probableexpenses (description, amount)")
    try:
        df = pd.read_excel(directory + '\\probable.xlsx', index_col= None, header= None)
        for i in range(1,len(df)):
            description = df[0][i]
            amount = df[1][i]
            cursor.execute("INSERT INTO probableexpenses (description, amount) VALUES (? , ?)", (description, amount))
            connection.commit()
        
        tp = cursor.execute("SELECT SUM(amount) FROM probableexpenses").fetchall()[0][0]
        
        y1 = int(query.y1txt.get('1.0', 'end-1c'))
        m1 = int(query.m1txt.get('1.0', 'end-1c'))
        d1 = int(query.d1txt.get('1.0', 'end-1c'))
        y2 = int(query.y2txt.get('1.0', 'end-1c'))
        m2 = int(query.m2txt.get('1.0', 'end-1c'))
        d2 = int(query.d2txt.get('1.0', 'end-1c'))
        datetime1 = jd.datetime(y1, m1, d1)
        datetime2 = jd.datetime(y2, m2, d2)
        timestamp1 = datetime1.timestamp()
        timestamp2 = datetime2.timestamp() 
        d = (datetime2 - datetime1).days
        
        pct = int(query.pct.get('1.0', 'end-1c'))

        name = query.name.get('1.0', 'end-1c')
        altname = name.replace('ي', 'ی')

        df = pd.read_excel(directory + '\\destinations.xlsx')

        for row in df.iterrows():
            if name == row[1]['name'] or altname == row[1]['name']:
                x = row[1]['sum']
        
        iin = cursor.execute("SELECT SUM(deposit) FROM records WHERE (time > ? AND time < ?)", (timestamp1, timestamp2)).fetchall()[0][0]
        
        result = (((iin * 30 / d) - x) * pct) - (tp - x)
        
        query.estimation.set(int(result))
    except:
        query.estimation.set(0)
    connection.close()

def main():
    gui = tk.Tk('Main Menu')
    gui.title('منو')
    
    datelabel = tk.Label(gui, text= ':شماره ستون تاریخ')
    datelabel.grid(row = 0, column = 1)
    main.datefield = tk.Text(gui, width = 5, height = 1)
    
    main.datefield.grid(row = 0, column = 0)

    timelabel = tk.Label(gui, text= ':شماره ستون ساعت')
    timelabel.grid(row = 1, column = 1)
    main.timefield = tk.Text(gui, width = 5, height = 1)
    
    main.timefield.grid(row = 1, column = 0)

    depositlabel = tk.Label(gui, text = ':شماره ستون واریز')
    depositlabel.grid(row = 2, column = 1)
    main.depositfield = tk.Text(gui, width = 5, height = 1)
    
    main.depositfield.grid(row = 2, column = 0)
    
    withdrawallabel = tk.Label(gui, text = ':شماره ستون برداشت')
    withdrawallabel.grid(row = 3, column = 1)
    main.withdrawalfield = tk.Text(gui, width = 5, height = 1)
    
    main.withdrawalfield.grid(row = 3, column = 0)
    
    feelabel = tk.Label(gui, text = ':شماره ستون کارمزد')
    feelabel.grid(row = 4, column = 1)
    main.feefield = tk.Text(gui, width = 5, height = 1)
    
    main.feefield.grid(row = 4, column = 0)
    
    sourcelabel = tk.Label(gui, text= ':شماره ستون مبدا')
    sourcelabel.grid(row = 5, column = 1)
    main.sourcefield = tk.Text(gui, width = 5, height = 1)
    
    main.sourcefield.grid(row = 5, column = 0)
    
    destinationlabel = tk.Label(gui, text= ':شماره ستون مقصد')
    destinationlabel.grid(row = 6, column = 1)
    main.destinationfield = tk.Text(gui, width = 5, height = 1)
    
    main.destinationfield.grid(row = 6, column = 0)

    commentlabel = tk.Label(gui, text= ':شماره ستون توضیحات')
    commentlabel.grid(row = 7, column = 1)
    main.commentfield = tk.Text(gui, width = 5, height = 1)
    
    main.commentfield.grid(row = 7, column = 0)

    holdlabel = tk.Label(gui, text= ':مانده منقول')
    holdlabel.grid(row = 8, column =1)
    main.holdfield = tk.Text(gui, width = 12, height = 1)
    main.holdfield.insert(tk.END, '0')
    main.holdfield.grid(row = 8, column = 0)

    availablelabel = tk.Label(gui, text= ':قابل برداشت')
    availablelabel.grid(row = 9, column = 1)
    main.availablemoney = tk.IntVar(gui, value= 0)
    main.availableshow = tk.Label(gui, textvariable= main.availablemoney)
    main.availableshow.grid(row = 9, column = 0)
    
    main.updatebuttontext = tk.StringVar(gui, 'آپدیت')
    main.updatebutton = tk.Button(gui, textvariable= main.updatebuttontext, command = start_updating)
    main.updatebutton.grid(row = 10, column = 0)
    
    main.querybutton = tk.Button(gui, text= 'نتیجه گیری', command = query)
    connection = sqlite3.connect(directory + '\\data.db')
    cursor = connection.cursor()
    try:
        columns = cursor.execute("SELECT * FROM columns").fetchall()
        datecol = str(columns[0][0])
        timecol = str(columns[0][1])
        depositcol = str(columns[0][2])
        withdrawalcol = str(columns[0][3])
        feecol = str(columns[0][4])
        sourcecol = str(columns[0][5])
        destinationcol = str(columns[0][6])
        commentcol = str(columns[0][7])
    except:
        datecol = '1'
        timecol = '2'
        depositcol = '5'
        withdrawalcol = '7'
        feecol = '6'
        sourcecol = '20'
        destinationcol = '11'
        commentcol = '13'
    finally:
        main.datefield.insert(tk.END, datecol)
        main.timefield.insert(tk.END, timecol)
        main.depositfield.insert(tk.END, depositcol)
        main.withdrawalfield.insert(tk.END, withdrawalcol)
        main.feefield.insert(tk.END, feecol)
        main.sourcefield.insert(tk.END, sourcecol)
        main.destinationfield.insert(tk.END, destinationcol)
        main.commentfield.insert(tk.END, commentcol)
    try:
        cursor.execute("SELECT * FROM records")
    except: 
        main.querybutton['state'] = tk.DISABLED
    connection.close()
    main.querybutton.grid(row = 10, column = 1)

    gui.mainloop()

def start_updating():
    
    updatethread = threading.Thread(target= updatedb, daemon= True)
    main.updatebutton["state"] = tk.DISABLED
    updatethread.start()
    

def query():
    querygui = tk.Tk('Query')
    querygui.title('جستجو')

    query.date1label = tk.Label(querygui, text = ':تاریخ ابتدا')
    query.date1label.grid(row = 0, column = 3)
    
    query.date2label = tk.Label(querygui, text = ':تاریخ انتها')
    query.date2label.grid(row = 1, column = 3)

    query.y1txt = tk.Text(querygui, width = 4, height = 1)
    query.y1txt.insert(tk.END, '1390')
    query.y1txt.grid(row = 0, column = 0)
    
    query.m1txt = tk.Text(querygui, width = 2, height = 1)
    query.m1txt.insert(tk.END, '1')
    query.m1txt.grid(row = 0, column = 1)
    
    query.d1txt = tk.Text(querygui, width = 2, height = 1)
    query.d1txt.insert(tk.END, '1')
    query.d1txt.grid(row = 0, column = 2)
    
    query.y2txt = tk.Text(querygui, width = 4, height = 1)
    query.y2txt.insert(tk.END, '1410')
    query.y2txt.grid(row = 1, column = 0)
    
    query.m2txt = tk.Text(querygui, width = 2, height = 1)
    query.m2txt.insert(tk.END, '1')
    query.m2txt.grid(row = 1, column = 1)
    
    query.d2txt = tk.Text(querygui, width = 2, height = 1)
    query.d2txt.insert(tk.END, '1')
    query.d2txt.grid(row = 1, column = 2)

    query.profit = tk.IntVar(querygui, 0)

    profitlabel = tk.Label(querygui, text = ':سود/زیان در دوره')
    profitlabel.grid(row = 2, column = 3)
    profitshow = tk.Label(querygui, textvariable= query.profit)
    profitshow.grid(row = 2, column = 1)

    query.estimation = tk.IntVar(querygui, 0)

    estimationlabel = tk.Label(querygui, text = ':سود/زیان احتمالی')
    estimationlabel.grid(row = 3, column = 3)
    estimationshow = tk.Label(querygui, textvariable= query.estimation)
    estimationshow.grid(row = 3, column = 1)

    pctlabel = tk.Label(querygui, text = ':pct')
    pctlabel.grid(row = 4, column = 3)
    query.pct = tk.Text(querygui, width = 10, height = 1)
    query.pct.insert(tk.END, '0')
    query.pct.grid(row = 4, column = 1)

    namelabel = tk.Label(querygui, text = ':نام')
    namelabel.grid(row= 5, column = 3)
    query.name = tk.Text(querygui, width = 10, height = 1)
    query.name.grid(row = 5, column = 1)
    
    resultbutton = tk.Button(querygui, text = 'محاسبه', command = getquery)
    resultbutton.grid(row = 6, column = 0)

    querygui.mainloop()

def getquery():
    destinations()
    profit()
    estimation()

main()
