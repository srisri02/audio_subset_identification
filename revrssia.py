import numpy as np
import pandas as pd
import time
import os
import scipy.io.wavfile
from matplotlib import pyplot as plt
from IPython.display import HTML, display
import mysql.connector
import time
import shutil
import random

shared_master_path = '/home/ubuntu/shared1/master'
shared_uploads_path = '/home/ubuntu/shared1/uploads'

config = {
        'user': 'admin',
        'password': 'admin1234',
        'host': 'index-2.caxqdhwqqvt8.ap-northeast-1.rds.amazonaws.com',
        'port': 3306,
        'database': 'index'
    }
conn = mysql.connector.connect(**config)
cursor = conn.cursor()
# Create the 'subset' table
cursor.execute('''CREATE TABLE IF NOT EXISTS subset (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user VARCHAR(255),
    file VARCHAR(200),
    master VARCHAR(200),
    rate FLOAT,
    start VARCHAR(50),
    end VARCHAR(50)
)''')

# Create the 'master' table
cursor.execute('''CREATE TABLE IF NOT EXISTS master (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user VARCHAR(255),
    file VARCHAR(200),
    path VARCHAR(200),
    format VARCHAR(50),
    rate FLOAT,
    dimension INT,
    length INT
)''')

# Save (commit) the changes
conn.commit()

def progress(value, master, max=100):
    return f"{value} - {master}\nProgress: {value}/{max}"

def index_master():
    skip = 0
    count = 0
    total_files = 8723
    start_time = time.time()

    for dirname, _, filenames in os.walk(shared_master_path):
        for filename in filenames:
            try:
                master = os.path.join(dirname, filename)
                master_rate, master_data = scipy.io.wavfile.read(master)
                master_dim = master_data.ndim

                cursor.execute("INSERT INTO master (user, file, path, format, rate, dimension, length) VALUES('0', %s, %s, %s, %s, %s, %s)",
                               (filename, master, str(master_data.dtype), str(master_rate), str(master_dim), str(len(master_data))))
                conn.commit()
                count += 1
                print(f"Progress: {count}/{total_files} - Master: {master}")

            except Exception as e:
                print(e)
                skip += 1

    print("Skipped Files Due to format Problem:", skip)
    print("Files Indexed:", count)

    end_time = time.time()
    execution_time = end_time - start_time
    print("Execution Time: {:.2f} seconds".format(execution_time))

print("Time Required to index all master files:")
index_master()

def insert_master(user, filename, path, data_format, rate, dimension, length):
    query = "INSERT INTO master (user, file, path, format, rate, dimension, length) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    values = (str(user), filename, path, str(data_format), rate, dimension, length)
    cursor.execute(query, values)
    conn.commit()

def get_master(user, filename):
    query = "SELECT * FROM master WHERE user=%s and file=%s"
    values = (str(user), filename)
    cursor.execute(query, values)
    res = cursor.fetchall()
    return res

def get_subset(user, filename):
    query = "SELECT * FROM subset WHERE user=%s and file=%s"
    values = (str(user), filename)
    cursor.execute(query, values)
    res = cursor.fetchall()
    return res

def insert_subset(user, filename, master, rate, start, end):
    query = "INSERT INTO subset (user, file, master, rate, start, end) VALUES (%s, %s, %s, %s, %s, %s)"
    values = (str(user), filename, master, int(rate), int(start), int(end))
    cursor.execute(query, values)
    conn.commit()

def proposed_model(user,filename,path):
    rate, data = scipy.io.wavfile.read(path)
    dim=data.ndim
    count=0
    out = display(progress(0, "master",8723), display_id=True)
    files = select_master(data.dtype, data.ndim,len(data))
    flag=False
    
    for file in files:
        
        master = file[0]
        flag=False
        count=count+1
        out.update(progress(count,master, len(files)))

        master_rate, master_data = scipy.io.wavfile.read(master)
        i=0
        M = len(master_data)-1
        N = len(data)
        possibles_first=np.where(master_data[:-(N-1)] == data[0])[0]
        reverse_data = data[::-1] 
        possibles_last =np.where(master_data[:-(N-1)] == reverse_data[0])[0]
        for i in possibles_first:

            if(np.all(master_data[i + N - 1] == data[N-1] )):
                """
                if np.all(master_data[i: i + N - 1] == data[:N-1]):
                    flag=True
                    break
                """
                if np.all(master_data[i : i+int(len(data)/10)] == data[:int(len(data)/10)]):
                    print("Level 1")
                    if np.all(master_data[i+int(len(data)/10) : i+int(len(data)/10)*2 ] == data[int(len(data)/10) : int(len(data)/10)*2]):
                        print("Level 2")
                        if np.all(master_data[i+int(len(data)/10)*2 : i+int(len(data)/10)*3 ] == data[int(len(data)/10)*2 : int(len(data)/10)*3]):
                            print("Level 3")
                            if np.all(master_data[i+int(len(data)/10)*3 : i+int(len(data)/10)*4 ] == data[int(len(data)/10)*3 : int(len(data)/10)*4]):
                                print("Level 4")
                                if np.all(master_data[i+int(len(data)/10)*4 : i+int(len(data)/10)*5 ] == data[int(len(data)/10)*4 : int(len(data)/10)*5]):
                                    print("Level 5")
                                    if np.all(master_data[i+int(len(data)/10)*5 : i+int(len(data)/10)*6 ] == data[int(len(data)/10)*5 : int(len(data)/10)*6]):
                                        print("Level 6")
                                        if np.all(master_data[i+int(len(data)/10)*6 : i+int(len(data)/10)*7 ] == data[int(len(data)/10)*6 : int(len(data)/10)*7]):
                                            print("Level 7")
                                            if np.all(master_data[i+int(len(data)/10)*7 : i+int(len(data)/10)*8 ] == data[int(len(data)/10)*7 : int(len(data)/10)*8]):
                                                print("Level 8")
                                                if np.all(master_data[i+int(len(data)/10)*8 : i+int(len(data)/10)*9 ] == data[int(len(data)/10)*8 : int(len(data)/10)*9]):
                                                    print("Level 9")
                                                    if np.all(master_data[i+int(len(data)/10)*9 : i+int(len(data)/10)*10 ] == data[int(len(data)/10)*9 : int(len(data)/10)*10]):
                                                        print("Level 10")
                                                        print("Checking Usual")
                                                        flag=True
                                                        break

        if not flag:
            for i in possibles_last:
                if(np.all(master_data[i + N - 1] == reverse_data[N-1])):
                    Levels = 10
                    for level in range(1,Levels + 1):
                        if np.all(master_data[i+int(len(reverse_data)/10) : i+int(len(reverse_data)/10)*level ] == reverse_data[int(len(reverse_data)/10) : int(len(data)/10)*level]):
                            print("Level ",level)
                            if(Levels == level):
                                print("Checking reverse")
                                flag=True
                                break
                if flag:
                    break

        if (flag==True):
            print(filename + " is a subset of Master at :" + master)
            print("Adding Subset to Subset table...")
            insert_subset(user,filename,master,master_rate,i,i+N)
            break
    if(flag==False):
        print(filename + " is not a subset")
        print("Adding as Master File")
        shutil.copy(path, shared_master_path + '/' + filename)
        insert_master(user,filename,path,data.dtype,rate,dim,len(data))

    for dirname, _, filenames in os.walk(shared_uploads_path):
      for filename in filenames:
        file_path = os.path.join(dirname, filename)
        proposed_model('default_user',filename,file_path)

    # Subset File Details
print('\nSubset Table values:')
cursor.execute('''SELECT * FROM subset''')
data = cursor.fetchall()
for row in data:
    print(row)

# Master Table Details
print('\nMaster Table values:')
cursor.execute('''SELECT * FROM master''')
data = cursor.fetchall()
for row in data:
    print(row)


# Save (commit) the changes
conn.commit()

conn.close()
