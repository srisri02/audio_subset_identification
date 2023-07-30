import numpy as np
import pandas as pd
import time
import os
import scipy.io.wavfile
from matplotlib import pyplot as plt
from IPython.display import HTML, display
import mysql.connector
import time
import sqlite3
import shutil
import random

shared_master_path = '/home/ubuntu/shared/master'
shared_uploads_path = '/home/ubuntu/shared/uploads'
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


def select_master(data_format, dimension, length):
    query = "SELECT path FROM master WHERE format='" + str(data_format) + "' and dimension=" + str(dimension) + " and length >=" + str(length)
    cursor.execute(query)
    res = cursor.fetchall()
    return res

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


def proposed_model(user, filename, path):
    rate, data = scipy.io.wavfile.read(path)
    dim = data.ndim
    count = 0
    files = select_master(data.dtype, data.ndim, len(data))
    flag = False

    for file in files:
        master = file[0]
        flag = False
        count += 1
        print("Progress: {}/{}".format(count, len(files)))

        master_rate, master_data = scipy.io.wavfile.read(master)

        i = 0
        M = len(master_data) - 1
        N = len(data)
        possibles = np.where(master_data[:-(N - 1)] == data[0])[0]

        for i in possibles:
            if np.all(master_data[i + N - 1] == data[N - 1]):
                Levels = 10
                for level in range(1, Levels + 1):
                    if np.all(master_data[i + int(len(data) / 10): i + int(len(data) / 10) * level] == data[int(len(data) / 10): int(len(data) / 10) * level]):
                        print("Level ", level)
                        if Levels == level:
                            flag = True
                            break

        if flag:
            print(filename + " is a subset of Master at: " + master)
            print("Adding Subset to Subset table...")
            insert_subset(user, filename, master, master_rate, i, i + N)
            break

    if not flag:
        print(filename + " is not a subset")
        print("Adding as Master File")
        shutil.copy(path, shared_master_path + '/' + filename)
        insert_master(user, filename, path, data.dtype, rate, dim, len(data))


for dirname, _, filenames in os.walk(shared_uploads_path):
  for filename in filenames:
    file_path = os.path.join(dirname, filename)
    proposed_model('user',filename, file_path)

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

