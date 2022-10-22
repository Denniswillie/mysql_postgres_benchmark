import os
from dotenv import load_dotenv
load_dotenv()

mysql_username = os.getenv("MYSQL_USERNAME")
password = os.getenv("PASSWORD")
postgresql_username = os.getenv("POSTGRESQL_USERNAME")

"""
This read operation on an indexed column, e.g., we will have a table that contains 2 columns, the cryptocurrency and the denomination; the cryptocurrency column will be indexed; we will largely use a query like: select denomination from <table_name> where cryptocurrency = "BTC";

For Research Methods CA 1 - Dennis Willie and Jeremy Neo
"""

# 0. POPULATING DATASET
# This generates 12000 base-26 characters with a catch, the characters are from "a" to "z"
def generate_crypto(num):
	seq = []
	while num > 0:
		rem = num % 26
		num = num // 26
		seq.insert(0, chr(rem + 97))
	ret = "".join(seq)
	return ret	

crypto_nums = range(1, int(input("enter number of cryptocurrencies in dataset: ")) + 1)
cryptos = [(generate_crypto(i), str(i)) for i in crypto_nums]

# FOR POPULATING THE DATABASES
# command to create table
create_table_query = "CREATE TABLE cryptocurrency (currency VARCHAR(255) not null, denomination VARCHAR(255), PRIMARY KEY(currency))"
populate_table_query = "INSERT INTO cryptocurrency (currency, denomination) VALUES (%s, %s)"

# 1. POPULATING MYSQL DATABASE
import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user=mysql_username,
  password=password
)

mycursor = mydb.cursor()

mysql_database_name = "rmMySQL"
mycursor.execute("DROP DATABASE IF EXISTS " + mysql_database_name)
mycursor.execute("CREATE DATABASE " + mysql_database_name)

mycursor.execute("USE " + mysql_database_name)
mycursor.execute(create_table_query)
mydb.commit()

mycursor.executemany(populate_table_query, cryptos)
mydb.commit()

# 2. POPULATING POSTGRESQL DATABASE
import psycopg2

conn = None

try:
	# need to create a database called "cryptocurrency" first.
	conn = psycopg2.connect("dbname=cryptocurrency user={} password={}".format(postgresql_username, password))

	pgcursor = conn.cursor()

	pgcursor.execute("DROP TABLE IF EXISTS cryptocurrency")
	conn.commit()

	pgcursor.execute(create_table_query)
	conn.commit()

	pgcursor.executemany(populate_table_query, cryptos)
	conn.commit()
except (Exception, psycopg2.DatabaseError) as error:
	print(error)

"""
3. Compare the speed of mysql and postgresql. For each DBMS, we run 12 times. In the end, we'll use the average time it used. 
"""
import time
import random

def observe(seed):
	query = "select denomination from cryptocurrency where currency = %s"
	num_of_tests = 20
	randomlist = random.sample(crypto_nums, num_of_tests)
	# Test MySQL (use the mysql cursor that we created above)
	mysql_total_time = 0
	for currency_num_rep in randomlist:
		currency = generate_crypto(currency_num_rep)
		start_time = time.time()
		mycursor.execute(query, (currency,))
		myres = mycursor.fetchone()
		mydb.commit()
		total_time = time.time() - start_time
		assert(myres[0] == str(currency_num_rep))
		mysql_total_time += total_time
	mysql_avg_time = mysql_total_time / num_of_tests

	# Test PostgreSQL
	postgres_total_time = 0
	for currency_num_rep in randomlist:
		currency = generate_crypto(currency_num_rep)
		start_time = time.time()
		pgcursor.execute(query, (currency,))
		pgres = pgcursor.fetchone()
		conn.commit()
		total_time = time.time() - start_time
		assert(pgres[0] == str(currency_num_rep))
		postgres_total_time += total_time
	postgres_avg_time = postgres_total_time / num_of_tests
	return True if mysql_avg_time < postgres_avg_time else False


num_of_observations = 100
mysql_wins = postgres_wins = 0
for seed in range(num_of_observations):
	res = observe(seed)
	if res:
		mysql_wins += 1
	else:
		postgres_wins += 1

print("mysql_wins: ", mysql_wins, "/100")
print("postgres_wins: ", postgres_wins, "/100")

