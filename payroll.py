
import mysql.connector
from csv import reader
from mysql.connector import errorcode

# create the connection
cnx = mysql.connector.connect(user='root', password='root', host='127.0.0.1:8889',
                              unix_socket='/Applications/MAMP/tmp/mysql/mysql.sock')

DATABASE_NAME = 'GraceAndAhmad1'

cursor = cnx.cursor()

# ceate the database


def creating_databases(cursor):
    try:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DATABASE_NAME))
    except mysql.connector.Error as err:
        print("Failed to create a database: {}".format(err))
        exit(1)


# if the database is not created we create it now
try:
    cursor.execute("USE {}".format(DATABASE_NAME))
except mysql.connector.Error as err:
    print("Database {} does not exixt.".format(DATABASE_NAME))

    if err.errno == errorcode.ER_BAD_DB_ERROR:
        creating_databases(cursor)
        print("Database '{}' created".format(DATABASE_NAME))
        cnx.database = DATABASE_NAME

    else:
        print(err)
        exit(1)

# create a table dictionary

table = {}


# creeate the tables

table['employee'] = (
    "  CREATE TABLE `employee` ("
    "  `employeeId` int(50) NOT NULL,"
    "  `FirstName` varchar(255) NOT NULL,"
    "  `LastName` varchar(255) NOT NULL,"
    "  `Gender` char(1) NOT NULL,"
    "  `Age` int(50) NOT NULL,"
    "  `Phone` varchar(50) NOT NULL,"
    "  `Address` varchar(255) NOT NULL,"
    "  `Email` varchar(255) NOT NULL,"
    "  `payementAmount` varchar(50) NOT NULL,"
    "  PRIMARY KEY (`employeeId`)"
    ") ENGINE=InnoDB")


table['employer'] = (
    "  CREATE TABLE `employer` ("
    "  `employerId` int(50) NOT NULL,"
    "  `FirstName` varchar(255) NOT NULL,"
    "  `LastName` varchar(255) NOT NULL,"
    "  `Gender` char(1) NOT NULL,"
    "  `Age` int(50) NOT NULL,"
    "  `Phone` varchar(50) NOT NULL,"
    "  `Email` varchar(255) NOT NULL,"
    "  `Adddress` varchar(255) NOT NULL,"
    "  `payementAmount` varchar(50) NOT NULL,"
    "  PRIMARY KEY (`employerId`)"
    ")  ENGINE = InnoDB")


table['Bank'] = (
    "  CREATE TABLE `bank` ("
    "  `bankId` int(50) NOT NULL,"
    "  `Name` varchar(255) NOT NULL,"
    "  `Phone` varchar(50) NOT NULL,"
    "  `Address` varchar(255) NOT NULL,"
    "  `employeeId` int(50),"
    "  `payementCode` int(50),"
    "  PRIMARY KEY (`bankId`),"
    "  CONSTRAINT `payementCode` FOREIGN KEY (`payementCode`)"
    "  REFERENCES `payement` (`payementCode`) ON DELETE NO ACTION ON UPDATE NO ACTION,"
    "  CONSTRAINT `employeeId` FOREIGN KEY (`employeeId`)"
    "  REFERENCES `employee` (`employeeId`) ON DELETE NO ACTION ON UPDATE NO ACTION"
    ") ENGINE=InnoDB")

table['payement'] = (
    "  CREATE TABLE `payement` ("
    "  `payementCode` int(11) NOT NULL,"
    "  `employerId` int(11) NOT NULL,"
    "  `employerAmount` varchar(45) NOT NULL,"
    "  PRIMARY KEY (`payementCode`),"
    "  CONSTRAINT `employerId` FOREIGN KEY (`employerId`)"
    "  REFERENCES `employer` (`employerId`) ON DELETE NO ACTION ON UPDATE NO ACTION"
    ") ENGINE = InnoDB")


# A loop to iterate through the table
for the_name_of_table in table:
    table_description = table[the_name_of_table]

    try:
        print("create table {} :".format(the_name_of_table), end='')
        cursor.execute(table_description)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("table exists already")
        else:
            print(err.msg)
    else:
        print("it is created now!")

# opening the files

employees_table = open("employee.csv")
employer_table = open("employer.csv")
bank_table = open("bank.csv")
payement_table = open("payement.csv")

# read the files

read_csv_Files = reader(employees_table)
# readind the next row
next(read_csv_Files)

# now we inserte the values by specifying the number of colums

for row in read_csv_Files:
    try:
        cursor.execute("INSERT INTO employee VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                       [row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8]])
        cnx.commit()
    except:
        continue


# reading the mnager file

read_csv_Files = reader(employer_table)
next(read_csv_Files)

for row in read_csv_Files:
    try:
        cursor.execute("INSERT INTO employer VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                       [row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8]])
        cnx.commit()
    except:
        continue


read_csv_Files = reader(bank_table)
next(read_csv_Files)


for row in read_csv_Files:
    try:
        cursor.execute("INSERT INTO bank VALUES (%s,%s,%s,%s,%s,%s)",
                       [row[0], row[1], row[2], row[3], row[4], row[5]])
        cnx.commit()
    except:
        continue

read_csv_Files = reader(payement_table)
next(read_csv_Files)

for row in read_csv_Files:
    try:
        cursor.execute("INSERT INTO payement VALUES (%s,%s,%s)",
                       [row[0], row[1], row[2]])
        cnx.commit()
    except:
        continue

# UI
while True:
    print("__________________________________________________________\n")

    select = input("1. Female employees who have larger salary than other employers.\n" +
                   "2. Getting the average salary based on gender.\n" +
                   "3. Print all from table bank\n" +
                   "4. CROSS JOIN query.\n" +
                   "5. CREATE VIEW new AS SELECT CONCAT\n" +
                   "6.Inner join\n" +
                   "0. Quit\n" +
                   "-----------------------------------------------------------------\n" +
                   "Please choose one option: ")

    if select == "1":
        # using two tables to compare salaries
        print("Comparing salaries\n")
        query1 = """SELECT employer.employerId, employer.FirstName,\
        employer.LastName, employee.employeeId, employee.FirstName, \
        employee.LastName
        FROM employer, employee
        WHERE employee.payementAmount > employer.payementAmount AND employee.Gender = 'F'"""
        cursor.execute(query1)
        my_result = cursor.fetchall()

        for row in my_result:
            print(row)

    elif select == "2":
        # using Aggregation AVG()
        print("Aggregation\n")
        query21 = """SELECT AVG(employee.payementAmount) FROM employee WHERE employee.Gender = 'F'"""
        cursor.execute(query21)
        my_result = cursor.fetchall()
        for row in my_result:
            print("Female average salary: ", row)
        query22 = """SELECT AVG(employee.payementAmount) FROM employee WHERE employee.Gender = 'M'"""
        cursor.execute(query22)
        my_result = cursor.fetchall()
        for row in my_result:
            print("Male average salary: ", row)

    elif select == "3":
        # select all from table
        print("Select all\n")
        query3 = "SELECT * FROM bank"
        cursor.execute(query3)
        my_result = cursor.fetchall()
        for row in my_result:
            print(row)

    elif select == "4":
        print("Cross join\n")
        query4 = """SELECT employer.employerId, employer.FirstName,\
        employer.LastName, employee.employeeId,\
        employee.FirstName, employee.LastName
        FROM employer CROSS JOIN employee \
        WHERE employer.payementAmount = employee.payementAmount"""
        cursor.execute(query4)
        my_result = cursor.fetchall()
        for row in my_result:
            print(row)

    elif select == "5":
        print("VIEW\n")
        # Creating view and concatinatin two tables
        query5 = """CREATE VIEW new AS SELECT CONCAT('boss', employerId)\
            AS boss, employer.FirstName, employer.LastName, \
            'employer' AS status
            FROM employer
            UNION SELECT CONCAT('emp', employeeId) AS emp, \
            employee.FirstName, employee.LastName, 'employee' AS status
            FROM employee"""
        try:
            cursor.execute(query5)
            cnx.commit()
        except:
            cnx.rollback()
        cursor.execute("SELECT * FROM new ORDER BY FirstName")

        for x in cursor:
            print(x)

    elif select == "6":
        # using inner join
        print("inner join\n")
        query6 = """SELECT * 
                    FROM payement
                    INNER JOIN employee
                    ON payement.payementCode = employee.employeeId"""
        cursor.execute(query6)
        my_result = cursor.fetchall()
        for row in my_result:
            print(row)

    elif select == "0":
        cnx.close()
        break