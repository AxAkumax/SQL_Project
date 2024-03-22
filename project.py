
import sys
import csv
import os
import mysql.connector
from mysql.connector import Error

# Function to connect to the MySQL database
def create_database_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            password=user_password,
            database=db_name
        )
        
        #print("MySQL Database connection successful")
    except Error as e:
        print(f"The error '{e}' occurred")

    return connection



# Function to execute a query in the database
def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        #print("Query executed successfully")
    except Error as e:
        print(f"The error '{e}' occurred")



#-----------------------------------------------------------------------------------------------Function 1 : to import data from CSV files into the database ---------------------------------------------------------------------------------------------------------------
def import_data(folder_name, connection):

    cursor = connection.cursor()

    # List of table names derived from the provided CSV files
    table_names = ['Admins', 'Courses', 'Emails', 'Machines', 'Projects', 'Students','StudentUse', 'Manage', 'Users']

    #----------------------- drop tables that already exist in the database --------------------------------------------------
    #To avoid foreign key constraint errors when dropping tables
    # Define the order of table deletion, ensuring dependent tables are deleted first
    tables_to_drop_in_order = [
        'Had','StudentUse', 'Manage',
        'Projects', 'Emails', 'Students', 'Admins',
        'Machines', 'Courses', 'Users'
    ]

    # Disable foreign key checks to facilitate table deletion
    cursor.execute("SET FOREIGN_KEY_CHECKS=0;")

    # Delete tables in the defined order
    for table in tables_to_drop_in_order:
        cursor.execute(f"DROP TABLE IF EXISTS `{table}`;")
        #print(f"Table `{table}` dropped successfully.")

    # Re-enable foreign key checks
    cursor.execute("SET FOREIGN_KEY_CHECKS=1;")

    #------------------------------------------------End of drop tables that already exist in the database --------------------------------------------------
    
    
    
    #----------------------- ------------------------Create new tables based on DDLs --------------------------------------------------
    create_table_queries = {
        'Users': """CREATE TABLE IF NOT EXISTS Users (UCINetID char(50) NOT NULL, FirstName varchar(50), MiddleName varchar(50), LastName varchar(50), PRIMARY KEY(UCINetID));""",
        'Emails': """CREATE TABLE IF NOT EXISTS Emails (UCINetID char(50), email_address varchar(50), PRIMARY KEY(UCINetID, email_address), FOREIGN KEY(UCINetID) REFERENCES Users(UCINetID) ON DELETE CASCADE);""",
        'Students': """CREATE TABLE IF NOT EXISTS Students (UCINetID char(50) NOT NULL, PRIMARY KEY(UCINetID), FOREIGN KEY(UCINetID) REFERENCES Users(UCINetID));""",
        'Admins': """CREATE TABLE IF NOT EXISTS Admins (admin_UCINetID char(50) NOT NULL, PRIMARY KEY(admin_UCINetID), FOREIGN KEY(admin_UCINetID) REFERENCES Users(UCINetID));""",
        'Courses': """CREATE TABLE IF NOT EXISTS Courses (course_id char(50) NOT NULL, title varchar(255), quarter varchar(20), PRIMARY KEY(course_id));""",
        'Projects': """CREATE TABLE IF NOT EXISTS Projects (project_id char(50) NOT NULL, project_name varchar(100), project_description TEXT, course_id char(50) NOT NULL,  PRIMARY KEY(project_id), FOREIGN KEY(course_id) REFERENCES Courses(course_id));""",
        'Machines': """CREATE TABLE IF NOT EXISTS Machines (machine_id char(50) NOT NULL, hostname varchar(255), IP_address varchar(15), operational_status varchar(50), location varchar(255), PRIMARY KEY(machine_id));""",
        'Had': """CREATE TABLE IF NOT EXISTS Had(project_id char(50), course_id char(50), PRIMARY KEY(project_id), FOREIGN KEY(project_id) REFERENCES Projects(project_id), FOREIGN KEY(course_id) REFERENCES Courses(course_id)); """,
        'StudentUse': """CREATE TABLE IF NOT EXISTS StudentUse (project_id char(50), UCINetID char(50), machine_id char(50), start_date date, end_date date, PRIMARY KEY(UCINetID, project_id, machine_id), FOREIGN KEY(UCINetID) REFERENCES Users(UCINetID), FOREIGN KEY(project_id) REFERENCES Projects(project_id), FOREIGN KEY(machine_id) REFERENCES Machines(machine_id));""",
        'Manage': """CREATE TABLE IF NOT EXISTS Manage (admin_UCINetID char(50), machine_id char(50), PRIMARY KEY(admin_UCINetID, machine_id), FOREIGN KEY(admin_UCINetID) REFERENCES Admins(admin_UCINetID), FOREIGN KEY(machine_id) REFERENCES Machines(machine_id));"""
    }

    for table, query in create_table_queries.items():
        cursor.execute(query)
        #print(f"Table `{table}` created successfully.")

    #-----------------------------------------------------END of Creating new tables based on DDLs -----------------------------------------------------------
    
    
    
    #----------------------------------------------------- Insert data for each table --------------------------------------------------
    # First, process the CSV file for the 'users' table

    with open(os.path.join(folder_name, 'users.csv'), 'r') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            insert_query = "INSERT INTO Users VALUES (%s, %s, %s, %s);"
            cursor.execute(insert_query, tuple(row))
        #print("Data inserted into Users table successfully.")

    # Specify the tables and their corresponding CSV files to be processed in order
    ordered_tables = ['Admins', 'Students', 'Emails', 'Courses', 'Projects', 'Machines', 'StudentUse','Manage']
    
    #Disable foreign key checks
    cursor.execute("SET FOREIGN_KEY_CHECKS=0;")
    
    # Process the remaining CSV files in the specified order
    for table in ordered_tables:
        name = table 
        if table=="StudentUse":
            name = "use"
        with open(os.path.join(folder_name, f'{name.lower()}.csv'), 'r') as file:
            csv_reader = csv.reader(file)
            for row in csv_reader:
                # Dynamically construct the INSERT INTO statement based on the structure of the CSV file
                column_count = len(row)
                insert_query = f"INSERT INTO `{table}` VALUES ({', '.join(['%s'] * column_count)});"
                cursor.execute(insert_query, tuple(row))
            #print(f"Data inserted into {table.capitalize()} table successfully.")
    #----------------------------------------------------- End oF Insert data for each table --------------------------------------------------
    
    # Re-enable foreign key checks
    cursor.execute("SET FOREIGN_KEY_CHECKS=1;")
    
    # Count records in each table and print the counts
    new_table_names = ['Users', 'Machines', 'Courses']
    record_counts = []
    for table in new_table_names:
        cursor.execute(f"SELECT COUNT(*) FROM `{table}`;")
        count = cursor.fetchone()[0]
        record_counts.append(str(count))
    print(",".join(record_counts))

    connection.commit()
    cursor.close()
#-----------------------------------------------------------------------------------------------End of Function 1 to import data from CSV files into the database ---------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------Function 2 :to insert a new student into the database -------------------------------------------------------------------------------------------------------------------------------------#
def insert_student(connection, UCINetID, email, first, middle, last):
    cursor = connection.cursor()
    success = True  # Assume success unless an error occurs
    try:
        # Insert into Users table
        user_insert_query = "INSERT INTO Users (UCINetID, FirstName, MiddleName, LastName) VALUES (%s, %s, %s, %s);"
        cursor.execute(user_insert_query, (UCINetID, first, middle, last))
        
        # Insert into Students table
        student_insert_query = "INSERT INTO Students (UCINetID) VALUES (%s);"
        cursor.execute(student_insert_query, (UCINetID,))
        
        # Insert into Emails table
        email_insert_query = "INSERT INTO Emails (UCINetID, email_address) VALUES (%s, %s);"
        cursor.execute(email_insert_query, (UCINetID, email))

        connection.commit()  # Commit the transaction
        print("Success")
    except Error as e:
        #print(f"The error '{e}' occurred")
        #print("Fail")
        success = False  # Set success to False if an error occurs
        connection.rollback()  # Rollback the transaction

    cursor.close()
    return success

# --------------------------------------------------------------------------------------End of Function2 to insert a new student into the database ---------------------------------------------------------------------------------------------------------------------------------------------



#--------------------------------------------------------------------------------------- Function 3 : to add an email to a user ----------------------------------------------------------------------------------------------------------------------------------------------------#
def add_email(connection, UCINetID, email):
    cursor = connection.cursor()
    success = True  # Assume success unless an error occurs
    try:
        # Insert into Emails table
        email_insert_query = "INSERT INTO Emails (UCINetID, email_address) VALUES (%s, %s);"
        cursor.execute(email_insert_query, (UCINetID, email))

        connection.commit()  # Commit the transaction
        print("Success")
    except Error as e:
        print(f"The error '{e}' occurred")
        success = False  # Set success to False if an error occurs
        connection.rollback()  # Rollback the transaction

    cursor.close()
    return success
#--------------------------------------------------------------------------------------- END of Function 3 : to add an email to a user ----------------------------------------------------------------------------------------------------------------------------------------------------#

#--------------------------------------------------------------------------------------- Function 4 : delete student ----------------------------------------------------------------------------------------------------------------------------------------------------#
def delete_student(connection, UCINetID):
    cursor = connection.cursor()
    success = True
    try:
        delete_student_query = "DELETE FROM Students WHERE UCINetID = %s"
        delete_user_query = "DELETE FROM Users WHERE UCINetID = %s"
        
        # Execute delete operation in Student table first to maintain referential integrity.
        cursor.execute(delete_student_query, (UCINetID,))
        cursor.execute(delete_user_query, (UCINetID,))
        
        connection.commit()
        print("Success" if cursor.rowcount > 0 else "Fail")
    except mysql.connector.Error as err:
        print("Fail")
        print(err)
        success = False
        connection.rollback()

    cursor.close()
    return success
#--------------------------------------------------------------------------------------- END of Function 4 : delete student ----------------------------------------------------------------------------------------------------------------------------------------------------#

#--------------------------------------------------------------------------------------- Function 5 : Insert machine ----------------------------------------------------------------------------------------------------------------------------------------------------#
def insert_machine(connection, machine_id, hostname, ip_addr, status, location):
    cursor = connection.cursor()
    success = True
    try:
        machine_insert_query = """INSERT INTO Machines (machine_id, hostname, IP_address, operational_status, location) VALUES (%s, %s, %s, %s, %s)"""
        cursor.execute(machine_insert_query, (machine_id, hostname, ip_addr, status, location))
        connection.commit()
        print("Success" if cursor.rowcount > 0 else "Fail")
    except mysql.connector.Error as err:
        print("Fail")
        print(err)
        success = False
        connection.rollback()
    
    cursor.close()
    return success
#--------------------------------------------------------------------------------------- END of Function 5 : Insert machine ----------------------------------------------------------------------------------------------------------------------------------------------------#

#--------------------------------------------------------------------------------------- Function 6 : Insert use record ----------------------------------------------------------------------------------------------------------------------------------------------------#
def insert_use(connection, proj_id, ucinetid, machine_id, start_date, end_date):
    cursor = connection.cursor()
    success = True
    try:
        query = """INSERT INTO StudentUse (project_id, UCINetID, machine_id, start_date, end_date) VALUES (%s, %s, %s, %s, %s)"""
        cursor.execute(query, (proj_id, ucinetid, machine_id, start_date, end_date))
        connection.commit()
        print("Success" if cursor.rowcount > 0 else "Fail")
    except mysql.connector.Error as err:
        print("Fail")
        print(err)
        success = False
        connection.rollback()

    cursor.close()
    return success
#--------------------------------------------------------------------------------------- END of Function 6 : Insert use record ----------------------------------------------------------------------------------------------------------------------------------------------------#



#--------------------------------------------------------------------------------------- Function 7 : to update a Course  ----------------------------------------------------------------------------------------------------------------------------------------------------#


def updateCourse(connection, courseID, title):
    cursor = connection.cursor()
    success = True  # Assume success unless an error occurs
    try:
        # Insert into Emails table
        title_update_query = "UPDATE Courses SET title = (%s) WHERE course_id = (%s);"
        cursor.execute(title_update_query, (title, courseID))

        connection.commit()  # Commit the transaction
        print("Success")
    except Error as e:
        print("Fail")
        # print(f"The error '{e}' occurred")
        success = False  # Set success to False if an error occurs
        connection.rollback()  # Rollback the transaction

    cursor.close()
    return success


#--------------------------------------------------------------------------------------- END of Function 7 ----------------------------------------------------------------------------------------------------------------------------------------------------#



#--------------------------------------------------------------------------------------- Function 8 :listCourse  ----------------------------------------------------------------------------------------------------------------------------------------------------#

def listCourse(connection, UCINetID):
    cursor = connection.cursor()
    
    try:
        list_course_query = """
        SELECT DISTINCT C.course_id, C.title, C.quarter
        FROM Courses C, StudentUse U, Projects P
        WHERE U.project_id = P.project_id 
        AND P.course_id = C.course_id 
        AND U.UCINetID = '{0}'
        ORDER BY C.course_id ASC; 
        """.format(UCINetID)

        # Execute the query
        #print("Trying...")
        #print(cursor)
        #print('coonection: ', connection)
        #print(list_course_query)
        cursor.execute(list_course_query)
        #print("Tried...")
        # Fetch the results
        results = cursor.fetchall()
        #print("Results: ", results)
        list_course = []
        for row in results:
            list_course.append(','.join(str(col) for col in row))
            #print(list_course)
        print("\n".join(list_course))

        return True  # Return True to indicate success
    
    except Exception as e:
        print("Fail")
        print(f"The error '{e}' occurred")
        return False  # Return False in case of failure

    finally:
        cursor.close()


#--------------------------------------------------------------------------------------- END of Function 8 ----------------------------------------------------------------------------------------------------------------------------------------------------#

#--------------------------------------------------------------------------------------- Function 9 :popularCourse  ----------------------------------------------------------------------------------------------------------------------------------------------------#

def popularCourse(connection, num):
    cursor = connection.cursor()

    try:
        course_query = """ 
        SELECT C.course_id, C.title, COUNT(*) AS studentCount
        FROM Courses C 
        JOIN Projects P ON C.course_id = P.course_id
        JOIN StudentUse U ON P.project_id = U.project_id
        GROUP BY C.course_id, C.title
        ORDER BY studentCount DESC, C.course_id DESC
        LIMIT %s;
        """
        # Execute the query
        #print("Number: ", num, type(num)) 
        cursor.execute(course_query, (int(num), ))

        # Fetch the results
        results = list(cursor.fetchall()) 
        #print(results)  

        pop_course = []
        for row in results:
            pop_course.append(','.join(str(col) for col in row))
            #print(list_course)
        print("\n".join(pop_course))

        return True  # Return True to indicate success
    
    except Exception as e:
        print(f"The error '{e}' occurred")
        return False  # Return False in case of failure

    finally:
        cursor.close()


#--------------------------------------------------------------------------------------- END of Function 9 ----------------------------------------------------------------------------------------------------------------------------------------------------#



#Given a machine ID, find all administrators of that machine. List the emails of those administrators. Ordered by netid ascending.
def adminEmail(connection, machineId):
    cursor = connection.cursor()
    try:
        adminEmail_query = """
       SELECT AD.admin_UCINetID, U.FirstName, U.MiddleName, U.LastName, GROUP_CONCAT(UE.email_address SEPARATOR ';') AS email_addresses
        FROM Admins AD
        JOIN Users U ON AD.admin_UCINetID = U.UCINetID
        JOIN Emails UE ON AD.admin_UCINetID = UE.UCINetID 
        WHERE AD.admin_UCINetID IN (
            SELECT A.admin_UCINetID
            FROM Admins A
            JOIN Manage AMM ON A.admin_UCINetID = AMM.admin_UCINetID
            WHERE AMM.machine_id = %s
        )
        GROUP BY AD.admin_UCINetID
        ORDER BY AD.admin_UCINetID ASC;
        """
        cursor.execute(adminEmail_query, (machineId,))
        rows = cursor.fetchall()
        result = []
        for row in rows:
            admin_info = ",".join(map(str, row[:4]))  # Combine admin information
            emails = row[4].split(';')  # Split email addresses
            result.append(admin_info + "," + ";".join(emails))  # Combine admin info and emails
       
        list1= "\n".join(result)  # Join all admin information
        print(list1)
        return list1
        
    except Exception as e:
        print(f"The error '{e}' occurred")
        return False  # Return False in case of failure
    finally:
        cursor.close()

#--------------------------------------------------------------------------------------- END of Function 10 ----------------------------------------------------------------------------------------------------------------------------------------------------#


from datetime import datetime

def activeStudents(connection, machineid, start_date, end_date, N):
    cursor = connection.cursor()
    try:
        activeStudents_query = """
            SELECT U.UCINetID, U.FirstName, U.MiddleName, U.LastName
            FROM Users U
            JOIN Students S ON U.UCINetID = S.UCINetID
            JOIN StudentUse ON U.UCINetID = StudentUse.UCINetID
            JOIN Machines M ON StudentUse.machine_id = M.machine_id
            WHERE StudentUse.machine_id = %s
            AND StudentUse.start_date >= %s
            AND StudentUse.end_date <= %s
            AND M.operational_status = 'Active'
            GROUP BY U.UCINetID
            HAVING COUNT(*) >= %s
            ORDER BY U.UCINetID ASC;
        """
        cursor.execute(activeStudents_query, (machineid, start_date, end_date, N,))
        rows = cursor.fetchall()
        result = "\n".join([",".join(map(str, row[:4])) for row in rows])
        print(result)
        return rows
    except Exception as e:
        print(f"The error '{e}' occurred")
        return False
    finally:
        cursor.close()

# SELECT *
#            FROM `Use`
#            WHERE `Use`.machine_id = %s 
#                 AND `Use`.start_date >= %s 
#                 AND `Use`.end_date <= %s 

#--------------------------------------------------------------------------------------- END of Function 11 ----------------------------------------------------------------------------------------------------------------------------------------------------#

def numMachineUsage(connection, courseId):
    cursor = connection.cursor()
    try:
        numMachineUsage_query = """ 
        SELECT M.machine_id, M.hostname, M.IP_address, IFNULL(COUNT(SU.machine_id), 0) AS count
        FROM Machines M
        LEFT JOIN (
            SELECT DISTINCT machine_id
            FROM StudentUse
            WHERE project_id IN (
                SELECT project_id
                FROM Projects
                WHERE course_id = %s
            )
        ) SU ON M.machine_id = SU.machine_id
        GROUP BY M.machine_id
        ORDER BY M.machine_id DESC;
        """
        cursor.execute(numMachineUsage_query, (courseId,))
        rows = cursor.fetchall()
        result = "\n".join([",".join(map(str, row[:4])) for row in rows])
        print(result)
        return rows

    except Exception as e:
        print(f"The error '{e}' occurred")
        return False  # Return False in case of failure
    finally:
        cursor.close()

#--------------------------------------------------------------------------------------- END of Function 12 ----------------------------------------------------------------------------------------------------------------------------------------------------#

# Main function to parse command-line arguments and call the appropriate function
def main():
    if len(sys.argv) < 2:
        print("Usage: python3 project.py <function name> [parameters]")
        return
    
    command = sys.argv[1]
    #test, password
    connection = create_database_connection("localhost", 'test', 'password', "cs122a")  # Remember Update with our own credentials

    if command == "import":
        if len(sys.argv) != 3:
            print("Usage: python3 project.py import [folderName]")
        else:
            folder_name = sys.argv[2]
            import_data(folder_name, connection)
    elif command == "insertStudent":
        if len(sys.argv) != 7:
            print("Usage: python3 project.py insertStudent [UCINetID] [email] [First] [Middle] [Last]")
        else:
            UCINetID = sys.argv[2]
            email = sys.argv[3]
            first = sys.argv[4]
            middle = sys.argv[5]
            last = sys.argv[6]
            success = insert_student(connection, UCINetID, email, first, middle, last)
            if not success:
                print("Fail")
    elif command == "addEmail":
        if len(sys.argv) != 4:
            print("Usage: python3 project.py addEmail [UCINetID] [email]")
        else:
            UCINetID = sys.argv[2]
            email = sys.argv[3]
            success = add_email(connection, UCINetID, email)
            if not success:
                print("Fail")
    elif command == "deleteStudent":
        if len(sys.argv) != 3:
            print("Usage: python3 project.py deleteStudent [UCINetID]")
        else:
            UCINetID = sys.argv[2]
            delete_student(connection, UCINetID)
    elif command == "insertMachine":
        if len(sys.argv) != 7:
            print("Usage: python3 project.py insertMachine [MachineID] [hostname] [IPAddr] [status] [location]")
        else:
            machine_id = sys.argv[2]
            hostname = sys.argv[3]
            ip_addr = sys.argv[4]
            status = sys.argv[5]
            location = sys.argv[6]
            insert_machine(connection, machine_id, hostname, ip_addr, status, location)
    elif command == "insertUse":
        if len(sys.argv) != 7:
            print("Usage: python3 project.py insertUse [ProjId] [UCINetID] [MachineID] [start] [end]")
        else:
            proj_id = sys.argv[2]
            ucinetid = sys.argv[3]
            machine_id = sys.argv[4]
            start_date = sys.argv[5]
            end_date = sys.argv[6]
            insert_use(connection, proj_id, ucinetid, machine_id, start_date, end_date)

 # Rishika - func 7, 8, 9 
    elif command == "updateCourse":
        if len(sys.argv) != 4:
            print("Usage: python3 project.py updateCourse [CourseId:int] [title:str]")
        else:
            course_id = sys.argv[2]
            title = sys.argv[3]
            success = updateCourse(connection, course_id, title)
            if not success:
                print("Fail")

    elif command == "listCourse":
        if len(sys.argv) != 3:
            print("Usage: python3 project.py listCourse [UCINetID]")
        else:
            ucinetid = sys.argv[2]
            listCourse(connection, ucinetid)

    elif command == "popularCourse":
        if len(sys.argv) != 3:
            print("Usage: python3 project.py popularCourse [N]")
        else:
            num = sys.argv[2]
            popularCourse(connection, num)
    elif command =='adminEmails':
        if len(sys.argv) != 3:
            print("Usage: python3 project.py adminEmails [machineId]")
        else:
            machine_id = sys.argv[2]
            adminEmail(connection, machine_id)
    
    elif command =='activeStudent':
        if len(sys.argv) != 6:
            print("Usage: python3 project.py activeStudent [machineId:int] [N:int] [start_date:Date] [end:Date]")
        else:
            machine_id = sys.argv[2]
            num = sys.argv[3]
            start_date = sys.argv[4]
            end_date = sys.argv[5]
            activeStudents(connection, machine_id, start_date, end_date, num)
    elif command =='machineUsage':
        if len(sys.argv) !=3:
            print("Usage: project.py machineUsage [courseId: int]")
        else:
            course_id = sys.argv[2]
            numMachineUsage(connection, course_id)

    # Add more elif blocks here for other commands like insertStudent, addEmail, etc.
    else:
        print("Invalid command")

if __name__ == "__main__":
    main()