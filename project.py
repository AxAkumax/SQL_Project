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
            passwd=user_password,
            database=db_name
        )
        print("MySQL Database connection successful")
    except Error as e:
        print(f"The error '{e}' occurred")

    return connection



# Function to execute a query in the database
def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query executed successfully")
    except Error as e:
        print(f"The error '{e}' occurred")



#-----------------------------------------------------------------------------------------------Function 1 : to import data from CSV files into the database ---------------------------------------------------------------------------------------------------------------
def import_data(folder_name, connection):
    cursor = connection.cursor()

    # List of table names derived from the provided CSV files
    table_names = ['Admins', 'Courses', 'Emails', 'Machines', 'Projects', 'Students','used', 'manage', 'Users']

    #----------------------- drop tables that already exist in the database --------------------------------------------------
    #To avoid foreign key constraint errors when dropping tables
    # Define the order of table deletion, ensuring dependent tables are deleted first
    tables_to_drop_in_order = [
        'Had','used', 'manage',
        'projects', 'emails', 'students', 'admins',
        'machines', 'courses', 'users'
    ]

    # Disable foreign key checks to facilitate table deletion
    cursor.execute("SET FOREIGN_KEY_CHECKS=0;")

    # Delete tables in the defined order
    for table in tables_to_drop_in_order:
        cursor.execute(f"DROP TABLE IF EXISTS {table};")
        print(f"Table {table} dropped successfully.")

    # Re-enable foreign key checks
    cursor.execute("SET FOREIGN_KEY_CHECKS=1;")

    #------------------------------------------------End of drop tables that already exist in the database --------------------------------------------------
    
    
    
    #----------------------- ------------------------Create new tables based on DDLs --------------------------------------------------
    create_table_queries = {
        'Users': """CREATE TABLE IF NOT EXISTS users (UCINetID char(50) NOT NULL, FirstName varchar(50), MiddleName varchar(50), LastName varchar(50), PRIMARY KEY(UCINetID));""",
        'Emails': """CREATE TABLE IF NOT EXISTS emails (UCINetID char(50), email_address varchar(50), PRIMARY KEY(UCINetID, email_address), FOREIGN KEY(UCINetID) REFERENCES users(UCINetID) ON DELETE CASCADE);""",
        'Students': """CREATE TABLE IF NOT EXISTS students (UCINetID char(50) NOT NULL, PRIMARY KEY(UCINetID), FOREIGN KEY(UCINetID) REFERENCES users(UCINetID));""",
        'Admins': """CREATE TABLE IF NOT EXISTS admins (admin_UCINetID char(50) NOT NULL, PRIMARY KEY(admin_UCINetID), FOREIGN KEY(admin_UCINetID) REFERENCES users(UCINetID));""",
        'Courses': """CREATE TABLE IF NOT EXISTS courses (course_id char(50) NOT NULL, title varchar(255), quarter varchar(20), PRIMARY KEY(course_id));""",
        'Projects': """CREATE TABLE IF NOT EXISTS projects (project_id char(50) NOT NULL, course_id char(50) NOT NULL, project_name varchar(100), project_description TEXT, PRIMARY KEY(project_id), FOREIGN KEY(course_id) REFERENCES courses(course_id));""",
        'Machines': """CREATE TABLE IF NOT EXISTS machines (machine_id char(50) NOT NULL, hostname varchar(255), IP_address varchar(15), operational_status varchar(50), location varchar(255), PRIMARY KEY(machine_id));""",
        'Had': """CREATE TABLE IF NOT EXISTS had(project_id char(50), course_id char(50), PRIMARY KEY(project_id), FOREIGN KEY(project_id) REFERENCES projects(project_id), FOREIGN KEY(course_id) REFERENCES courses(course_id)); """,
        'Used': """CREATE TABLE IF NOT EXISTS used (UCINetID char(50), project_id char(50), machine_id char(50), start_date date, end_date date, PRIMARY KEY(UCINetID, project_id, machine_id), FOREIGN KEY(UCINetID) REFERENCES users(UCINetID), FOREIGN KEY(project_id) REFERENCES projects(project_id), FOREIGN KEY(machine_id) REFERENCES machines(machine_id));""",
        'Manage': """CREATE TABLE IF NOT EXISTS manage (admin_UCINetID char(50), machine_id char(50), PRIMARY KEY(admin_UCINetID, machine_id), FOREIGN KEY(admin_UCINetID) REFERENCES Admins(admin_UCINetID), FOREIGN KEY(machine_id) REFERENCES machines(machine_id));"""
    }

    for table, query in create_table_queries.items():
        cursor.execute(query)
        print(f"Table {table} created successfully.")

    #-----------------------------------------------------END of Creating new tables based on DDLs -----------------------------------------------------------
    
    
    
    #----------------------------------------------------- Insert data for each table --------------------------------------------------
    # First, process the CSV file for the 'users' table
    with open(os.path.join(folder_name, 'users.csv'), 'r') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            insert_query = "INSERT INTO users VALUES (%s, %s, %s, %s);"
            cursor.execute(insert_query, tuple(row))
        print("Data inserted into Users table successfully.")

    # Specify the tables and their corresponding CSV files to be processed in order
    ordered_tables = ['Admins', 'students', 'emails', 'courses', 'projects', 'machines', 'used','manage']
    
    #Disable foreign key checks
    cursor.execute("SET FOREIGN_KEY_CHECKS=0;")
    
    # Process the remaining CSV files in the specified order
    for table in ordered_tables:
        with open(os.path.join(folder_name, table + '.csv'), 'r') as file:
            csv_reader = csv.reader(file)
            for row in csv_reader:
                # Dynamically construct the INSERT INTO statement based on the structure of the CSV file
                column_count = len(row)
                insert_query = f"INSERT INTO {table} VALUES ({', '.join(['%s'] * column_count)});"
                cursor.execute(insert_query, tuple(row))
            print(f"Data inserted into {table.capitalize()} table successfully.")
    #----------------------------------------------------- End oF Insert data for each table --------------------------------------------------
    
    # Re-enable foreign key checks
    cursor.execute("SET FOREIGN_KEY_CHECKS=1;")
    
    # Count records in each table and print the counts
    for table in table_names:
        cursor.execute(f"SELECT COUNT(*) FROM {table};")
        count = cursor.fetchone()[0]
        print(f"Number of records in {table}: {count}")

    connection.commit()
    cursor.close()
#-----------------------------------------------------------------------------------------------End of Function 1 to import data from CSV files into the database ---------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------Function 2 :to insert a new student into the database -------------------------------------------------------------------------------------------------------------------------------------#
def insert_student(connection, UCINetID, email, first, middle, last):
    cursor = connection.cursor()
    success = True  # Assume success unless an error occurs
    try:
        # Insert into Users table
        user_insert_query = "INSERT INTO users (UCINetID, FirstName, MiddleName, LastName) VALUES (%s, %s, %s, %s);"
        cursor.execute(user_insert_query, (UCINetID, first, middle, last))
        
        # Insert into Students table
        student_insert_query = "INSERT INTO students (UCINetID) VALUES (%s);"
        cursor.execute(student_insert_query, (UCINetID,))
        
        # Insert into Emails table
        email_insert_query = "INSERT INTO emails (UCINetID, email_address) VALUES (%s, %s);"
        cursor.execute(email_insert_query, (UCINetID, email))

        connection.commit()  # Commit the transaction
        print("Success")
    except Error as e:
        print(f"The error '{e}' occurred")
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
        email_insert_query = "INSERT INTO emails (UCINetID, email_address) VALUES (%s, %s);"
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
        delete_student_query = "DELETE FROM Student WHERE UCINetID = %s"
        delete_user_query = "DELETE FROM User WHERE UCINetID = %s"
        
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
        machine_insert_query = "INSERT INTO Machine (MachineID, hostname, IPAddr, status, location) VALUES (%s, %s, %s, %s, %s)"
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
        query = "INSERT INTO UseRecord (ProjId, UCINetID, MachineID, StartDate, EndDate) VALUES (%s, %s, %s, %s, %s)"
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



# Main function to parse command-line arguments and call the appropriate function
def main():
    if len(sys.argv) < 2:
        print("Usage: python3 project.py <function name> [parameters]")
        return
    
    command = sys.argv[1]
    connection = create_database_connection("localhost", "root", "zjc760313", "116project")  # Remember Update with our own credentials

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
    # Add more elif blocks here for other commands like insertStudent, addEmail, etc.
    else:
        print("Invalid command")

if __name__ == "__main__":
    main()
