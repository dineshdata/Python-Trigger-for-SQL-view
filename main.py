#Importing base dependencies(system functions) which helps to install other dependencies
import subprocess
import sys

#Installing all the dependencies as per the requirements file
subprocess.check_call([sys.executable, "-m", "pip", "install", "-r","requirements.txt"])


#importing all the necessary classes and functions from the classes_and_functions file
from classes_and_functions import open_sql_connection
from classes_and_functions import Sql_Project
import config

## Below algorithm will refresh the view when there is a change in the SurveyStructure
if __name__ =='__main__':
    try:
        # Opening a SQL Connection
        sql_conn = open_sql_connection()
        # Instanciating the Sql_Project class to the Object trigger
        trigger = Sql_Project(conn=sql_conn)
        # Checking for the change in SurveyStructue Data by comparing old data in CSV and new data in DB, if not matched return True
        if  trigger.compare_SurveyStructure_data():
            # Refreshing the View either by creating or altering when change detected
            is_view_refreshed = trigger.sql_trigger_view()
            if is_view_refreshed:
                print("{} view refreshed successfully in Database since Survey structure changed".format(config.view_name))
            else:
                print("Error while refreshing view {}".format(view_name))
            trigger.refresh_csv_view()
            trigger.refresh_csv_surveystructure()
        else:
            # When no Change detected in Survery Structure, scripts do nothing"
            print("No Change in SurveryStructure Data hence view refresh not required")

    except Excpetion as e:
        # Printing any exceptions found during execution
        print(e.message)
    finally:
        # Closing the SQL Connection at the end of the script
        sql_conn.close()
        print("Sql Connection Closed Successfully")


