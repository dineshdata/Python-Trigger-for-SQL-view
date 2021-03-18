# Python-Trigger-for-SQL-view
Creating a Python Script equivalent to SQL Trigger to refresh SQL view. 

# Objective: 
  Create a python script which acts as SQL trigger to refresh SQL view. This script will connect to the database 
  and compare the data of surveystructure table with current and previous set of data 
  and create or refresh a SQL view whenever there is a change in the data.

There are four python files in the project:
      1. config.py
      2. classes_and_functions.py
      3. sqlstrings.py
      4. main.py

# Config.Py
  Holds the details about the environment variables and database credentials. 
  To intialize most of the variables needed for the project
  
# sqlstrings.py
  Creating variables for each SQL string based on their functionality and assign it.
  
# classes_and_functions.py
  Holds different classes and function which can acts a module and to use it in main script 
  by importing and creating objects from them for better readability and flexibility
  
# main.py
  Core functionality of the project to attain the objective using all the components and  files imported.
  
  
# Algorithm:
  # Step 1 : 
  Connect to the SQL server database and retrieve the data from the table 'Surveystructure' and store it in a pandas dataframe as current version    
  # Step 2 :
  Check for the availablity of previous version of Survery structure table in our local data folder. If available go to Step 3, if not go to Step 5    
  # Step 3 :
  Import the previous version of Survey structure data into another pandas Dataframe and compare the data between current version and previous version.
  If matches go to Step 4, else go to Step 5.
  # Step 4 :
  Since there is no change in previous and current version there is no need for refresh. So exit the code immediately
  # Step 5 :
  Whenever the data mismatches or previous version not available, SQL view should be refreshed or created.
  # Step 6 :
  Write the latest surverystructure data in the data folder for future validations
    
