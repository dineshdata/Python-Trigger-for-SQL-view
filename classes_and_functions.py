#Importing all the dependent packages used in below classes and functions

import config
import sqlstrings
import pandas as pd
import pyodbc
import sqlparse
import sys
import os.path



# open_sql_connection function opens a sql connection based on the connection details available in config
def open_sql_connection(server_name=config.server_name,database=config.database,username=config.username,pwd=config.pwd,trusted_connection=config.Trusted_connection):
    """ open_sql_connection(server_name,database,username,pwd,trusted_connection) 
        Opens an SQL Connections based on the parameters available in config file by default
    """
    conn_string_trusted = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER='+config.server_name+';DATABASE='+config.database+';Trusted_Connection=yes'
    conn_string_usrpwd = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER='+config.server_name+';DATABASE='+config.database+';UID='+config.username+';PWD='+config.pwd
    try:
        #check if the connection is trusted else use username and password as given in the config file
        if config.Trusted_connection:
            conn = pyodbc.connect(conn_string_trusted)
        else:
            conn = pyodbc.connect(conn_string_usrpwd)
        print("SQL Connection established Successfully")
        return conn
    #Capturing most of the generic exceptions of failure
    except OSError as err:
        print("OS error: {0}".format(err))
    except ValueError:
        print("Could not convert data to string.")
    except pyodbc.Error as ex:
        sqlstate = ex.args[1]
        print(sqlstate)
    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        raise
    

class Sql_Project:
    """ 
    Sql_Project(conn,view_name,data_path,fname_surveystructure, fname_view_fresh_data, strQueryTemplateForAnswerColumn
    ,strQueryTemplateForNullColumnn, strcurrentQuestionquery, strQueryTemplateOuterUnionQuery)
    Base class which holds many methods compatible to achieve tasks individualy and also used in integration.

    method get_surverystructure_from_db() helps to get the latest data from Surveystructure table from database.
    method is_surveystructure_fileexits() helps to verify if there exists the surverystructure.csv file in the desired folder 
    method compare_SurveyStructure_data() compare the latest Surveystructure data from DB with the CSV file loaded in the previous run
    method generate_sql() helps to generate the sql which reflects the latest data for the view
    method sql_trigger_view() helps to create or alter the existing view when there is change detected in surveystructure data
    method refresh_csv_view() helps to load the latest surveystructure from DB into csv file in the data folder
    method refresh_csv_surveystructure() helps to load the latest view data from DB into csv file in the data folder whenever there is change detected in surveystructure
    """
    def __init__(self,conn,view_name=config.view_name,data_path=config.data_path, \
                 fname_surveystructure=config.fname_surveystructure,fname_view_fresh_data=config.fname_view_fresh_data, \
                 strQueryTemplateForAnswerColumn=sqlstrings.strQueryTemplateForAnswerColumn, \
                 strQueryTemplateForNullColumnn = sqlstrings.strQueryTemplateForNullColumnn, \
                 strcurrentQuestionquery = sqlstrings.strcurrentQuestionquery, \
                 strQueryTemplateOuterUnionQuery = sqlstrings.strQueryTemplateOuterUnionQuery):
        self.conn = conn
        self.view_name = view_name
        self.data_path = data_path
        self.fname_surveystructure = fname_surveystructure
        self.fname_view_fresh_data = fname_view_fresh_data
        self.strQueryTemplateForAnswerColumn = strQueryTemplateForAnswerColumn
        self.strQueryTemplateForNullColumnn = strQueryTemplateForNullColumnn
        self.strcurrentQuestionquery = strcurrentQuestionquery
        self.strQueryTemplateOuterUnionQuery = strQueryTemplateOuterUnionQuery
        
    def get_surverystructure_from_db(self):
        """ returns the latest data from SurveryStructure Table from DB """
        Sql_surverystsructureQuery = 'SELECT * FROM [dbo].[SurveyStructure]'
        df_allSurveyStructureData = pd.read_sql(Sql_surverystsructureQuery, self.conn)
        return df_allSurveyStructureData
    
    def is_surveystructure_fileexits(self):
        """ Check for the survery structure file exists in the data folder and returns False if not else True """
        isfileexists=os.path.isfile(self.data_path+self.fname_surveystructure)
        return isfileexists
    
    def compare_SurveyStructure_data(self):
        """ Compare the data between Database and last saved CSV file and returns True if mismatched or CSV file not exists else False """
        if self.is_surveystructure_fileexits():
            df_newSurveyStructureData = self.get_surverystructure_from_db()
            df_oldSurveyStructureData = pd.read_csv(self.data_path+self.fname_surveystructure)
            diff = df_oldSurveyStructureData.merge(df_newSurveyStructureData, how='outer', suffixes=['', '_'], indicator=True)
            diff_cnt =len(diff[diff['_merge'] != 'both'])
            diff_status = diff_cnt > 0
        else:
            diff_status = True
        return diff_status
    
    def generate_sql(self):
        """ Generates SQL to create or alter the view based on the latest Survery Structure when there is a mismatch """
        SurveyQuery = ' SELECT SurveyId FROM Survey ORDER BY SurveyId'
        df_SurveyCursor = pd.read_sql(SurveyQuery, self.conn)
        len_df_SurveyCursor = len(df_SurveyCursor)
        strFinalQuery =''
        for index,rows in df_SurveyCursor.iterrows():
            strColumnsQueryPart=''
            strCurrentUnionQueryBlock =''
            currentSurveyId = rows['SurveyId']
            currentQuestionquery = self.strcurrentQuestionquery.format(currentSurveyId=currentSurveyId)
            df_currentQuestionCursor = pd.read_sql(currentQuestionquery, self.conn)
            len_df_currentQuestionCursor = len(df_currentQuestionCursor)
            for index1,rows1 in df_currentQuestionCursor.iterrows():
                currentSurveyIdInQuestion,currentQuestionID,currentInSurvey =rows1['SurveyId'],rows1['QuestionId'],rows1['InSurvey']
                if currentInSurvey == 0:
                    strColumnsQueryPart = strColumnsQueryPart+self.strQueryTemplateForNullColumnn.format(currentQuestionID=currentQuestionID)
                elif currentInSurvey == 1:
                    strColumnsQueryPart = strColumnsQueryPart+self.strQueryTemplateForAnswerColumn.format(currentSurveyId=currentSurveyId,currentQuestionID=currentQuestionID)
                if index1+1 != len_df_currentQuestionCursor:
                    strColumnsQueryPart = strColumnsQueryPart + ','
            strCurrentUnionQueryBlock = self.strQueryTemplateOuterUnionQuery.format(strColumnsQueryPart=strColumnsQueryPart,currentSurveyId=currentSurveyId)
            strFinalQuery = strFinalQuery + strCurrentUnionQueryBlock
            if index+1 != len_df_SurveyCursor:
                strFinalQuery = strFinalQuery + ' UNION '

            else:
                strFinalQuery = sqlparse.format(strFinalQuery, reindent=True, keyword_case='upper')
        return strFinalQuery
    
    def sql_trigger_view(self):
        """ create or alter the view in the database when there is change detected in SurveyStructure table """
        strFinalQuery = self.generate_sql()
        strViewQuery_c = ("CREATE  OR ALTER VIEW [dbo].[{view_name}] AS ".format(view_name=self.view_name) + strFinalQuery)
        strViewQuery_c =(sqlparse.format(strViewQuery_c, reindent=True, keyword_case='upper'))
        try:
            cur = self.conn.cursor()
            cur.execute(strViewQuery_c)
            self.conn.commit()
            return True
        except:
            return False
        
    def refresh_csv_view(self):
        """ load the latest Survey Structure data into CSV file whenever there is a change detected in the structure """
        strViewQuery_l = "SELECT * FROM [dbo].[{view_name}] ".format(view_name = self.view_name)
        df_viewData = pd.read_sql(strViewQuery_l, self.conn)
        df_viewData.to_csv(self.data_path+self.fname_view_fresh_data,index=False)
        return True
    
    def refresh_csv_surveystructure(self):
        """ load the latest view data into CSV file after the view is refreshed with the latest surveystructure"""
        df_SurveryStructureData = self.get_surverystructure_from_db()
        df_SurveryStructureData.to_csv(self.data_path+self.fname_surveystructure,index=False)
        return True