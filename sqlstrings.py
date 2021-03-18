#Intializing all the SQL Strings necessary to generate the Desired SQL to create view.

strQueryTemplateForAnswerColumn = ' \
			COALESCE( \
				( \
					SELECT a.Answer_Value \
					FROM Answer as a \
					WHERE \
						a.UserId = u.UserId \
						AND a.SurveyId = {currentSurveyId} \
						AND a.QuestionId = {currentQuestionID}\
				), -1) AS ANS_Q{currentQuestionID} '

strQueryTemplateForAnswerColumn=strQueryTemplateForAnswerColumn.replace('\t','')

strQueryTemplateForNullColumnn = ' NULL AS ANS_Q{currentQuestionID} '

strcurrentQuestionquery = """SELECT *
					FROM
					(
						SELECT
							SurveyId,
							QuestionId,
							1 as InSurvey
						FROM
							SurveyStructure
						WHERE
							SurveyId = {currentSurveyId}
						UNION
						SELECT 
							{currentSurveyId} as SurveyId,
							Q.QuestionId,
							0 as InSurvey
						FROM
							Question as Q
						WHERE NOT EXISTS
						(
							SELECT *
							FROM SurveyStructure as S
							WHERE S.SurveyId = {currentSurveyId} AND S.QuestionId = Q.QuestionId
						)
					) as t
					ORDER BY QuestionId"""

strcurrentQuestionquery = strcurrentQuestionquery.replace('\t','')

strQueryTemplateOuterUnionQuery = ' \
			SELECT \
					UserId \
					, {currentSurveyId} as SurveyId \
					, {strColumnsQueryPart} \
			FROM \
				[User] as u \
			WHERE EXISTS \
			( \
					SELECT * \
					FROM Answer as a \
					WHERE u.UserId = a.UserId \
					AND a.SurveyId = {currentSurveyId} \
			) \
	'

strQueryTemplateOuterUnionQuery = strQueryTemplateOuterUnionQuery.replace('\t','')