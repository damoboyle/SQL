#This Analysis was conducted using Google's CoLab

! pip install -U pandasql

import pandas as pd
from pandasql import sqldf
import os

directory = "/content/"
visits = pd.read_csv(os.path.join(directory, "visits.csv"))
subscriptions = pd.read_csv(os.path.join(directory, "subscriptions.csv"))
execute_query = lambda query: sqldf(query, globals())
preview_query_result = lambda query: execute_query(query).head()
save_query_result_to_csv = lambda query, path: execute_query(query).to_csv(path)

query1 = """
SELECT region, SUM(revenue) AS revenue
FROM visits v JOIN subscriptions s ON v.account_id=s.account_id
GROUP BY region
ORDER BY revenue DESC
"""
execute_query(query1)

query2 = """
SELECT channel, ROUND(SUM(revenue)/COUNT(), 2) AS '$perVisit'
FROM visits v LEFT JOIN subscriptions s ON v.account_id=s.account_id
GROUP BY channel
ORDER BY `$perVisit` DESC
"""
execute_query(query2)

query3 = """
SELECT landing_page, ROUND(CAST(COUNT(revenue)AS FLOAT)/COUNT()*100, 2) AS 'conversion %'
FROM visits v LEFT JOIN subscriptions s ON v.account_id=s.account_id
GROUP BY landing_page
ORDER BY `conversion %` DESC
"""
execute_query(query3)

query4 = """
SELECT region, landing_page, MAX(convert) AS 'visitConversion %'
FROM (
  SELECT region, landing_page, ROUND(CAST(COUNT(revenue)AS FLOAT)/COUNT()*100, 2) AS convert
  FROM visits v LEFT JOIN subscriptions s ON v.account_id=s.account_id
  GROUP BY region, landing_page
  )
GROUP BY region
ORDER BY `visitConversion %` DESC
"""
execute_query(query4)

query4X = """
SELECT region, landing_page, ROUND(CAST(COUNT(revenue)AS FLOAT)/COUNT()*100, 2) AS 'visitConversion %'
FROM visits v LEFT JOIN subscriptions s ON v.account_id=s.account_id
GROUP BY region, landing_page
"""
execute_query(query4X)

query5 = """
SELECT region, ROUND((
  SELECT COUNT (subscription_start_date) 
  FROM visits v JOIN subscriptions s ON v.account_id=s.account_id
  WHERE trial_start_date is NULL
  GROUP BY region
  )/CAST(COUNT()AS FLOAT)*100, 2) AS 'directSub %'
FROM visits
GROUP BY region
ORDER BY `directSub %` DESC
"""
execute_query(query5)

query6 = """
SELECT region, ROUND(CAST(COUNT(trial_start_date)AS FLOAT)/COUNT()*100, 2) AS 'trial %'
FROM visits v LEFT JOIN subscriptions s ON v.account_id=s.account_id
GROUP BY region
ORDER BY `trial %` DESC
"""
execute_query(query6)

query7 = """
SELECT region, ROUND(CAST(COUNT(revenue)AS FLOAT)/COUNT()*100, 2) AS 'trialConversion %'
FROM visits v JOIN subscriptions s ON v.account_id=s.account_id
WHERE trial_start_date IS NOT NULL
GROUP BY region
ORDER BY `trialConversion %` DESC
"""
execute_query(query7)

query8A = """
SELECT channel, COUNT(revenue) AS users, SUM(revenue) AS revenue
FROM visits v LEFT JOIN subscriptions s ON v.account_id=s.account_id
WHERE region = 'US/Canada'
GROUP BY channel
"""
execute_query(query8A)