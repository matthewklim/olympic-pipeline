# SQL Exercise

Query 1: How many accounts are there in each industry?

<details open><summary> This query counts the number of accounts in each industry and groups the results by industry</summary>

```sql
SELECT 
       industry
,      COUNT(account_id)                                                                   AS account_count
FROM
       account
GROUP BY
       industry
;
```

</details>

Query 2: Which account_name has the highest unique users that had SUCCESS app logins in Aug 2020?

<details open><summary>This query identifies the account with the highest number of unique users who had successful app logins in August 2020</summary>

```sql
SELECT
       account.account_name
,      COUNT(DISTINCT app_login_events.user_id)                                            AS user_count
FROM
       account
JOIN
       users
ON
       account.account_id = users.account_id
JOIN
       app_login_events e
ON
       users.user_id = app_login_events.user_id
WHERE
       app_login_events.result = 'SUCCESS'
AND
       DATE_TRUNC('month',app_login_events.event_datetime) = '2020-08-01'
GROUP BY
       1
QUALIFY
       DENSE_RANK() OVER (ORDER BY user_count) = 1 -- return the first ranked account_name with the highest successful logins and will return multiple if tied for first
ORDER BY
       1;
```

</details>

Query 3: Which app_names have not had any login events today?

<details open><summary>This query retrieves the app names that have not recorded any login events on the current day</summary>

```SQL
SELECT 
       app_name
FROM
       app
WHERE
       NOT EXISTS (
              SELECT TRUE
              FROM app_login_events
              WHERE event_datetime::DATE = CURRENT_DATE()
              AND app.app_id = app_login_events.app_id
              )
;
```

</details>

Query 4: What's the average time it takes from customer acquisition to having their first app login?

<details open><summary>This query calculates the average time, in days, between customer acquisition and their first app login for customers who have activated</summary>

```sql
WITH account_acquisition_time AS (
SELECT 
       users.account_id
,      TIMESTAMPDIFF(
              'day'
       ,      account.customer_acquisition_date
       ,      MIN(app_login_events.event_datetime)
              )                                                                            AS time_since_acquisition
FROM
       app_login_events
JOIN
       users 
ON
       app_login_events.user_id = users.user_id
JOIN
       account
ON
       users.account_id = account.account_id
GROUP BY
       users.account_id
,      users.customer_acquisition_date
       )
SELECT 
       AVG(time_since_acquisition)                                                         AS average_days_since_acquisition
FROM
       account_acquisition_time
;    
```

</details>

Query 5: What are the top 10 app_names with the highest 30-day unique users and how many unique users do they have?

<detail opens><summary>This query identifies the top 10 app names with the highest number of unique users in the last 30 days and provides the count of unique users for each app</summary>

```sql
SELECT 
       app_name
,      COUNT(DISTINCT users.user_id)                                                       AS unique_users
,      DENSE_RANK() OVER (ORDER BY unique_users DESC)                                      AS user_rank
FROM
       app
JOIN
       app_login_events
ON
       app.app_id = app_login_events.app_id
JOIN
       users
ON
       app_login_events.user_id = users.user_id
WHERE
       app_login_events.event_datetime >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY
       app_name
QUALIFY
       user_rank <= 10
ORDER BY
       user_rank DESC
;
```

</details>

Query 6: Which app_name had the most app login events resulting in FAILURE by industry over the past 30 days?

<details open><summary>This query determines the app name that had the highest number of app login events resulting in failure, categorized by industry, over the last 30 days</summary>

```sql
SELECT
       app.app_name
,      app.industry
,      COUNT(app_login_events.event_id)                                                    AS failure_count
FROM
       app
JOIN
       app_login_events
ON
       app.app_id = app_login_events.app_id
WHERE
       app_login_events.event_datetime >= CURRENT_DATE - INTERVAL '30 days' 
AND
       app_login_events.result = 'FAILURE'
GROUP BY
       1,2
QUALIFY
       ROW_NUMBER() OVER (PARTITION BY app.industry ORDER BY failure_count) = 1 --return the highest app_name by industry - this could be dense_rank if ties matter
ORDER BY
       failure_count DESC
;
```

</details>

Query 7: Table Design: Slowly Changing Dimension for User Status

For designing the slowly changing dimension in question 7, this could add a start and end timestamp to track points in time in a SCD2 type table. A user could join table containing all possible dates or another dataset of interest between start and end times of user status. Use cases explored in [sql_exercise.sql](sql_exercise.sql).

<details open><summary>This section provides a brief discussion on how to design a table to track the current and historical values of user_status. It suggests using additional columns like status_start_date and status_end_date to capture changes over time.
</summary>

```sql
SELECT
       calendar_events.calendar_date
,      users.user_status
,      COUNT(DISTINCT users.user_id)                                                       AS user_count
FROM
       calendar_events
JOIN
       users
AND
       calendar_events.calendar_date BETWEEN users.status_start_time AND users.status_end_time
WHERE
       users.status = 'ACTIVE'
GROUP BY
       1,2
;
```

</details>
