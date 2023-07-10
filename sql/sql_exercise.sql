/* sql 
Write SQL statements to answer these questions:
1. How many accounts are there in each industry?
2. Which account_name has the highest unique users that had SUCCESS app logins in
Aug 2020?
3. Which app_names have not had any login events today?
4. What's the average time it takes from customer acquisition to having their first app login?
5. What are the top 10 app_name with the highest 30 day unique users and how many 30
day unique users do each of those 10 apps have?
6. Which app_name had the most app login events resulting in FAILURE by industry over
the past 30 days?
7. The User table given at the top reflects the current status of a user. We know that user
status can change over time with the user_status field being a slowly changing
dimension. How would you design this table (hint: adding some columns) so that we can
track the current and historical values of user_status at any given time? Please be brief

### Users

| column name   | data type   |
| ------------- | ----------- |
| user_id       | varchar(18) |
| account_id    | varchar(18) |
| user_status\* | varchar(18) |
| created_date  | timestamp   |
* ACTIVE, INACTIVE, and LOCKED_OUT

### App Login Events

| column name    | data type   |
| -------------- | ----------- |
| event_datetime | timestamp   |
| event_id       | varchar(30) |
| user_id        | varchar(18) |
| app_id         | varchar(30) |
| result\*       | varchar(15) |

*SUCCESS, FAILURE

### App

| column name | data type   |
| ----------- | ----------- |
| app_id      | varchar(30) |
| app_name    | varchar(30) |

### Account

| column name               | data type    |
| ------------------------- | ------------ |
| account_id                | varchar(18)  |
| account_name              | varchar(100) |
| customer_acquisition_date | date         |
| industry                  | varchar(50)  |


*/

/* 1. How many accounts are there in each industry? 
* assuming no duplicate account_id 
*/

SELECT industry
,      COUNT(account_id)                                                                    AS account_count
FROM account
GROUP BY industry;


/* 2. Which account_name has the highest unique users that had SUCCESS app logins in
Aug 2020? */

SELECT
       account_name
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
ORDER BY
       1
LIMIT 1;


/* 3. Which app_names have not had any login events today? */

SELECT 
       app_name
FROM
       app
WHERE
       NOT EXISTS (
              SELECT TRUE
              FROM app_login_events
              WHERE event_date_time::DATE = current_timestamp::DATE
              AND app.id = app_login_events.app_id
              )

/* 4. What's the average time it takes from customer acquisition to having their first app login? */
SELECT AVG(DATEDIFF('day', account.customer_acquisition_date
,      MIN(app_login_events.event_datetime)))                                                             AS avg_time
FROM
       account
JOIN
       users 
ON
       account.account_id = users.account_id
JOIN
       app_login_events 
ON
       users.user_id = app_login_events.user_id
GROUP BY
       account.account_id;


/* 5. What are the top 10 app_name with the highest 30 day unique users and how many 30
day unique users do each of those 10 apps have? */

SELECT app_name
,      COUNT(DISTINCT u.user_id)                                                           AS unique_users
FROM
       app
JOIN
       app_login_events
ON
       app.app_id = app_login_events.app_id
JOIN
       users u ON app_login_events.user_id = users.user_id
WHERE
       app_login_events.event_datetime >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY
       app_name
ORDER BY
       unique_users DESC
LIMIT 10;


/* 6. Which app_name had the most app login events resulting in FAILURE by industry over
the past 30 days? */

SELECT 
       app.app_name
,      app.industry
,      COUNT(*) AS failure_count
FROM
       app a
JOIN
       app_login_events e ON account.app_id = app_login_events.app_id
WHERE
       app_login_events.event_datetime >= CURRENT_DATE - INTERVAL '30 days' AND app_login_events.result = 'FAILURE'
GROUP BY
       account.app_name, account.industry
ORDER BY
       failure_count DESC
LIMIT 1;


/* The User table given at the top reflects the current status of a user. We know that user
status can change over time with the user_status field being a slowly changing
dimension. How would you design this table (hint: adding some columns) so that we can
track the current and historical values of user_status at any given time? Please be brief */

ALTER TABLE users
ADD COLUMN status_start_date timestamp,
ADD COLUMN status_end_date timestamp;



For designing the slowly changing dimension in question 7, this could add a start and end timestamp to track points in time in a SCD2 type tablapp_login_events. A user could then run a query like to find active user counts at a point in time:

SELECT
       calendar_events.calendar_date
,      COUNT(DISTINCT user_id)                                                             AS user_count
FROM
       calendar_events
JOIN
       users
AND
       calendar_events.calendar_date BETWEEN users.status_start_time AND users.status_end_time
WHERE
       user_status = 'ACTIVE'
