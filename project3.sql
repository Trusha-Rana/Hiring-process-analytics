use project3;
CREATE TABLE job_data (
    ds VARCHAR(100),
    job_id FLOAT,
    actor_id INT,
    event VARCHAR(100),
    language VARCHAR(100),
    time_spent INT,
    org VARCHAR(30)
);
SELECT 
    *
FROM
    job_data;
#drop table job_data;
show variables like "secure_file_priv";
load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/job_data.csv"
into table job_data
fields terminated by ','
enclosed by'"'
lines terminated by "\n"
ignore 1 rows 
;
SELECT 
    *
FROM
    job_data;

alter table job_data add column  date_stored date;
UPDATE job_data 
SET 
    date_stored = STR_TO_DATE(ds, '%Y-%m-%d');

select*from job_data;
 
 -- CASE STUDY 1--
 #1.Write an SQL query to calculate the number of jobs reviewed per hour for each day in November 2020.
 
 SELECT*
FROM job_data;
select date_stored as 'date',
round((count(job_id)/(sum(time_spent))*3600)) as jobs_per_hour 
from job_data
where date_stored between '2020-11-01' and '2020-11-30'
group by date_stored
;

#2.Write an SQL query to calculate the 7-day rolling average of throughput.
# Additionally, explain whether you prefer using the daily metric or the 7-day rolling average for throughput, and why.
SELECT round(count(event)/sum(time_spent),2) as "weekly_throughput"
FROM job_data;


SELECT date_stored as dates, round(count(event)/sum(time_spent),2) as "Daily throughput"

FROM job_data
group by date_stored
order by date_stored ;


#I would prefer using the 7-day rolling average for throughput because it provides a more stable and representative measure of performance than the daily metric. The daily metric can be affected by outliers, which can skew the results. The 7-day rolling average smooths out these outliers and provides a more accurate picture of the overall trend.

select* from job_data;
#3. : Calculate the percentage share of each language in the last 30 days.
#Your Task: Write an SQL query to calculate the percentage share of each language over the last 30 days.
SELECT language,
       round(100*count(*)/ total,2)as percentage_share
FROM job_data
cross join (select count(*) as total from job_data) sub
GROUP BY language;

#4.Objective: Identify duplicate rows in the data.
#Your Task: Write an SQL query to display duplicate rows from the job_data table.
SELECT *
FROM job_data
;
SELECT actor_id, count(*) as Duplicates
    FROM job_data
    GROUP BY actor_id
    HAVING COUNT(*) > 1;

-- CASE STUDY 2--
use project3;
create table events(

user_id int,
occured_at varchar(100),
event_type varchar(50),
event_name varchar(100),
location varchar(100),
device varchar(30),
user_type int );
select*from events;

show variables like "secure_file_priv";
load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/events.csv"
into table events
fields terminated by ','
enclosed by'"'
lines terminated by "\n"
ignore 1 rows;
select*from events;

alter table events add column  temp_occured_at datetime;
#update events set temp_occured_at = str_to_date(occured_at,"%d/%m/%Y  %H:%i");
UPDATE events
SET temp_occured_at = STR_TO_DATE(occured_at, "%m/%d/%Y %H:%i")
WHERE occured_at IS NOT NULL;

#update users set temp_created_at = str_to_date(created_at, "%d-%m-%Y %H:%i");

alter table events drop column occured_at;
alter table events change column  temp_occured_at occured_at datetime;
select*from events;

#a. Write an SQL query to calculate the weekly user engagement.
select*from users;
select extract( Week from occured_at) as "Week number",
count(distinct user_id) as "Weekly Active Users"
from events
where event_type= "engagement"
group by 1;
 

#b. Analyze the growth of users over time for a product.
#Your Task: Write an SQL query to calculate the user growth for the product.

select*from events
limit 10;
SELECT
    Months, Users, round(((Users/Lag(Users,1)Over (Order by months)-1)*100),2)as "Growth in %"
    from
    (
    select extract(Month from created_at) as months,
    count(activated_at)as users
    from users
    where activated_at not in ("")
    group by 1
    order by 1
    ) sub;
#3.Analyze the retention of users on a weekly basis after signing up for a product.
#Your Task: Write an SQL query to calculate the weekly retention of users based on their sign-up cohort.
SELECT first AS "Week Numbers",
SUM(CASE WHEN week_number = 0 THEN 1 ELSE 0 END) AS "Week 0",
SUM(CASE WHEN week_number = 1 THEN 1 ELSE 0 END) AS "Week 1",
SUM(CASE WHEN week_number = 2 THEN 1 ELSE 0 END) AS "Week 2",
SUM(CASE WHEN week_number = 3 THEN 1 ELSE 0 END) AS "Week 3",
SUM(CASE WHEN week_number = 4 THEN 1 ELSE 0 END) AS "Week 4",
SUM(CASE WHEN week_number = 5 THEN 1 ELSE 0 END) AS "Week 5",
SUM(CASE WHEN week_number = 6 THEN 1 ELSE 0 END) AS "Week 6",
SUM(CASE WHEN week_number = 7 THEN 1 ELSE 0 END) AS "Week 7",
SUM(CASE WHEN week_number = 8 THEN 1 ELSE 0 END) AS "Week 8",
SUM(CASE WHEN week_number = 9 THEN 1 ELSE 0 END) AS "Week 9",
SUM(CASE WHEN week_number = 10 THEN 1 ELSE 0 END) AS "Week 10",
SUM(CASE WHEN week_number = 11 THEN 1 ELSE 0 END) AS "Week 11",
SUM(CASE WHEN week_number = 12 THEN 1 ELSE 0 END) AS "Week 12",
SUM(CASE WHEN week_number = 13 THEN 1 ELSE 0 END) AS "Week 13",
SUM(CASE WHEN week_number = 14 THEN 1 ELSE 0 END) AS "Week 14",
SUM(CASE WHEN week_number = 15 THEN 1 ELSE 0 END) AS "Week 15",
SUM(CASE WHEN week_number = 16 THEN 1 ELSE 0 END) AS "Week 16",
SUM(CASE WHEN week_number = 17 THEN 1 ELSE 0 END) AS "Week 17",
SUM(CASE WHEN week_number = 18 THEN 1 ELSE 0 END) AS "Week 18"
from 
	(
SELECT m.user_id, m.login_week, n.first, m.login_week - first AS week_number FROM
(SELECT user_id, EXTRACT(WEEK FROM occured_at) AS login_week FROM events GROUP BY 1, 2) m,
(SELECT user_id, MIN(EXTRACT(WEEK FROM occured_at)) AS first FROM events GROUP BY 1) n
WHERE m.user_id = n.user_id
	) sub
GROUP BY first 
ORDER BY first;


#d.Objective: Measure the activeness of users on a weekly basis per device.
#Your Task: Write an SQL query to calculate the weekly engagement per device.

SELECT EXTRACT(WEEK FROM occured_at) AS "Week Numbers",
COUNT(DISTINCT CASE WHEN device IN('dell inspiron notebook') THEN user_id ELSE NULL END) AS "Dell Inspiron Notebook",
COUNT(DISTINCT CASE WHEN device IN('iphone 5') THEN user_id ELSE NULL END) AS
"iPhone 5",
COUNT(DISTINCT CASE WHEN device IN('iphone 4s') THEN user_id ELSE NULL END) AS
"iPhone 4S",
COUNT(DISTINCT CASE WHEN device IN('windows surface') THEN user_id ELSE NULL END) AS "Windows Surface",
COUNT(DISTINCT CASE WHEN device IN('macbook air') THEN user_id ELSE NULL END) AS "Macbook Air",
COUNT(DISTINCT CASE WHEN device IN('iphone 5s') THEN user_id ELSE NULL END) AS
"iPhone 5S",
COUNT(DISTINCT CASE WHEN device IN('macbook pro') THEN user_id ELSE NULL END) AS "Macbook Pro",
COUNT(DISTINCT CASE WHEN device IN('kindle fire') THEN user_id ELSE NULL END) AS "Kindle Fire",
COUNT(DISTINCT CASE WHEN device IN('ipad mini') THEN user_id ELSE NULL END) AS "iPad Mini",
COUNT(DISTINCT CASE WHEN device IN('nexus 7') THEN user_id ELSE NULL END) AS
"Nexus 7",
COUNT(DISTINCT CASE WHEN device IN('nexus 5') THEN user_id ELSE NULL END) AS
"Nexus 5",
COUNT(DISTINCT CASE WHEN device IN('samsung galaxy s4') THEN user_id ELSE NULL END) AS "Samsung Galaxy S4",
COUNT(DISTINCT CASE WHEN device IN('lenovo thinkpad') THEN user_id ELSE NULL END) AS "Lenovo Thinkpad",
COUNT(DISTINCT CASE WHEN device IN('samsumg galaxy tablet') THEN user_id ELSE NULL END) AS "Samsumg Galaxy Tablet",
COUNT(DISTINCT CASE WHEN device IN('acer aspire notebook') THEN user_id ELSE NULL END) AS "Acer Aspire Notebook",
COUNT(DISTINCT CASE WHEN device IN('asus chromebook') THEN user_id ELSE NULL END) AS "Asus Chromebook",
COUNT(DISTINCT CASE WHEN device IN('htc one') THEN user_id ELSE NULL END) AS "HTC One",
COUNT(DISTINCT CASE WHEN device IN('nokia lumia 635') THEN user_id ELSE NULL END) AS "Nokia Lumia 635",
COUNT(DISTINCT CASE WHEN device IN('samsung galaxy note') THEN user_id ELSE NULL END) AS "Samsung Galaxy Note",
COUNT(DISTINCT CASE WHEN device IN('acer aspire desktop') THEN user_id ELSE NULL END) AS "Acer Aspire Desktop",
COUNT(DISTINCT CASE WHEN device IN('mac mini') THEN user_id ELSE NULL END) AS "Mac Mini",
COUNT(DISTINCT CASE WHEN device IN('hp pavilion desktop') THEN user_id ELSE NULL END) AS "HP Pavilion Desktop",
COUNT(DISTINCT CASE WHEN device IN('dell inspiron desktop') THEN user_id ELSE NULL END) AS "Dell Inspiron Desktop",
COUNT(DISTINCT CASE WHEN device IN('ipad air') THEN user_id ELSE NULL END) AS "iPad Air",
 
COUNT(DISTINCT CASE WHEN device IN('amazon fire phone') THEN user_id ELSE NULL END) AS "Amazon Fire Phone",
COUNT(DISTINCT CASE WHEN device IN('nexus 10') THEN user_id ELSE NULL END) AS
"Nexus 10" FROM events
WHERE event_type = 'engagement' GROUP BY 1
ORDER BY 1;



#Email Engagement Analysis:


use project3;
CREATE TABLE email_events (
user_id	int,occurred_at varchar(50),	action varchar(30),	user_type int

   
);
SELECT 
    *
FROM
    email_events;

show variables like "secure_file_priv";
load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/email_events.csv"
into table email_events
fields terminated by ','
enclosed by'"'
lines terminated by "\n"
ignore 1 rows 
;
SELECT 
    *
FROM
    email_events;

alter table email_events
 add column  temp_occured_at datetime;
UPDATE email_events
SET 
    temp_occured_at = STR_TO_DATE(occurred_at, '%d/%m/%Y %H:%i');
alter table email_events drop column occurred_at;
alter table email_events change column  temp_occured_at occurerd_at datetime;
select*from email_events;
#Objective: Analyze how users are engaging with the email service.
#Your Task: Write an SQL query to calculate the email engagement metrics.

SELECT Week,
ROUND((weekly_digest/total*100),2) AS "Weekly Digest Rate", 
ROUND((email_opens/total*100),2) AS "Email Open Rate", 
ROUND((email_clickthroughs/total*100),2) AS "Email Clickthrough Rate", 
ROUND((reengagement_emails/total*100),2) AS "Reengagement Email Rate" 
FROM
(
SELECT EXTRACT(WEEK FROM occurerd_at) AS Week,
COUNT(CASE WHEN action = 'sent_weekly_digest' THEN user_id ELSE NULL END) AS weekly_digest,
 
COUNT(CASE WHEN action = 'email_open' THEN user_id   ELSE NULL END) as email_opens,
COUNT(CASE WHEN action = 'email_clickthrough' THEN user_id ELSE NULL END) AS email_clickthroughs,
COUNT(CASE WHEN action = 'sent_reengagement_email' THEN user_id ELSE NULL END) AS reengagement_emails,
COUNT(user_id) AS total FROM email_events GROUP BY 1
) sub
GROUP BY 1
ORDER BY 1;

