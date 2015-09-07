#----------设置编码
options(encoding = "utf8")
# Sys.setlocale(locale = "utf8")
Sys.setenv(NLS_LANG="AMERICAN_AMERICA.UTF8")

library(RODBC)
library("reshape2")
con <- odbcConnect("UATMDM1", uid="prdoperation", pwd="prdoperation")
#-----------incident的SQL
incsql<-"select * from PRDOPERATION.Z_ITSM_INCIDENT_BACKEND_WEEKLY@PRDMDM_LK"

wosql<-"SELECT week_start_day, ticket_type, cc_team, COUNT(*) AS ticket_number
  FROM (SELECT TO_CHAR(SUBMIT_DATE, 'YYYY-MM') AS MONTH,
               'WEEK' ||
CEIL((SUBMIT_DATE - TO_DATE('2014-02-28', 'YYYY-MM-DD')) / 7) AS WEEK,
TO_CHAR(TRUNC(NEXT_DAY(SUBMIT_DATE - 7, 6)), 'YYYY/MM/DD') AS week_start_day,
TRUNC(NEXT_DAY(SUBMIT_DATE - 7, 1)) + 4 AS week_end_day,
SUBMIT_DATE AS REPORTED_DATE,
'S' || CHR(38) || 'A_Workorder' AS TICKET_TYPE,
CASE
WHEN ASSIGNED_GROUP IN
('MM', 'SRM EBP', 'SRM SUS', '3PO/E-sourcing') THEN
'MM' || CHR(38) || 'SRM'
ELSE
ASSIGNED_GROUP
END AS CC_TEAM,
WORK_ORDER_ID AS TICKET_ID,
ASSIGNED_GROUP,
ASSIGNEE
FROM prdoperation.Z_ITSM_WORKORDER@Prdmdm_Lk a
WHERE SUBMIT_DATE >= TRUNC(NEXT_DAY(SYSDATE - 58, 1))
AND ASSIGNED_SUPPORT_ORGANIZATION IN ('CC-Back End')
UNION
SELECT TO_CHAR(reported_date, 'YYYY-MM') AS MONTH,
'WEEK' ||
CEIL((reported_date - TO_DATE('2014-02-28', 'YYYY-MM-DD')) / 7) AS week,
TO_CHAR(TRUNC(NEXT_DAY(REPORTED_DATE - 7, 6)), 'YYYY/MM/DD') AS week_start_day,
TRUNC(NEXT_DAY(REPORTED_DATE - 7, 1)) + 4 AS week_end_day,
reported_date,
CASE
WHEN LOWER(internet_email) LIKE '%ppmonitor%' THEN
'Monitor_Incident'
ELSE
'User_Incident'
END AS TICKET_TYPE,
CASE
WHEN assigned_group IN ('Strategic PLM', 'PLM') THEN
'PLM'
WHEN assigned_group IN
('SRM EBP', 'SRM SUS', '3PO/E-sourcing', 'MM') THEN
'MM' || CHR(38) || 'SRM'
WHEN assigned_group IN
('PP',
'i2 FP/DS',
'i2 ABPP',
'i2 COST FCST',
'i2 VMI',
'i2 SCP',
'i2 DF',
'Service Planning') OR
assigned_group LIKE 'PP%Planning' THEN
'PP' || CHR(38) || 'Planning'
ELSE
'UNKNOW'
END AS CC_TEAM,
INCIDENT_ID AS TICKET_ID,
ASSIGNED_GROUP,
ASSIGNEE
FROM PRDOPERATION.Z_ITSM_INCIDENT@Prdmdm_Lk a
WHERE reported_date >= TRUNC(NEXT_DAY(SYSDATE - 58, 1))
AND ASSIGNED_SUPPORT_ORGANIZATION IN ('CC-Back End')
AND assigned_group IN
(SELECT assigned_group
FROM PRDOPERATION.Z_CONF_ASSIGNED_GROUP@Prdmdm_Lk)
AND status <> 'Cancelled')
GROUP BY week_start_day, cc_team, ticket_type
ORDER BY week_start_day DESC, cc_team, ticket_type
"

effortsql<-"
SELECT week_start_day,
       CHR_SUPPORTORGANIZATION AS CHR_SUPPORTORGANIZATION,
       CASE
         WHEN CHR_ASSIGNEDGROUP IN
              ('SRM EBP', 'SRM SUS', '3PO/E-sourcing', 'MM') THEN
          'MM' || CHR(38) || 'SRM'
         ELSE
          CHR_ASSIGNEDGROUP
       END AS ASSIGNEDGROUP,
       TICKET_TYPE,
       SUM(EFFORTTIMESPENT_HOURS) AS EFFORT_HOURS,
       COUNT(*) AS effort_recordS
  FROM PRDOPERATION.Z_ITSM_EFFORT@Prdmdm_Lk
 WHERE CHR_SUPPORTORGANIZATION = 'CC-Back End'
   and CREATE_DATE >= TRUNC(NEXT_DAY(SYSDATE - 58, 1))
 GROUP BY week_start_day,
          CHR_SUPPORTORGANIZATION,
          CHR_ASSIGNEDGROUP,
          TICKET_TYPE
 ORDER BY week_start_day,
          CHR_SUPPORTORGANIZATION,
          CHR_ASSIGNEDGROUP,
          TICKET_TYPE
"

alleffortsql<-"
SELECT TO_CHAR(TRUNC(NEXT_DAY(CREATE_DATE - 7, 6)), 'YYYY/MM/DD') AS week_start_day,
       SUM(INT_EFFORTTIMESPENT) AS SUM_EFFORT,
       count(distinct CHR_ASSIGNEE) AS ENGINEER_NUMBER,
       ROUND(SUM(INT_EFFORTTIMESPENT) / count(distinct CHR_ASSIGNEE), 2) AS AVERAGE_EFFORT
  FROM prdoperation.Z_ITSM_TICKETEFFORT@Prdmdm_Lk A
 WHERE CHR_SUPPORTORGANIZATION = 'CC-Back End'
   AND CREATE_DATE >= TRUNC(NEXT_DAY(SYSDATE - 58, 1))
 GROUP BY TO_CHAR(TRUNC(NEXT_DAY(CREATE_DATE - 7, 6)), 'YYYY/MM/DD')
"
inc <- sqlQuery(con, incsql)
wo<-sqlQuery(con,wosql)
effort<-sqlQuery(con,effortsql)
alleffort<-sqlQuery(con,alleffortsql)
odbcClose(con)


alleffort$Percentage<-alleffort$AVERAGE_EFFORT/40

library(ggplot2)
