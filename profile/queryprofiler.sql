-- =============================================
-- Author:      Mac
-- Create Date: 6 Jun 2020
-- Description: Tiny sql profiler
--
-- Retrieves SQL statements and their average running time (worker time / execution count)
-- Adjust the limiter below to retrieve more results
-- =============================================

SELECT TOP 100 query_stats.query_hash AS "QueryHash",
	MIN(query_stats.creation_time) AS "Timestamp",
	MIN(user_name) AS "User",
	MIN(login_name) AS "UserLogin",
	MIN(host) AS "ClientHost",
	MIN([program_name]) AS "ProgramName",
	MIN(net_transport) AS NetTransport,
	MIN(dbname) AS "Database",
    SUM(query_stats.total_worker_time) / SUM(query_stats.execution_count) AS "Avg CPU Time",
    REPLACE(REPLACE(REPLACE(MIN(query_stats.statement_text), CHAR(13), ''), CHAR(10), ''), CHAR(9), ' ') AS "Statement Text" -- remove CR, LF and TAB characters
FROM
    (SELECT QS.*,
	CONN.client_net_address,
	SES.host_name AS host, SES.login_name, SES.original_login_name, SES.program_name, CONN.net_transport,
	db_name(ST.dbid) AS dbname,
    SUBSTRING(ST.text, (QS.statement_start_offset/2) + 1,
    ((CASE QS.statement_end_offset
        WHEN -1 THEN DATALENGTH(ST.text)
        ELSE QS.statement_end_offset END
            - QS.statement_start_offset)/2) + 1) AS statement_text,
	 USR.name AS user_name
     FROM sys.dm_exec_query_stats AS QS
     CROSS APPLY sys.dm_exec_sql_text(QS.sql_handle) AS ST
	 CROSS APPLY sys.dm_exec_plan_attributes(QS.plan_handle) AS ATTR
	 LEFT JOIN sysusers AS USR on USR.uid = ATTR.value and ATTR.attribute = 'USER_ID'
	 LEFT JOIN sys.dm_exec_requests AS REQ on REQ.query_hash = QS.query_hash
	 LEFT JOIN sys.dm_exec_connections AS CONN on CONN.connection_id = REQ.connection_id
	 LEFT JOIN sys.dm_exec_sessions AS SES on SES.session_id = REQ.session_id
	 ) AS query_stats
GROUP BY query_stats.query_hash
ORDER BY 2 DESC;