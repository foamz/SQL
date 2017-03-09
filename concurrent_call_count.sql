# This works out concurrent calls on your Asterisk CDR database
# Per year / month / day / hour / minute
# Depending on the amount of calls in your database, this set of queries will be extremly intensive 
# and can take a long time to execute if you span your date range for longer than a day.

SET @from_dt := '2017-03-03 00:00:00';
SET @to_dt := '2017-03-03 23:59:59';

DROP TABLE IF EXISTS tmp_c1;

CREATE TABLE tmp_c1 AS (
	SELECT 
		UNIX_TIMESTAMP(calldate) AS start_time, 
		UNIX_TIMESTAMP(calldate) + duration AS end_time
	FROM 
		cdr a 
	WHERE 
	(LENGTH(a.dst) > 9 OR LENGTH(a.src) > 9)  ## looking at number length longer than 9 digits, to ensure I only get external calls
	AND duration > 0 
	AND calldate >= @from_dt 
	AND calldate <= @to_dt
);

## these indexes might not help anything at all, but its worth a try...
CREATE INDEX startend ON tmp_c1 (start_time, end_time);
CREATE INDEX start_time ON tmp_c1 (start_time);
CREATE INDEX end_time ON tmp_c1 (end_time);

DROP TABLE IF EXISTS tmp_c2;

CREATE TABLE tmp_c2 AS (
	SELECT 
		calldate AS start_time 
	FROM 
		cdr a
	WHERE 
	(LENGTH(a.dst) > 9 OR LENGTH(a.src) >9)  ## looking at number length longer than 9 digits, to ensure I only get external calls
	AND duration > 0 
	AND calldate >= @from_dt 
	AND calldate <= @to_dt
);

CREATE INDEX start_time ON tmp_c2 (start_time);

DROP TABLE IF EXISTS t_concurrent;
CREATE TABLE t_concurrent
SELECT 
	YEAR(start_time) AS Yr, 
	MONTH(start_time) AS Mth, 
	DAY(start_time) AS Dy, 
	HOUR(start_time) AS Hr,
	MINUTE(start_time) AS Min,
	(SELECT COUNT(*) FROM tmp_c1 a
	WHERE 
		a.start_time <= UNIX_TIMESTAMP(b.start_time) 
		AND 
		a.end_time >= UNIX_TIMESTAMP(b.start_time) 
	) AS Concurrent
FROM tmp_c2 b  ;

## insert the data collected above into a table we will store more permanently.
INSERT INTO concurrent_stats SELECT * FROM t_concurrent;

SELECT * FROM concurrent_stats;
