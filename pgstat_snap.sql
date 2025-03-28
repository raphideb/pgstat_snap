/*
pstat_snap.sql - script to create snapshots of pg_stat_statements and pg_stat_activity

Author: raphi@crashdump.ch
Version: 1.0

REQUIREMENTS
pg_stat_statements must be loaded and tracking activated in the postgres config:
shared_preload_libraries = 'pg_stat_statements'

Recommended settings:
pg_stat_statements.track = all
pg_stat_statements.track_utility = off

INSTALLATION
It is recommended to install the script in the postgres database:

psql
\i /path/to/pgstat_snap.sql

This will create the following tables and views:
  pgstat_snap.pgstat_stat_history -> pg_stat_statements history
  pgstat_snap.pgstat_act_history  -> pg_stat_activity history 
  pgstat_snap_diff_all            -> view containing the sum and difference of each statement execution
  pgstat_snap_diff                -> view containing only the difference of each statement execution

Note: a new schema "pgstat_snap" will be created for the tables.  For conveniance, the views are created 
      in the schema that installed pgstat_snap.

USAGE
Start gathering snapshots with, e.g. every 1 second 60 times:
  CALL pgstat_snap.create_snapshot(1, 60);

Because everything is timestamped, a reset is usually not needed between CALLs to create_snapshot. But you 
can to cleanup and keep the tables smaller, you can also reset pg_stats.

Reset all pgstat_snap tables with:
  SELECT pgstat_snap.reset();   -> reset only pgstat_snap.pgstat*history tables
  SELECT pgstat_snap.reset(1);  -> also select pg_stat_statements_reset()
  SELECT pgstat_snap.reset(2);  -> also select pg_stat_reset()

HOW IT WORKS
The first argument to create_snapshot is the interval in seconds, the second argument is how many snapshots 
should be collected. Every <interval> seconds, select * from pg_stat_statements will be inserted into
pgstat_stat_history and select * from pgstat_act_statements into pgstat_act_history. 

For every row, a timestamp will be added. Only rows where the "rows" column has changed will be inserted into 
pgstat_stat_history and always only one row per timestamp, dbid and queryid. Every insert is immediately committed 
to be able to live follow the tables/views.

The views have a _d column which displays the difference between the current row and the last row where the query
was recorded in the pgstat_stat_history table. NULL values in rows_d, calls_d and so on mean, that no previous row 
for this query was found because it was executed the first time since create_snapshot was running. 

The views also contain the datname, wait events and the first 20 characters of the query, making it easier to
identify queries of interest.

UNINSTALL
To completely uninstall pgstat_snap, run:
  SELECT pgstat_snap.uninstall();
  DROP SCHEMA pgstat_snap CASCADE;

EXAMPLES
Depending on screensize, you might want to set format to aligned, especially when querying pstat_snap_diff_all:
\pset format aligned

- What was happening:
  select * from pgstat_snap_diff order by 1;

- What was every query doing:
  select * from pgstat_snap_diff order by 2,1;

- Which database touched the most rows:
  select sum(rows_d),datname from pgstat_snap_diff group by datname;

- Which query DML affected the most rows:
  select sum(rows_d),queryid,query from pgstat_snap_diff where upper(query) not like 'SELECT%' group by queryid,query;

- What wait events happened which weren't of type Client:
  select * from pgstat_snap_diff where wait_event_type is not null and wait_event_type <> 'Client' order by 2,1;

- If needed you can access all columns for a particular query directly in the history tables:
  select * from pgstat_snap.pgstat_stat_history where queryid='123455678909876';

*/

-- Create pgstat_snap schema to hold everything in one place
CREATE SCHEMA IF NOT EXISTS pgstat_snap;

-- Function to create the necessary tables and views
CREATE OR REPLACE FUNCTION pgstat_snap.install()
RETURNS void AS $$

BEGIN
    -- Create pgstat_act_history table
    IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'pgstat_snap' AND tablename = 'pgstat_act_history') THEN
        EXECUTE '
            CREATE TABLE pgstat_snap.pgstat_act_history AS
                SELECT localtimestamp(0) AS snapshot_time, * 
                FROM pg_stat_activity 
                WHERE 1=2;
            ALTER TABLE pgstat_snap.pgstat_act_history ALTER COLUMN snapshot_time SET NOT NULL;
            CREATE INDEX idx_pgstat_act_history_snapshot_time ON pgstat_snap.pgstat_act_history (snapshot_time);
        ';
    END IF;

    -- Create pgstat_stat_history table
    IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'pgstat_snap' AND tablename = 'pgstat_stat_history') THEN
        EXECUTE '
            CREATE TABLE pgstat_snap.pgstat_stat_history AS
                SELECT localtimestamp(0) AS snapshot_time, * 
                FROM pg_stat_statements
                WHERE 1=2;
            ALTER TABLE pgstat_snap.pgstat_stat_history ALTER COLUMN snapshot_time SET NOT NULL;
            ALTER TABLE pgstat_snap.pgstat_stat_history ADD PRIMARY KEY (snapshot_time, queryid, dbid);
            CREATE INDEX ON pgstat_snap.pgstat_stat_history (snapshot_time);
        ';
    END IF;

    -- Create pgstat_snap_diff_all view - this view contains the difference and sum of every query execution
    EXECUTE '
        CREATE OR REPLACE VIEW pgstat_snap_diff_all AS
            SELECT
            snapshot_time,
            queryid,
            query,
            datname,
            usename,
            wait_event_type,
            wait_event,
            rows,
            rows_diff rows_d,
            calls,
            calls_diff calls_d,
            round(total_exec_time::numeric, 6) exec_ms,
            round(total_exec_time_diff::numeric, 6) exec_ms_d,
            shared_blks_hit sb_hit,
            shared_blks_hit_diff sb_hit_d,
            shared_blks_read sb_read,
            shared_blks_read_diff sb_read_d,
            shared_blks_dirtied sb_dirt,
            shared_blks_dirtied_diff sb_dirt_d,
            shared_blks_written sb_write,
            shared_blks_written_diff sb_write_d

        FROM (
            SELECT
                a.snapshot_time snapshot_time,
                queryid,
                replace(replace(substring(b.query from 0 for 20), E''\n'',''''),E''\t'','' '') query,
                c.datname datname,
                b.usename usename,
                wait_event,
                wait_event_type,
                rows,
                rows - LAG(rows) OVER (PARTITION BY queryid, dbid ORDER BY a.snapshot_time) AS rows_diff,
                calls,
                calls - LAG(calls) OVER (PARTITION BY queryid, dbid ORDER BY a.snapshot_time) AS calls_diff,
                total_exec_time,
                total_exec_time - LAG(total_exec_time) OVER (PARTITION BY queryid, dbid ORDER BY a.snapshot_time) AS total_exec_time_diff,
                shared_blks_hit,
                shared_blks_hit - LAG(shared_blks_read) OVER (PARTITION BY queryid, dbid ORDER BY a.snapshot_time) AS shared_blks_hit_diff,
                shared_blks_read,
                shared_blks_read - LAG(shared_blks_read) OVER (PARTITION BY queryid, dbid ORDER BY a.snapshot_time) AS shared_blks_read_diff,
                shared_blks_dirtied,
                shared_blks_dirtied - LAG(shared_blks_dirtied) OVER (PARTITION BY queryid, dbid ORDER BY a.snapshot_time) AS shared_blks_dirtied_diff,
                shared_blks_written,
                shared_blks_written - LAG(shared_blks_written) OVER (PARTITION BY queryid, dbid ORDER BY a.snapshot_time) AS shared_blks_written_diff,
                CASE
                    WHEN LAG(rows) OVER (PARTITION BY queryid, dbid ORDER BY a.snapshot_time) IS NULL OR rows != LAG(rows) OVER (PARTITION BY queryid, dbid ORDER BY a.snapshot_time) THEN TRUE
                    ELSE FALSE
                END AS rows_changed
            FROM
                pgstat_snap.pgstat_stat_history a,
                pgstat_snap.pgstat_act_history b,
                pg_database c
            WHERE a.queryid = b.query_id
              AND a.dbid = b.datid
              AND a.snapshot_time = b.snapshot_time
              and a.dbid = c.oid
        ) AS subquery
        WHERE rows_changed = TRUE;
        ';
    
    -- Create pgstat_snap_diff view - this view contains only the difference between every query execution
    EXECUTE '
        CREATE OR REPLACE VIEW pgstat_snap_diff AS
            SELECT
            snapshot_time,
            queryid,
            query,
            datname,
            usename,
            wait_event_type,
            wait_event,
            rows_diff rows_d,
            calls_diff calls_d,
            round(total_exec_time_diff::numeric, 6) exec_ms_d,
            shared_blks_hit_diff sb_hit_d,
            shared_blks_read_diff sb_read_d,
            shared_blks_dirtied_diff sb_dirt_d,
            shared_blks_written_diff sb_write_d

        FROM (
            SELECT
                a.snapshot_time snapshot_time,
                queryid,
                replace(replace(substring(b.query from 0 for 20), E''\n'',''''),E''\t'','' '') query,
                c.datname datname,
                b.usename usename,
                wait_event,
                wait_event_type,
                rows - LAG(rows) OVER (PARTITION BY queryid, dbid ORDER BY a.snapshot_time) AS rows_diff,
                calls - LAG(calls) OVER (PARTITION BY queryid, dbid ORDER BY a.snapshot_time) AS calls_diff,
                total_exec_time - LAG(total_exec_time) OVER (PARTITION BY queryid, dbid ORDER BY a.snapshot_time) AS total_exec_time_diff,
                shared_blks_hit - LAG(shared_blks_hit) OVER (PARTITION BY queryid, dbid ORDER BY a.snapshot_time) AS shared_blks_hit_diff,
                shared_blks_read - LAG(shared_blks_read) OVER (PARTITION BY queryid, dbid ORDER BY a.snapshot_time) AS shared_blks_read_diff,
                shared_blks_dirtied - LAG(shared_blks_dirtied) OVER (PARTITION BY queryid, dbid ORDER BY a.snapshot_time) AS shared_blks_dirtied_diff,
                shared_blks_written - LAG(shared_blks_written) OVER (PARTITION BY queryid, dbid ORDER BY a.snapshot_time) AS shared_blks_written_diff,
                CASE
                    WHEN LAG(rows) OVER (PARTITION BY queryid, dbid ORDER BY a.snapshot_time) IS NULL OR rows != LAG(rows) OVER (PARTITION BY queryid, dbid ORDER BY a.snapshot_time) THEN TRUE
                    ELSE FALSE
                END AS rows_changed
            FROM
                pgstat_snap.pgstat_stat_history a,
                pgstat_snap.pgstat_act_history b,
                pg_database c
            WHERE a.queryid = b.query_id
              AND a.dbid = b.datid
              AND a.snapshot_time = b.snapshot_time
              and a.dbid = c.oid
        ) AS subquery
        WHERE rows_changed = TRUE;
        ';

    RAISE NOTICE '
Tables created:
  pgstat_snap.pgstat_stat_history -> pg_stat_statements history
  pgstat_snap.pgstat_act_history  -> pg_stat_activity history 
  pgstat_snap_diff_all            -> view containing the sum and difference of each statement execution
  pgstat_snap_diff                -> view containing only the difference of each statement execution

Start gathering snapshots with, e.g. every 1 second 60 times:
  CALL pgstat_snap.create_snapshot(1, 60);
  
Reset all pgstat_snap tables with:
  SELECT pgstat_snap.reset();   -> reset only pgstat_snap tables
  SELECT pgstat_snap.reset(1);  -> also select pg_stat_statements_reset()
  SELECT pgstat_snap.reset(2);  -> also select pg_stat_reset()

To completely uninstall pgstat_snap, run:
  SELECT pgstat_snap.uninstall();
  DROP SCHEMA pgstat_snap CASCADE;
  ';

END;
$$ LANGUAGE plpgsql;

-- Function to drop all pgstat_snap tables and views
CREATE OR REPLACE FUNCTION pgstat_snap.uninstall()
RETURNS void AS $$
BEGIN
    -- Drop pgstat_stat_history table
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'pgstat_snap' AND tablename = 'pgstat_stat_history') THEN
        EXECUTE 'DROP TABLE pgstat_snap.pgstat_stat_history CASCADE';
        RAISE NOTICE 'pgstat_stat_history table dropped.';
    END IF;

    -- Drop pgstat_act_history table
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'pgstat_snap' AND tablename = 'pgstat_act_history') THEN
        EXECUTE 'DROP TABLE pgstat_snap.pgstat_act_history CASCADE';
        RAISE NOTICE 'pgstat_act_history table dropped.';
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Function to reset all tables and if 1 or 2 is given, also reset pg_stat_statements and all pg_stats
CREATE OR REPLACE FUNCTION pgstat_snap.reset(
    full_reset INTEGER DEFAULT NULL
)
RETURNS void AS $$
BEGIN
    PERFORM pgstat_snap.uninstall();
    PERFORM pgstat_snap.install();

    IF full_reset IS NOT NULL THEN
        PERFORM pg_stat_statements_reset(); 
        IF full_reset = 2 THEN
            PERFORM pg_stat_reset(); 
        END IF;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Procedure to generate the snapshots
CREATE OR REPLACE PROCEDURE pgstat_snap.create_snapshot(
    interval_seconds INTEGER,
    num_iterations INTEGER DEFAULT NULL
)
LANGUAGE plpgsql AS $$
DECLARE
    current_snapshot_time TIMESTAMP WITHOUT TIME ZONE;
    iteration INTEGER := 0;
BEGIN
    WHILE (num_iterations IS NULL OR iteration < num_iterations) LOOP
        current_snapshot_time := localtimestamp(0);
        
        -- Create pgsnap_activity snapshot
        INSERT INTO pgstat_snap.pgstat_act_history 
        SELECT 
            localtimestamp(0), *
        FROM pg_stat_activity;
        COMMIT;
        
        -- Create pgsnap_statements snapshot
        INSERT INTO pgstat_snap.pgstat_stat_history
        SELECT 
            localtimestamp(0), *
        FROM pg_stat_statements
        ON CONFLICT (snapshot_time, queryid, dbid)
        DO NOTHING;
        COMMIT;
 
        iteration := iteration + 1;
        PERFORM pg_sleep(interval_seconds);
    END LOOP;
END;
$$;

-- create all tables and views
select pgstat_snap.install(); 