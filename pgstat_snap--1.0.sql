/* 
pstat_snap - extension to create snapshots of pg_stat_statements and pg_stat_activity

Author: Raphael Debinski (raphi@crashdump.ch)
Version: 1.0

*/

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pgstat_snap" to load this file. \quit

-- create the necessary tables and views
DO $$
BEGIN
    -- create pgstat_snap_act_history table
    IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = '@extschema@' AND tablename = 'pgstat_snap_act_history') THEN
        EXECUTE '
            CREATE TABLE @extschema@.pgstat_snap_act_history AS
                SELECT localtimestamp(0) AS snapshot_time, * 
                FROM pg_stat_activity 
                WHERE 1=2;
            ALTER TABLE @extschema@.pgstat_snap_act_history ALTER COLUMN snapshot_time SET NOT NULL;
            CREATE INDEX idx_pgstat_act_history_snapshot_time ON @extschema@.pgstat_snap_act_history (snapshot_time);
        ';
    END IF;

    -- create pgstat_snap_stat_history table
    IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = '@extschema@' AND tablename = 'pgstat_snap_stat_history') THEN
        EXECUTE '
            CREATE TABLE @extschema@.pgstat_snap_stat_history AS
                SELECT localtimestamp(0) AS snapshot_time, * 
                FROM pg_stat_statements
                WHERE 1=2;
            ALTER TABLE @extschema@.pgstat_snap_stat_history ALTER COLUMN snapshot_time SET NOT NULL;
            ALTER TABLE @extschema@.pgstat_snap_stat_history ADD PRIMARY KEY (snapshot_time, queryid, dbid);
            CREATE INDEX idx_pgstat_stat_history_snapshot_time ON @extschema@.pgstat_snap_stat_history (snapshot_time);
        ';
    END IF;

    -- Create pgstat_snap_diff_all view - this view contains the difference and sum of every query execution
    EXECUTE '
        CREATE OR REPLACE VIEW @extschema@.pgstat_snap_diff_all AS
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
                @extschema@.pgstat_snap_stat_history a,
                @extschema@.pgstat_snap_act_history b,
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
        CREATE OR REPLACE VIEW @extschema@.pgstat_snap_diff AS
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
                @extschema@.pgstat_snap_stat_history a,
                @extschema@.pgstat_snap_act_history b,
                pg_database c
            WHERE a.queryid = b.query_id
              AND a.dbid = b.datid
              AND a.snapshot_time = b.snapshot_time
              and a.dbid = c.oid
        ) AS subquery
        WHERE rows_changed = TRUE;
        ';
END;
$$ LANGUAGE plpgsql;

-- Function to print basic usage
CREATE OR REPLACE FUNCTION @extschema@.pgstat_snap_help()
RETURNS void AS $$

BEGIN
    RAISE NOTICE '
Tables created:
  pgstat_snap_stat_history  -> pg_stat_statements history
  pgstat_snap_act_history   -> pg_stat_activity history 
  pgstat_snap_diff_all -> view containing the sum and difference of each statement execution
  pgstat_snap_diff     -> view containing only the difference of each statement execution

Start gathering snapshots with, e.g. every 1 second 60 times:
  CALL pgstat_snap_collect(1, 60);
  
Reset all pgstat_snap tables with:
  SELECT pgstat_snap_reset();   -> reset only pgstat_snap tables
  SELECT pgstat_snap_reset(1);  -> also select pg_stat_statements_reset()
  SELECT pgstat_snap_reset(2);  -> also select pg_stat_reset()

Basic queries:
  select * from pgstat_snap_diff order by 1;
  select * from pgstat_snap_diff order by 2,1;
  select sum(rows_d),datname from pgstat_snap_diff group by datname;

To completely uninstall pgstat_snap, run:
  DROP EXTENSION pgstat_snap;

Note: search_path must include "@extschema@"

Check full documentation and source code here: https://github.com/raphideb/pgstat_snap
  ';

END;
$$ LANGUAGE plpgsql;

-- Function to reset all tables and if 1 or 2 is given, also reset pg_stat_statements and all pg_stats
CREATE OR REPLACE FUNCTION @extschema@.pgstat_snap_reset(
    full_reset INTEGER DEFAULT NULL
)
RETURNS void AS $$
BEGIN
    EXECUTE 'TRUNCATE TABLE @extschema@.pgstat_snap_stat_history';
    EXECUTE 'TRUNCATE TABLE @extschema@.pgstat_snap_act_history';

    IF full_reset IS NOT NULL THEN
        PERFORM pg_stat_statements_reset(); 
        IF full_reset = 2 THEN
            PERFORM pg_stat_reset(); 
        END IF;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Procedure to generate the snapshots
CREATE OR REPLACE PROCEDURE @extschema@.pgstat_snap_collect(
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
        INSERT INTO @extschema@.pgstat_snap_act_history 
        SELECT 
            localtimestamp(0), *
        FROM pg_stat_activity;
        COMMIT;
        
        -- Create pgsnap_statements snapshot
        INSERT INTO @extschema@.pgstat_snap_stat_history
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
