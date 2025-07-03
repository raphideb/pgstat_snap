# Purpose of this extension
The cumulative statistics system (CSS) in PostgreSQL and pg_stat_statements in particular lack any timing information, all values are cumulative and the only way to figure out the difference between query executions is to reset the stats every time or work with averages. 

With the pgstat_snap extension, you can create timestamped snapshots of pg_stat_statements and pg_stat_activity when needed. It also provides views that show the difference between every snapshot for every query and database. 

# Requirements
pg_stat_statements must be loaded and tracking activated in the postgres config:  
```
shared_preload_libraries = 'pg_stat_statements'
```
Recommended settings:  
```
pg_stat_statements.track = all  
pg_stat_statements.track_utility = off
```

The extension has to be created in the database in which pgstat_snap will be installed:
```
create extension pg_stat_statements;
```

# Installation
To install the extension, download these files:
```
pgstat_snap--1.0.sql
pgstat_snap.control
```

And copy them to the extension directory of PostgreSQL
```
sudo cp pgstat_snap* $(pg_config --sharedir)/extension/
```

You can then install the extension in any database that has the pg_stat_statements extension enabled, superuser right are NOT needed:
```
create extension pgstat_snap;
```

It can also be installed into a different schema but be sure to have it included in the search_path:
```
create extension pgstat_snap schema my_schema;
```

This will create the following tables and views:
```
  pgstat_snap_stat_history   -> pg_stat_statements history (complete snapshot)
  pgstat_snap_act_history    -> pg_stat_activity history (complete snapshot)
  pgstat_snap_diff_all       -> view containing the sum and difference of each statement between snapshots
  pgstat_snap_diff           -> view containing only the difference of each statement between snapshots
```

# Usage
Start gathering snapshots with, e.g. every 1 second 60 times:
```
CALL pgstat_snap_collect(1, 60);
```
Or gather a snapshot every 5 seconds for 10 minutes:
```
CALL pgstat_snap_collect(5, 120);
```

**IMPORTANT:** on very busy clusters with many databases a lot of data can be collected, 500mb per minute or more. Don't let it run for a very long time with short intervals, unless you have the disk space for it.

## Reset
Because everything is timestamped, a reset is usually not needed between CALLs to create_snapshot. But you can to cleanup and keep the tables smaller. You can also reset pg_stats*.

Reset all pgstat_snap tables with:
```
  SELECT pgstat_snap_reset();   -> reset only pgstat_snap.pgstat*history tables
  SELECT pgstat_snap_reset(1);  -> also select pg_stat_statements_reset()
  SELECT pgstat_snap_reset(2);  -> also select pg_stat_reset()
```

# How it works 
The first argument to create_snapshot is the interval in seconds, the second argument is how many snapshots should be collected. Every *interval* seconds, *select * from pg_stat_statements* will be inserted into *pgstat_snap_stat_history* and *select * from pgstat_act_statements* into *pgstat_snap_act_history*. 

For every row, a timestamp will be added. Only rows where the "rows" column has changed will be inserted into *pgstat_snap_stat_history* and always only one row per timestamp, dbid and queryid. Every insert is immediately committed to be able to live follow the tables/views.

The views have a *_d* column which displays the difference between the current row and the last row where the query was recorded in the pgstat_snap_stat_history table. *NULL* values in *rows_d*, *calls_d* and so on mean, that no previous row for this query was found because it was executed the first time since create_snapshot was running. 

The views also contain the datname, wait events and the first 20 characters of the query, making it easier to identify queries of interest.

# Uninstall
To completely uninstall pgstat_snap, run:
```
DROP EXTENSION pgstat_snap;
```

# Views description
## pgstat_snap_diff
This view only contains the difference between the previous and next execution of a queryid/dbid pair:

| Column Name | Description |
|-------------|-------------|
| **snapshot_time** | Timestamp |
| **queryid** | Query ID |
| **query** | Query Text (first 20 characters) |
| **datname** | Database Name |
| **usename** | Username |
| **wait_event_type** | Event Type - NULL if no wait occurred |
| **wait_event** | Wait Event - NULL if no wait occurred |
| **rows_d** | Difference in rows from the previous snapshot_time |
| **calls_d** | Difference in calls from the previous snapshot_time |
| **exec_ms_d** | Difference in total execution time from the previous snapshot_time |
| **sb_hit_d** | Difference in shared block hits from the previous snapshot_time |
| **sb_read_d** | Difference in shared block reads from the previous snapshot_time |
| **sb_dirt_d** | Difference in shared blocks dirtied from the previous snapshot_time |
| **sb_write_d** | Difference in shared blocks written from the previous snapshot_time |

### Sample output

```
select * from pgstat_snap_diff order by 1;
```

| snapshot_time          | queryid               | query        | datname  | usename  | wait_event_type  | wait_event   | rows_d  | calls_d  | exec_ms_d  | sb_hit_d | sb_read_d | sb_dirt_d | sb_write_d |
|-------------------------|-----------------------|--------------|----------|----------|-------------------|-------------|---------|---------|------------|----------|-----------|-----------|------------|
| 2025-03-25 11:00:19     | 4380144606300689468   | UPDATE pgbench_tell | postgres | postgres | Lock            | transactionid | 4485    | 4485    | 986.262098 | 22827   | 0         | 0         | 0          |
| 2025-03-25 11:00:20     | 4380144606300689468   | UPDATE pgbench_tell | postgres | postgres | Lock            | transactionid | 1204    | 1204    | 228.822413 | 6115    | 0         | 0         | 0          |
| 2025-03-25 11:00:20     | 7073332947325598809   | UPDATE pgbench_bran | postgres | postgres | Lock            | transactionid | 1204    | 1204    | 1758.190499 | 5655    | 0         | 0         | 0          |
| 2025-03-25 11:00:21     | 7073332947325598809   | UPDATE pgbench_bran | postgres | postgres | Lock            | transactionid | 1273    | 1273    | 2009.227575 | 6024    | 0         | 0         | 0          |
| 2025-03-25 11:00:22     | 2931033680287349001   | UPDATE pgbench_acco | postgres | postgres | Client          | ClientRead     | 9377    | 9377    | 1818.464415 | 66121   | 3699      | 7358      | 35         |
| 2025-03-25 11:00:22     | 7073332947325598809   | UPDATE pgbench_bran | postgres | postgres | Lock            | transactionid | 1356    | 1356    | 1659.806856 | 6341    | 0         | 0         | 0          |
| 2025-03-25 11:00:23     | 7073332947325598809   | UPDATE pgbench_bran | postgres | postgres | Lock            | transactionid | 1168    | 1168    | 1697.322874 | 5484    | 0         | 0         | 0          |
| 2025-03-25 11:00:24     | 7073332947325598809   | UPDATE pgbench_bran | postgres | postgres | [NULL]          | [NULL]         | 1135    | 1135    | 1539.999618 | 5237    | 0         | 0         | 0          |
| 2025-03-25 11:00:24     | 5744520630148654507   | SELECT abalance FROM | postgres | postgres | [NULL]          | [NULL]         | 11679   | 11679   | 114.347514  | 49451   | 0         | 0         | 0          |
| 2025-03-25 11:00:25     | 7073332947325598809   | UPDATE pgbench_bran | postgres | postgres | Lock            | transactionid | 1274    | 1274    | 1861.747733 | 6043    | 0         | 0         | 0          |
| 2025-03-25 11:00:26     | 7073332947325598809   | UPDATE pgbench_bran | postgres | postgres | Lock            | transactionid | 1080    | 1080    | 1855.803660 | 5080    | 0         | 0         | 0          |
| 2025-03-25 11:00:27     | 7073332947325598809   | UPDATE pgbench_bran | postgres | postgres | Lock            | transactionid | 1155    | 1155    | 1669.499608 | 5373    | 0         | 0         | 0          |
| 2025-03-25 11:00:27     | 7113545590461720994   | SELECT CASE WHEN pg | postgres | postgres | Client          | ClientRead     | 1       | 1       | 0.098783    | 0       | 0         | 0         | 0          |
| 2025-03-25 11:00:28     | 7073332947325598809   | UPDATE pgbench_bran | postgres | postgres | Lock            | transactionid | 1178    | 1178    | 1623.365492 | 5466    | 0         | 0         | 0          |
| 2025-03-25 11:00:29     | 2931033680287349001   | UPDATE pgbench_acco | postgres | postgres | Client          | ClientRead     | 8350    | 8350    | 1456.806356 | 58400   | 3317      | 6135      | 30         |


## pgstat_snap_diff_all
This view contains the difference between the previous and next execution of a queryid/dbid pair and the sum of the fields as recorded in pg_stat_statements at that time:

| Column Name | Description |
|-------------|-------------|
| **snapshot_time** | Timestamp |
| **queryid** | Query ID |
| **query** | Query Text (first 20 characters) |
| **datname** | Database Name |
| **usename** | Username |
| **wait_event_type** | Event Type - NULL if no wait occurred |
| **wait_event** | Wait Event - NULL if no wait occurred |
| **rows** | Value of rows at this time in pg_stat_statements |
| **rows_d** | Difference in rows from the previous snapshot_time |
| **calls** | Value of calls at this time in pg_stat_statements |
| **calls_d** | Difference in calls from the previous snapshot_time |
| **exec_ms** | Value of total execution time at this time in pg_stat_statements |
| **exec_ms_d** | Difference in total execution time from the previous snapshot_time |
| **sb_hit** | Value of shared block hits at this time in pg_stat_statements |
| **sb_hit_d** | Difference in shared block hits from the previous snapshot_time |
| **sb_read** | Value of shared block reads at this time in pg_stat_statements |
| **sb_read_d** | Difference in shared block reads from the previous snapshot_time |
| **sb_dirt** | Value of shared blocks dirtied at this time in pg_stat_statements |
| **sb_dirt_d** | Difference in shared blocks dirtied from the previous snapshot_time |
| **sb_write** | Value of shared blocks written at this time in pg_stat_statements |
| **sb_write_d** | Difference in shared blocks written from the previous snapshot_time |

### Sample output

```
select * from pgstat_snap_diff_all order by 1;
```
| snapshot_time          | queryid               | query        | datname  | usename  | wait_event_type  | wait_event     | rows    | rows_d  | calls  | calls_d  | exec_ms     | exec_ms_d  | sb_hit  | sb_hit_d | sb_read | sb_read_d | sb_dirt | sb_dirt_d | sb_write | sb_write_d |
|-------------------------|-----------------------|--------------|----------|----------|-------------------|-------------|---------|---------|--------|---------|-------------|------------|---------|----------|---------|-----------|---------|-----------|----------|------------|
| 2025-03-25 11:00:19     | 4380144606300689468   | UPDATE pgbench_tell | postgres | postgres | Lock            | transactionid | 241818 | 4485    | 241818 | 4485    | 39693.679945 | 986.262098 | 1237102 | 1237098  | 4       | 0         | 90       | 0         | 22       | 0          |
| 2025-03-25 11:00:20     | 4380144606300689468   | UPDATE pgbench_tell | postgres | postgres | Lock            | transactionid | 243022 | 1204    | 243022 | 1204    | 39922.502358 | 228.822413 | 1243217 | 1243213  | 4       | 0         | 90       | 0         | 22       | 0          |
| 2025-03-25 11:00:20     | 7073332947325598809   | UPDATE pgbench_bran | postgres | postgres | Lock            | transactionid | 243020 | 1204    | 243020 | 1204    | 322868.346002 | 1758.190499 | 1142441 | 1142440  | 1       | 0         | 71       | 0         | 21       | 0          |
| 2025-03-25 11:00:21     | 7073332947325598809   | UPDATE pgbench_bran | postgres | postgres | Lock            | transactionid | 244293 | 1273    | 244293 | 1273    | 324877.573577 | 2009.227575 | 1148465 | 1148464  | 1       | 0         | 71       | 0         | 21       | 0          |
| 2025-03-25 11:00:22     | 2931033680287349001   | UPDATE pgbench_acco | postgres | postgres | Client          | ClientRead     | 245652 | 9377    | 245652 | 9377    | 51188.172394 | 1818.464415 | 2140583 | 2022215  | 122067   | 3699      | 233128   | 7358      | 2150     | 35         |
| 2025-03-25 11:00:22     | 7073332947325598809   | UPDATE pgbench_bran | postgres | postgres | Lock            | transactionid | 245649 | 1356    | 245649 | 1356    | 326537.380433 | 1659.806856 | 1154806 | 1154805  | 1       | 0         | 71       | 0         | 21       | 0          |
| 2025-03-25 11:00:23     | 7073332947325598809   | UPDATE pgbench_bran | postgres | postgres | Lock            | transactionid | 246817 | 1168    | 246817 | 1168    | 328234.703307 | 1697.322874 | 1160290 | 1160289  | 1       | 0         | 71       | 0         | 21       | 0          |
| 2025-03-25 11:00:24     | 7073332947325598809   | UPDATE pgbench_bran | postgres | postgres | [NULL]          | [NULL]         | 247952 | 1135    | 247952 | 1135    | 329774.702925 | 1539.999618 | 1165527 | 1165526  | 1       | 0         | 71       | 0         | 21       | 0          |
| 2025-03-25 11:00:24     | 5744520630148654507   | SELECT abalance FROM | postgres | postgres | [NULL]          | [NULL]         | 247954 | 11679   | 247954 | 11679   | 2917.940273  | 114.347514  | 1121749 | 1121749  | 0       | 0         | 0        | 0         | 0        | 0          |
| 2025-03-25 11:00:25     | 7073332947325598809   | UPDATE pgbench_bran | postgres | postgres | Lock            | transactionid | 249226 | 1274    | 249226 | 1274    | 331636.450658 | 1861.747733 | 1171570 | 1171569  | 1       | 0         | 71       | 0         | 21       | 0          |
| 2025-03-25 11:00:26     | 7073332947325598809   | UPDATE pgbench_bran | postgres | postgres | Lock            | transactionid | 250306 | 1080    | 250306 | 1080    | 333492.254318 | 1855.803660 | 1176650 | 1176649  | 1       | 0         | 71       | 0         | 21       | 0          |
| 2025-03-25 11:00:27     | 7073332947325598809   | UPDATE pgbench_bran | postgres | postgres | Lock            | transactionid | 251461 | 1155    | 251461 | 1155    | 335161.753926 | 1669.499608 | 1182023 | 1182022  | 1       | 0         | 71       | 0         | 21       | 0          |
| 2025-03-25 11:00:27     | 7113545590461720994   | SELECT CASE WHEN pg | postgres | postgres | Client          | ClientRead     | 3783    | 1       | 3783    | 1       | 373.398846    | 0.098783    | 0       | 0        | 0       | 0         | 0        | 0         | 0        | 0          |
| 2025-03-25 11:00:28     | 7073332947325598809   | UPDATE pgbench_bran | postgres | postgres | Lock            | transactionid | 252639 | 1178    | 252639 | 1178    | 336785.119418 | 1623.365492 | 1187489 | 1187488  | 1       | 0         | 71       | 0         | 21       | 0          |
| 2025-03-25 11:00:29     | 2931033680287349001   | UPDATE pgbench_acco | postgres | postgres | Client          | ClientRead     | 254002 | 8350    | 254002 | 8350    | 52644.978750  | 1456.806356 | 2198983 | 2076916  | 125384   | 3317      | 239263   | 6135      | 2180     | 30         |

# Query Examples 
Depending on screensize, you might want to set format to aligned, especially when querying *pstat_snap_diff_all*:

```
\pset format aligned
```

What was happening:
```
select * from pgstat_snap_diff order by 1;
```

What was every query doing:
```
select * from pgstat_snap_diff order by 2,1;
```

Which database touched the most rows:
```
select sum(rows_d),datname from pgstat_snap_diff group by datname;
```

Which query DML affected the most rows:
```
select sum(rows_d),queryid,query from pgstat_snap_diff where upper(query) not like 'SELECT%' group by queryid,query;
```

What wait events happened which weren't of type Client:
```
select * from pgstat_snap_diff where wait_event_type is not null and wait_event_type <> 'Client' order by 2,1;
```

If needed you can access all columns for a particular query directly in the history tables:
```
select * from pgstat_snap_stat_history where queryid='123455678909876';
```
