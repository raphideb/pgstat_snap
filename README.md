# Purpose of this extension
The cumulative statistics system (CSS) in PostgreSQL and pg_stat_statements in particular lack any timing information, all values are cumulative and the only way to figure out the difference between query executions is to reset the stats every time or work with averages. 

With the pgstat_snap extension, you can create timestamped snapshots of pg_stat_statements and pg_stat_activity when needed. It also provides views that show the difference between every snapshot for every query and database. 

The full documentation for the extension with view descriptions and query examples is available here: https://raphideb.github.io/postgres/pgstat_snap/

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

## Querying the views
To see second by second what queries were executed and what they were doing:
```
select * from pgstat_snap_diff order by 1;
```
## Reset
Because everything is timestamped, a reset is usually not needed between CALLs to create_snapshot. But you can to cleanup and keep the tables smaller. You can also reset pg_stats*.

Reset all pgstat_snap tables with:
```
SELECT pgstat_snap_reset();   -> reset only pgstat_snap.pgstat*history tables
SELECT pgstat_snap_reset(1);  -> also select pg_stat_statements_reset()
SELECT pgstat_snap_reset(2);  -> also select pg_stat_reset()
```
## Uninstall
To completely uninstall pgstat_snap, run:
```
DROP EXTENSION pgstat_snap;
```
## Help
The above is also available as a help function in the extension:
```
SELECT pgstat_snap_help();
```
