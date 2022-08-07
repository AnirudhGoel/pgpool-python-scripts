# PgPool Python Scripts
Sample PgPool scripts for failover and failback in Python

[Official PgPool sample scripts](https://github.com/pgpool/pgpool2/tree/master/src/sample/scripts) are provided in bash but we prefer to use Python in my work team, so I rewrote the PgPool failover and failback scripts in Python 3.

### Features:

- Synchronous replication - These scripts assume synchronous streaming replication setup by default as it provides better guarantee of data consistency when your replica is promoted to a primary (and it should have all transactions that had been written to the old primary)
- Automatically updates synchronous replication to asynchronous replication when only one PostgreSQL node is left in the cluster, otherwise transactions will wait forever to commit
- No passwordless SSH setup required - Instead of using ssh to perform replica promotion and toggling synchronous replication mode, we use psycopg2 to connect to the instance and then perform these operations
- Requires postgres superuser - To avoid SSH, we use the [PostgreSQL `ALTER SYSTEM`](https://www.postgresql.org/docs/current/sql-altersystem.html) command which requires superuser privileges
- Does not use replication slots
- Logs to `stdout` by default but [additional log handlers can be added](https://docs.python.org/3/howto/logging.html)

### Notes:
- To adapt to an asynchronous replication setup, simply comment out the synchronous replication parts
- The scripts can work with more than 2 PostgreSQL nodes but, note that in case of more than 2 PostgreSQL nodes, you need an additional follow_primary script (that I haven't yet written in Python so please use the official pgpool sample scripts)
- Currently, I have only migrated failover and failback scripts. follow_primary and online_recovery scripts are not yet migrated
- Both failover and failback scripts use the same utility file (util.py)