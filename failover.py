#!/usr/bin/env python3
"""This script performs a failover whenever any of the PostgreSQL nodes in the cluster fail.
In addition, if there's only one PostgreSQL node left in the cluster, it changes synchronous replication to asynchronous replication otherwise all transactions will wait indefinitely.

If the failed node is a replica, it just updates the status of synchronous replication.
If the failed node is a primary, it promotes the NEW_MAIN_NODE (usaully the first node up with the lowest ID as per pgpool), and updates sync replication.
"""

import logging
import os
import sys
import time
from datetime import datetime

import pytz

from util import Database, is_instance_down, number_of_instances_up


if __name__ == '__main__':
    # Set your own timezone here
    cet = pytz.timezone('CET')
    logging.basicConfig(
        format=f"{datetime.now(cet).strftime('%Y-%m-%dT%H:%M:%S%z')} [%(filename)s:%(lineno)d] %(processName)s[%(process)d] %(levelname)s: %(message)s",
        level=logging.DEBUG,
        handlers=[
            logging.StreamHandler(sys.stdout),
        ]
    )
    logger = logging.getLogger()

    if len(sys.argv) < 13:
        logger.error("All script parameters not set")
        sys.exit(1)

    args = sys.argv

    FAILED_NODE_ID = int(args[1] or 0)           # %d
    FAILED_NODE_HOST = args[2]                   # %h
    FAILED_NODE_PORT = int(args[3] or 0)         # %p
    FAILED_NODE_PGDATA = args[4]                 # %D
    NEW_MAIN_NODE_ID = int(args[5] or 0)         # %m
    NEW_MAIN_NODE_HOST = args[6]                 # %H
    OLD_MAIN_NODE_ID = int(args[7] or 0)         # %M
    OLD_PRIMARY_NODE_ID = int(args[8] or 0)      # %P
    NEW_MAIN_NODE_PORT = int(args[9] or 0)       # %r
    NEW_MAIN_NODE_PGDATA = args[10]              # %R
    OLD_PRIMARY_NODE_HOST = args[11]             # %N
    OLD_PRIMARY_NODE_PORT = int(args[12] or 0)   # %S

    PG_VERSION = '12.5'
    PGPOOL_SUPERUSER = 'pgpool_superuser'
    PGPOOL_SUPERUSER_PASSWORD = 'pgpool_superuser_password'

    PGHOME = f'/usr/local/pgsql/pgsql-{PG_VERSION}'  # postgres installation location
    PG_CTL = f'{PGHOME}/bin/pg_ctl'
    PGPOOL_STATUS = '/var/log/pgpool/pgpool_status'  # 'logdir' from pgpool.conf

    if NEW_MAIN_NODE_ID < 0:
        logger.info('All nodes are down, cannot perform failover')

        # By default, if all postgres nodes go down, pgpool cannot recover from that situation without a restart
        # However, I use Nomad to control pgpool in my setup, so when all nodes go down I wait for the last primary
        # to come back up and then kill pgpool so that it can be automatically restarted by Nomad without manual intervention
        if FAILED_NODE_ID == OLD_PRIMARY_NODE_ID:
            logger.info('Waiting for the last primary to come back up')
            while is_instance_down(OLD_PRIMARY_NODE_HOST, OLD_PRIMARY_NODE_PORT,
                                    PGPOOL_SUPERUSER, PGPOOL_SUPERUSER_PASSWORD):
                logger.info(f'Primary {OLD_PRIMARY_NODE_HOST} is down, waiting for 10 seconds before next check')
                time.sleep(10)
            logger.info(f'Primary {OLD_PRIMARY_NODE_HOST} is up! Killing PgPool to be restarted by Nomad')
            os.system('pgpool -m fast stop')
        else:
            logger.critical('Last failed node was not the primary? Possible bug with PgPool')
            sys.exit(0)


    # If standby is down, just check sync replication on primary
    if FAILED_NODE_ID != OLD_PRIMARY_NODE_ID:
        logger.info('Standby node is down, skipping failover')

        if number_of_instances_up(PGPOOL_STATUS) < 2:
            primary = Database(
                OLD_PRIMARY_NODE_HOST, OLD_PRIMARY_NODE_PORT,
                PGPOOL_SUPERUSER, PGPOOL_SUPERUSER_PASSWORD, database='postgres'
            )
            primary.set_synchronous_standby_names("''")
            primary.close_connection()

        sys.exit(0)


    # If primary is down, check sync replication on replica and then promote it
    # Replica as per original config (before failover)
    replica = Database(
        NEW_MAIN_NODE_HOST, NEW_MAIN_NODE_PORT,
        PGPOOL_SUPERUSER, PGPOOL_SUPERUSER_PASSWORD, database='postgres'
    )

    if number_of_instances_up(PGPOOL_STATUS) < 2:
        replica.set_synchronous_standby_names("''")

    logger.info(f'Primary node is down, promote replica {NEW_MAIN_NODE_ID}: {NEW_MAIN_NODE_HOST}')
    replica.execute('SELECT pg_promote();', autocommit=True)
    replica.close_connection()

    logger.info(f'Success: {NEW_MAIN_NODE_HOST} successfully promoted to primary!')
    sys.exit(0)
