#!/usr/bin/env python3
"""This script is run in 2 scenarios:
- when pcp_attach_node command is executed
- automatically (if pgpool auto_failback option is enabled) when PgPool detects that a previously evicted node is now again in streaming replication mode with the current primary

It attachs the previously evicted node back to the cluster

In this script, we check the postgres variable synchronous_standby_names, to convert asynchronous replication to synchronous replication
"""

import logging
import sys
from datetime import datetime

import pytz
from util import Database, number_of_instances_up


if __name__ == '__main__':
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

    FAILED_NODE_ID = int(args[1])           # %d
    FAILED_NODE_HOST = args[2]              # %h
    FAILED_NODE_PORT = int(args[3])         # %p
    FAILED_NODE_PGDATA = args[4]            # %D
    NEW_MAIN_NODE_ID = int(args[5])         # %m
    NEW_MAIN_NODE_HOST = args[6]            # %H
    OLD_MAIN_NODE_ID = int(args[7])         # %M
    OLD_PRIMARY_NODE_ID = int(args[8])      # %P
    NEW_MAIN_NODE_PORT = int(args[9])       # %r
    NEW_MAIN_NODE_PGDATA = args[10]         # %R
    OLD_PRIMARY_NODE_HOST = args[11]        # %N
    OLD_PRIMARY_NODE_PORT = int(args[12])   # %S

    PGPOOL_SUPERUSER = 'pgpool_superuser'
    PGPOOL_SUPERUSER_PASSWORD = 'pgpool_superuser_password'

    PGPOOL_STATUS = '/var/log/pgpool/pgpool_status'

    logger.info(f"Attached Node: {FAILED_NODE_ID}: {FAILED_NODE_HOST}:{FAILED_NODE_PORT}\n"
                f"Primary Node: {OLD_PRIMARY_NODE_ID}: {OLD_PRIMARY_NODE_HOST}:{OLD_PRIMARY_NODE_PORT}")


    if number_of_instances_up(PGPOOL_STATUS) >= 2:
        logger.info(f"Convert replication to synchronous on primary {OLD_PRIMARY_NODE_HOST}")
        primary = Database(
            OLD_PRIMARY_NODE_HOST, OLD_PRIMARY_NODE_PORT,
            PGPOOL_SUPERUSER, PGPOOL_SUPERUSER_PASSWORD, database='postgres'
        )
        primary.set_synchronous_standby_names("'*'")
        primary.close_connection()


        logger.info(f"Convert replication to synchronous on replica being re-attached {FAILED_NODE_HOST}")
        replica = Database(
            FAILED_NODE_HOST, FAILED_NODE_PORT,
            PGPOOL_SUPERUSER, PGPOOL_SUPERUSER_PASSWORD, database='postgres'
        )
        replica.set_synchronous_standby_names("'*'")
        replica.close_connection()

    logger.info(f'Success: Failback complete for {FAILED_NODE_HOST}!')
    sys.exit(0)
