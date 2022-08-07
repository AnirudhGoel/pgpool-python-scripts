#!/usr/bin/env python3

import logging
import sys
import psycopg2

logger = logging.getLogger(__name__)

class Database:
    def __init__(self, host, port, username, password, database) -> None:
        """Database Constructor: Establishes database connection using passed parameters."""
        retry = 2
        while retry:
            try:
                self.conn = psycopg2.connect(
                    host=host, port=port,
                    user=username, password=password, database=database
                )
                logger.info(f'Connected to {host} instance')
                self.cursor = self.conn.cursor()
                retry = 0
            except psycopg2.DatabaseError:
                logger.error(f'Failed to connect to {host} instance')
                retry -= 1
                if not retry:
                    logger.critical('Terminating')
                    sys.exit(1)
                logger.info('Attempting to connect again')

    def close_connection(self):
        """Perform clean up - Logging and Closing database connection."""
        logger.info("Closing Database Connection")
        self.cursor.close()
        self.conn.close()

    def execute(self, query, fetch=False, args=None, autocommit=False):
        """Utility function to execute database queries."""
        data = None

        try:
            self.conn.autocommit = autocommit
            self.cursor = self.conn.cursor()

            self.cursor.execute(query, args)
            logger.info("Executed query: {}".format(self.cursor.query.decode('utf-8')))

            if fetch is True:
                data = self.cursor.fetchone()

            if not autocommit:
                self.conn.commit()

            return data

        except psycopg2.DatabaseError as e:
            logger.error(f'Database error {type(e).__name__}: {str(e)}')
            if self.conn.status == psycopg2.extensions.STATUS_BEGIN:
                logger.info("Rolling back active transaction")
                self.conn.rollback()
            self.close_connection()
            sys.exit(1)
        except psycopg2.errors.ObjectNotInPrerequisiteState as e:
            logger.critical(f"Trying to execute pg_promote on primary?\nError: {type(e).__name__}: {str(e)}")
            self.close_connection()
            sys.exit(1)
        except Exception as e:
            logger.error(f"Exception: {type(e).__name__}: {str(e)}")
            self.close_connection()
            sys.exit(1)

    def set_synchronous_standby_names(self, value):
        """Update parameter synchronous_standby_names to specified value (enables/disables sync replication)."""
        logger.info(f"Setting synchronous_standby_names = {value}")
        self.execute(f"ALTER SYSTEM SET synchronous_standby_names = {value};", autocommit=True)
        self.execute(f"SELECT pg_reload_conf();", autocommit=True)


def number_of_instances_up(pgpool_status_file):
    """Returns number of instances that pgpool recognises as up based on pgpool_status file."""
    num_of_instances = 0

    with open(pgpool_status_file, 'r') as status_file:
        for line in status_file:
            if 'up' in line:
                num_of_instances += 1
    logger.info(f'{num_of_instances} instance(s) up')
    return num_of_instances

def is_instance_down(host, port, username, password):
    try:
        conn = psycopg2.connect(
            host=host, port=port,
            user=username, password=password
        )
    except psycopg2.OperationalError:
        return True
    return False
