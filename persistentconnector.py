import psycopg2


class PersistentDatabaseConnection:
    def __init__(self, *, dbname=None, user=None, password=None, host=None, port=None):
        """
        Connects to the database and caches the connection object.
        :param dbname: Database name
        :param user: User name
        :param password: Password
        :param host: Host to connect to
        :param port: Port to use
        """
        self.conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
        self.cursor = self.conn.cursor()

    def execute_sql(self, sql_statement, sql_arguments):
        """
        Executes an SQL statement with a previously cached connection in a separate transaction.
        :param sql_statement: SQL statement to execute
        :param sql_arguments: SQL arguments
        :return:
        """
        # From: http://initd.org/psycopg/docs/usage.html
        with self.conn:
            with self.cursor as mycursor:
                mycursor.execute(sql_statement, sql_arguments)

