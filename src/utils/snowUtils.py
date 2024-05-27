import snowflake.connector

class SnowflakeConnector:
    def __init__(self, account, user, password, warehouse, database, schema):
        """AI is creating summary for __init__

        Args:
            account ([type]): [description]
            user ([type]): [description]
            password ([type]): [description]
            warehouse ([type]): [description]
            database ([type]): [descripition]
            schema ([type]): [description]
        """
        self.account = account
        self.user = user
        self.password = password
        self.warehouse = warehouse
        self.database = database
        self.schema =schema
        self.connection = None
        self.cursor =None
    
    def connect(self):
        """
        Establish a connection to snowflake.
        """
        self.connection = snowflake.connector.connect(
            user=self.user,
            password=self.password,
            account=self.account,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema
        )
        self.cursor = self.connection.cursor()

    def execute_query(self, query):
        """
        Execute a SQL query.
        """    
        if not self.connection or not self.cursor:
            raise Exception("connection not established. Call connect() method first.")
        
        self.cursor.execute(query)
        return self.cursor.fetchall()
    
    def close_connection(self):
        """
        Close the connection.
        """
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()    


            
            
#Snowflake connection parameters
account = 'ZOTKUJP-MD45367'
user = 'ADITI'
password = 'Hitesh#123'
database = 'SNOWFLAKE_SAMPLE_DATA'
schema = 'TPCH_SF1'
warehouse = 'COMPUTE_WH'

#Connect to snowflake
conn = snowflake.connector.connect(
    user = user,
    password = password,
    account =account,
    warehouse = warehouse,
    database = database,
    schema = schema
)

#Execute a query
query = "SELECT * From customer limit 5"
cursor = conn.cursor()
cursor.execute(query)

#Fetch results
rows = cursor.fetchall()
for row in rows:
    print(row)