import glob
import getpass
import inquirer
import pandas as pd
from rich import print
from src.adapter.mysql import MySQLConnection
from src.adapter.postgres import PostgreSQLConnection
from sshtunnel import SSHTunnelForwarder

class Loader:
    def __init__(self) -> None:
        self.source_conn = None
        self.destination_conn = None

    async def run_cli(self) -> None:
        data_source = inquirer.list_input("Choose data sources: ", choices=['Database', 'Files'])
        if data_source.lower() == 'files':
            file_source = inquirer.list_input("Choose file sources: ", choices=['CSV', 'JSON', 'Excel'])
            if file_source.lower() == 'csv':
                files = glob.glob(f'./files/*.csv')
                file_source = inquirer.list_input("Choose file sources: ", choices=[ i.split('\\')[1] for i in files ])
                data = pd.read_csv(f'./files/{file_source}')
            elif file_source.lower() == 'json':
                files = glob.glob(f'./files/*.json')
                file_source = inquirer.list_input("Choose file sources: ", choices=[ i.split('\\')[1] for i in files ])
                data = pd.read_json(f'./files/{file_source}')
            elif file_source.lower() == 'excel':
                files = glob.glob(f'./files/*.xlsx') + glob.glob(f'./files/*.xls')
                file_source = inquirer.list_input("Choose file sources: ", choices=[ i.split('\\')[1] for i in files ])
                data = pd.read_excel(f'./files/{file_source}')

            schema = self.get_schema_from_dataframe(data, file_source.split('.')[0])
            

        elif data_source.lower() == 'database':
            ssh_option = inquirer.list_input("Would you like to use SSH tunnel for your source database?", choices=['Yes.', 'Nope.'])

            if ssh_option.lower() == 'yes.':
                print(f"[bold yellow]SSH Host/IP: [/bold yellow]", end=' ')
                ssh_ip = input()
                print(f"[bold yellow]SSH Port: [/bold yellow]", end=' ')
                ssh_port = input()
                print(f"[bold yellow]Username: [/bold yellow]", end=' ')
                ssh_user = input()
                print(f"[bold yellow]Password: [/bold yellow]", end=' ')
                ssh_pass = getpass.getpass("")

                source_db_credentials, source_selected_db = self.get_db_input("source")

                ssh_config = {
                    'ssh_address_or_host': (ssh_ip, int(ssh_port)),
                    'ssh_username': ssh_user,
                    'ssh_password': ssh_pass,
                    'remote_bind_address':(source_db_credentials['host'], int(source_db_credentials['port']))
                }          

                with SSHTunnelForwarder(**ssh_config) as tunnel:
                    source_db_credentials['host'] = '127.0.0.1'
                    source_db_credentials['port'] = tunnel.local_bind_port
                    
                    self.source_conn = self.connect_db(source_db_credentials, source_selected_db)
            else:
                source_db_credentials, source_selected_db = self.get_db_input("source")
                self.source_conn = self.connect_db(source_db_credentials, source_selected_db)

            source_pool = await self.source_conn.create_pool()

            source_table = await self.get_table_names(self.source_conn, source_pool, source_selected_db)

            selected_source_tables = inquirer.list_input("Choose source database?", choices=source_table)

            data = await self.get_data(self.source_conn, source_pool, selected_source_tables)

            schema = await self.get_schema_from_db(self.source_conn, source_pool, selected_source_tables, source_selected_db)

        ssh_option = inquirer.list_input("Would you like to use SSH tunnel for your destination database?", choices=['Yes.', 'Nope.'])

        if ssh_option.lower() == 'yes.':
            print(f"[bold yellow]SSH Host/IP: [/bold yellow]", end=' ')
            ssh_ip = input()
            print(f"[bold yellow]SSH Port: [/bold yellow]", end=' ')
            ssh_port = input()
            print(f"[bold yellow]Username: [/bold yellow]", end=' ')
            ssh_user = input()
            print(f"[bold yellow]Password: [/bold yellow]", end=' ')
            ssh_pass = getpass.getpass("")

            destination_db_credentials, destination_selected_db = self.get_db_input("destination")

            ssh_config = {
                'ssh_address_or_host': (ssh_ip, int(ssh_port)),
                'ssh_username': ssh_user,
                'ssh_password': ssh_pass,
                'remote_bind_address':(destination_db_credentials['host'], int(destination_db_credentials['port']))
            }          

            with SSHTunnelForwarder(**ssh_config) as tunnel:
                destination_db_credentials['host'] = '127.0.0.1'
                destination_db_credentials['port'] = tunnel.local_bind_port
                
                self.destination_conn = self.connect_db(destination_db_credentials, destination_selected_db)
        else:
            destination_db_credentials, destination_selected_db = self.get_db_input("destination")
            self.destination_conn = self.connect_db(destination_db_credentials, destination_selected_db)

    async def get_table_names(self, conn, pool, database: str) -> list:
        query = ""
        if database.lower() == 'mysql':
            query = """
                    show tables
                """
        elif database.lower() == 'postgresql':
            query = """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public' AND table_type = 'BASE TABLE';
                """

        _, result = await conn.get_query(pool, query)

        return [a[0] for a in list(result)]
                

    def get_db_input(self, title: str) -> (dict, str):
        selected_db = inquirer.list_input(f"Choose {title} database?", choices=['MySQL', 'PostgreSQL'])
        print(f"[yellow]Please input your {selected_db} database credentials.[/yellow]")
        db_credentials = {}
        print(f"[bold yellow]Host: [/bold yellow]", end=' ')
        db_credentials['host'] = input()
        print(f"[bold yellow]Port: [/bold yellow]", end=' ')
        db_credentials['port'] = input() or 0
        print(f"[bold yellow]User: [/bold yellow]", end=' ')
        db_credentials['user'] = input()
        print(f"[bold yellow]Password: [/bold yellow]", end=' ')
        db_credentials['password'] = getpass.getpass("")
        print(f"[bold yellow]Database: [/bold yellow]", end=' ')
        db_credentials['db'] = input()
        
        return db_credentials, selected_db
    
    def connect_db(self, credentials: dict, selected_db: str):
        if selected_db.lower() == 'mysql':
            conn = MySQLConnection(credentials)
        elif selected_db.lower() == 'postgresql':
            conn = PostgreSQLConnection(credentials)
        return conn
    
    async def get_data(self, client, pool, table: str) -> pd.DataFrame:
        query = f"select * from {table}"
        columns_name, result = await client.get_query(pool, query)
        data = [{columns_name[i] : row[i] for i in range(len(columns_name))} for row in result]
        df = pd.DataFrame(data)
        for column in df.columns:
            if df[column].dtype == 'object':
                df[column] = df[column].astype('string')
        return df
    
    def get_schema_from_dataframe(self, df: pd.DataFrame, tablename: str) -> list:
        schema = df.dtypes.to_dict()
        for key, val in schema.items():
            if str(val) == 'int64':
                schema[key] = 'int'
            elif str(val) == 'float64':
                schema[key] = 'float'
            elif str(val) == 'object':
                schema[key] = 'varchar(255)'
            elif str(val) == 'bool':
                schema[key] = 'boolean'
            else :
                schema[key] = 'datetime'
        
        sql = f"CREATE table {tablename} (" + ",".join([f"{cols} {types}" for cols, types in schema.items()]) + ");"
        return sql
    
    async def get_schema_from_db(self, client, pool, table, selected_db) -> str:
        if selected_db.lower() == 'mysql':
            query = f"SHOW CREATE TABLE {table}"
            columns_name, result = await client.get_query(pool, query)
            data = [{columns_name[i] : row[i] for i in range(len(columns_name))} for row in result]
            df = pd.DataFrame(data)
            return df['Create Table'].values[0]
        
        elif selected_db.lower() == 'postgresql':
            query = f"""
                    SELECT pg_table_def.table_name, pg_table_def.column_name, pg_table_def.data_type
                    FROM information_schema.columns AS pg_table_def
                    WHERE pg_table_def.table_schema = 'public' AND pg_table_def.table_name = '{table}';
            """
            columns_name, result = await client.get_query(pool, query)
            data = [{columns_name[i] : row[i] for i in range(len(columns_name))} for row in result]
            df = pd.DataFrame(data)
            schema = ",".join([ f"{row['column_name']} {row['data_type']}" for _, row in df.iterrows() ])
        

def DatabaseLoader() -> Loader:
    return Loader()