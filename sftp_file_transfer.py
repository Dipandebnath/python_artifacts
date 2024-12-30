import pandas as pd,psycopg2 as ps
import configparser,os
import paramiko
from datetime import datetime
from io import StringIO

class sftp_process():
    def __init__(self,config):

        ### sftp conn details
        self.host =  config.get('sftp_details','host')    
        self.port = config.get('sftp_details','port')                  
        self.username = config.get('sftp_details','username')   
        self.password = config.get('sftp_details','password') 

        ## db connection details
        self.server = config.get('database', 'host')
        self.dbname = config.get('database', 'dbname')
        self.db_user= config.get('database', 'user')
        self.db_password= config.get('database', 'password')
        self.db_port = config.get('database', 'port')

    def data_transformation(self):

        # Create a connection to the database
        connection = ps.connect(
            host=self.server,
            database=self.dbname,
            user=self.db_user,
            password=self.db_password,
            port=self.db_port
        )

        # Create a cursor object
        cursor = connection.cursor()

        # Write the SQL query to retrieve data
        query = """SELECT * FROM bts.bts_production_estimate where as_on_date in 
        (select max(as_on_date) from bts.bts_production_estimate) """

        # Execute the query
        cursor.execute(query)
        data = cursor.fetchall()

        # Get column names from the cursor description
        columns = [desc[0] for desc in cursor.description]

        # Store the data in a Pandas DataFrame
        df = pd.DataFrame(data, columns=columns)

        print("Data is stored into the dataframe")
        # Close the cursor and connection
        cursor.close()
        connection.close()

        cols_to_take=['material','plant','production_date','est_quantity_in_kg',
       'est_quantity_in_mt', 'est_quantity_in_lb']

        df=df[df['plant'].notna()]
        df=df[df['production_date'].notna()]
        df=df[df['est_quantity_in_mt'].notna()]

        # Replace NaN values in material with values from product_code
        df['material'] = df['material'].fillna(df['product_code'])
        df['material']=df['material'].astype(str)
        df['plant']=df['plant'].astype(str)

        df=df[cols_to_take]

        df.rename(columns={'material':'Item','plant':'Plant Code','production_date':'Day',
        'est_quantity_in_kg':'Production_QTY_KG','est_quantity_in_mt':'Production_QTY_MT','est_quantity_in_lb':'Production_QTY_lb'},inplace=True)

        print("Data transformation is done")
        timestamp=datetime.today().strftime('%Y%m%d_%H%M%S')
        # Save the DataFrame to a DSV file with pipe delimiter, text qualifier, and UTF-8 encoding
        df.to_csv(f'Production_orders_{timestamp}.dsv', sep='|', index=False, header=True, quotechar='"', encoding='utf-8')

        return df 

    def sftp_file_transfer(self):

        # SFTP server credentials
        host = self.host       
        port = self.port                      
        username = self.username    
        password = self.password    
        remote_directory = '/CPET/Inbound'  # Directory you want to access

        timestamp=datetime.today().strftime('%Y%m%d_%H%M%S')
        remote_filename = f'Production_orders_{timestamp}.dsv'  
        remote_path = os.path.join(remote_directory,remote_filename).replace('\\','/') 
        

        # Create an SFTP client connection
        try:
            # Initialize the SSH client
            ssh = paramiko.SSHClient()
            
            # Automatically add the server's host key (to avoid manual acceptance)
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            
            # Connect to the SFTP server
            ssh.connect(host, port=port, username=username, password=password)
            print("SFTP connection established")

            
            # Open an SFTP session
            sftp = ssh.open_sftp()

            
            # # Change the working directory to the desired path
            # sftp.chdir(remote_directory)
            
            # print(f"Successfully connected to {host} and changed to {remote_directory}")

            ### Calling the data transformation function
            df=self.data_transformation()

            # Create a string buffer to hold the CSV data
            csv_buffer = StringIO()

            # Save the DataFrame to the buffer in DSV format (pipe-delimited in this case)
            df.to_csv(csv_buffer, sep='|', index=False, header=True, quotechar='"', encoding='utf-8')

            # Move the buffer's position to the beginning of the stream
            csv_buffer.seek(0)

            # Upload the file to the SFTP server with the specified name
            with sftp.open(remote_path, 'w') as remote_file:
                remote_file.write(csv_buffer.getvalue())

            print(f"File successfully uploaded to {remote_path}")
            
            
            # Close the SFTP connection
            sftp.close()
            
            # Close the SSH connection
            ssh.close()
            print("SFTP connection is closed")
            
        except Exception as e:
            print(f"Error: {e}")

        pass




if __name__=='__main__':
    
    # Create a ConfigParser object
    config = configparser.ConfigParser()

    ## Read the config file
    config.read(r'config.ini')

## Calling the constructor
    sftp=sftp_process(config)


    ### calling the sftp connection
    sftp.sftp_file_transfer()