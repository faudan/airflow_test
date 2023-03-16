from datetime import datetime, timedelta
from airflow.decorators import dag, task
import pandas as pandas
import psycopg2

DB_NAME = ""
DB_USER = "airflow"
DB_PASS = "airflow"
DB_HOST = "postgres"
DB_PORT = "5432"


def Employee(id, first_name, last_name, email, phone):
    return {
        "userId": id,
        "firstName": first_name,
        "lastName": last_name,
        "email": email,
        "phone": phone,
    }
   
class OktaEmployee:
    def __init__(self, employee):
        self.employee = employee

    def export(self):
        return {'userId': self.employee['userId'], 'email': self.employee['email'], 'service': 'okta', 'dag': 'granular'}    

@dag(
    dag_id="0_Employee_Sync_Granular_Steps",
    description="DAG for employees - PoC",
    start_date=datetime(2022, 3, 1, 2),
    schedule_interval=None,
)
def employees_dag():
    # db_connection = f'./dags/data/stored_employees.json'
    db_connection = psycopg2.connect(database=DB_NAME,
                            user=DB_USER,
                            password=DB_PASS,
                            host=DB_HOST,
                            port=DB_PORT)
    
    users_endpoint_url = r"./dags/input/employees.csv"
    okta_users_endpoint_url = r"./dags/output/okta_user_data.json"
    
    @task
    def db_setup():
        cur = db_connection.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS Employees (
        id         TEXT         PRIMARY KEY,
        first_name VARCHAR(255)   NOT NULL,
        last_name  VARCHAR(255)   NOT NULL,
        email  VARCHAR(255)   NOT NULL,
        phone  VARCHAR(255)   NOT NULL
        )""")
        db_connection.commit()
    
    @task(task_id='fetch_external_users_data')
    def fetch_external_users_data():
        local_employees = pandas.read_csv(users_endpoint_url)
        return local_employees.to_dict('records')
    
    @task(task_id='validate_data')
    def validate_data(task_instance = None):
        employees = task_instance.xcom_pull(task_ids="fetch_external_users_data")
        return employees
    
    @task(task_id='map_external_to_lattice')
    def map_external_to_lattice(task_instance = None):
        employees = task_instance.xcom_pull(task_ids="validate_data")
        return [Employee(employee['UserId'], employee['FirstName'], employee['LastName'], employee['Email'], employee['Phone']) for employee in employees]

    @task
    def save_user_data(task_instance = None):
        employees = task_instance.xcom_pull(task_ids="map_external_to_lattice")
        v = ['(' + ','.join(["\'"+employee['userId']+"\'", "\'"+employee['firstName']+"\'", "\'"+employee['lastName']+"\'", "\'"+employee['email']+"\'", "\'"+employee['phone']+"\'"])+')' for employee in employees]
        values = ','.join(v)

        cur = db_connection.cursor()

        cur.execute("""
            INSERT INTO Employees (id,first_name, last_name, email, phone) VALUES
        """+ values + " ON CONFLICT ON CONSTRAINT employees_pkey DO NOTHING")
        db_connection.commit()
        # pandas.DataFrame(employees).to_json(path, orient='records', indent=1)
    
    @task(task_id='load_user_data')
    def load_user_data():
        cur = db_connection.cursor()

        cur.execute("SELECT id, first_name, last_name, email, phone FROM Employees")

        rows = cur.fetchall()
        return [Employee(data[0], data[1] , data[2], data[3], data[4]) for data in rows]
        # employees_json = pandas.read_json(db_connection).to_dict('records')
        # return [Employee(employee['userId']) for employee in employees_json]

    @task(task_id='map_lattice_to_okta')
    def map_lattice_to_okta(task_instance = None):
        employees = task_instance.xcom_pull(task_ids="load_user_data")

        return [OktaEmployee(employee).export() for employee in employees]

    @task(task_id='send_data')
    def send_data(task_instance = None):
        user_data = task_instance.xcom_pull(task_ids="map_lattice_to_okta")
        pandas.DataFrame(user_data).to_json(okta_users_endpoint_url, orient='records', indent=1)

    db_setup() >> \
    fetch_external_users_data() >> \
    validate_data() >> \
    map_external_to_lattice() >> \
    save_user_data() >> \
    load_user_data() >> \
    map_lattice_to_okta() >> \
    send_data()


employees_dag()