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
        return {'userId': self.employee['userId'], 'email': self.employee['email'], 'service': 'okta', 'dag': 'condensed'}    


def fetch_external_users_data(path):
    local_employees = pandas.read_csv(path)
    return local_employees.to_dict('records')

def validate_data(employees):
    return employees

def map_external_to_lattice(employees):
    return [Employee(employee['UserId'], employee['FirstName'], employee['LastName'], employee['Email'], employee['Phone']) for employee in employees]

def save_user_data(employees, db_connection):
    v = ['(' + ','.join(["\'"+employee['userId']+"\'", "\'"+employee['firstName']+"\'", "\'"+employee['lastName']+"\'", "\'"+employee['email']+"\'", "\'"+employee['phone']+"\'"])+')' for employee in employees]
    values = ','.join(v)

    cur = db_connection.cursor()

    cur.execute("""
        INSERT INTO Employees (id,first_name, last_name, email, phone) VALUES
    """+ values + " ON CONFLICT ON CONSTRAINT employees_pkey DO NOTHING")
    db_connection.commit()
    # pandas.DataFrame(employees).to_json(path, orient='records', indent=1)

def load_user_data(db_connection):
    cur = db_connection.cursor()

    cur.execute("SELECT id, first_name, last_name, email, phone FROM Employees")

    rows = cur.fetchall()
    return [Employee(data[0], data[1] , data[2], data[3], data[4]) for data in rows]
    # employees_json = pandas.read_json(db_connection).to_dict('records')
    # return [Employee(employee['userId']) for employee in employees_json]


def map_lattice_to_okta(employees):
    return [OktaEmployee(employee) for employee in employees]

def send_data(user_data, path):
    pandas.DataFrame([data.export() for data in user_data]).to_json(path, orient='records', indent=1)

@dag(
    dag_id="0_Employee_Sync",
    description="DAG for employees - PoC",
    start_date=datetime(2022, 3, 1, 2),
    schedule_interval=None,
)
def employees():
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

    @task
    def update_external_users_data():
        external_users_data = fetch_external_users_data(users_endpoint_url)
        validate_data(external_users_data)
        users_data = map_external_to_lattice(external_users_data) 
        save_user_data(users_data, db_connection)

    @task
    def push_users_data_to_okta():
        users_data = load_user_data(db_connection)
        okta_users_data = map_lattice_to_okta(users_data)
        send_data(okta_users_data, okta_users_endpoint_url)

    db_setup() >> update_external_users_data() >> push_users_data_to_okta()

employees()