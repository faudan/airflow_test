from datetime import datetime, timedelta
from airflow.decorators import dag, task
import pandas as pandas

def Employee(id):
    return {"userId": id}
   
class OktaEmployee:
    def __init__(self, employee):
        self.employee = employee

    def export(self):
        return {'userId': self.employee['userId'], 'service': 'okta'}    

def now():
    return datetime.now().strftime(r'%Y%m%d%H%M%S')


# internal transformation function
def year_of_birth(date_of_birth):
    return datetime.strptime(
        date_of_birth, '%Y-%m-%d'
    ).year


# validation hook function
def local_employees_year_of_birth(local_employees):
    local_employees['YearOfBirth'] = local_employees['DateOfBirth'] \
        .apply(year_of_birth)
    return local_employees[local_employees['YearOfBirth'] < 1950]


def read_external_user_data(path):
    local_employees = pandas.read_csv(path)
    return local_employees.to_dict('records')

def data_validation(employees):
    return employees

def external_to_lattice(employees):
    return [Employee(employee['UserId']) for employee in employees]

def save_user_data(employees, path):
    pandas.DataFrame(employees).to_json(path, orient='records', indent=1)

def load_user_data(json_path):
    employees_json = pandas.read_json(json_path).to_dict('records')
    return [Employee(employee['userId']) for employee in employees_json]


def lattice_to_okta(employees):
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
    json_path = f'./dags/data/stored_employees.json'
    file_path = r"./dags/input/employees.csv"
    okta_output_path = r"./dags/output/okta_user_data.json"

    @task
    def fetch_external_user_data():
        user_data = read_external_user_data(file_path)
        validated_data = data_validation(user_data)
        employees = external_to_lattice(validated_data) 
        save_user_data(employees, json_path)

    @task
    def send_to_okta():
        employees = load_user_data(json_path)
        okta_user_data = lattice_to_okta(employees)
        send_data(okta_user_data, okta_output_path)

    fetch_external_user_data() \
    >> send_to_okta()

employees()