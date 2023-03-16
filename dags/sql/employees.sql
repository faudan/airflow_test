CREATE TABLE IF NOT EXISTS Employees (
    id         TEXT         PRIMARY KEY,
    first_name VARCHAR(255)   NOT NULL,
    last_name  VARCHAR(255)   NOT NULL,
    email  VARCHAR(255)   NOT NULL,
    phone  VARCHAR(255)   NOT NULL
);