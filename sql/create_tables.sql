CREATE TABLE random_users (
    uuid VARCHAR(255) PRIMARY KEY,
    gender VARCHAR(10),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(150),
    age INT,
    dob_date TIMESTAMP,
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    username VARCHAR(100),
    password VARCHAR(255),
    fetch_time TIMESTAMP
);

CREATE TABLE partitions_info (
    fetch_time TIMESTAMP PRIMARY KEY,
    num_partitions INT,
    row_count INT
);
