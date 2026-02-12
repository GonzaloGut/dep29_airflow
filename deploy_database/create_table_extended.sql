CREATE TABLE IF NOT EXISTS users_extended (
    id INT PRIMARY KEY,
    name VARCHAR(255),
    username VARCHAR(255),
    email VARCHAR(255),
    street VARCHAR(255),
    city VARCHAR(255),
    zipcode VARCHAR(255),
    lat VARCHAR(50),
    lng VARCHAR(50),
    phone VARCHAR(255),
    website VARCHAR(255),
    company_name VARCHAR(255)
);
