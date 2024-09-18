
CREATE TABLE IF NOT EXISTS transactions (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    transaction_code VARCHAR(50),
    credit NUMERIC(15, 2),
    notes TEXT,
    name VARCHAR(255),
    bank VARCHAR(50)
);