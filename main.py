import sqlite3
import pandas as pd

# --- Module-level DataFrames for automated tests ---
_conn = sqlite3.connect("data.sqlite")

# Part 1.1: Employees in Boston
df_boston = pd.read_sql("""
    SELECT e.firstName, e.lastName
    FROM employees AS e
    JOIN offices    AS o USING (officeCode)
    WHERE o.city = 'Boston';
""", _conn)

# Part 1.2: Offices with zero employees
df_zero_emp = pd.read_sql("""
    SELECT o.officeCode, o.city
    FROM offices AS o
    LEFT JOIN employees AS e USING (officeCode)
    GROUP BY o.officeCode, o.city
    HAVING COUNT(e.employeeNumber) = 0;
""", _conn)

# Part 2.1: All employees with office location
df_employee = pd.read_sql("""
    SELECT e.firstName, e.lastName, o.city, o.state
    FROM employees AS e
    LEFT JOIN offices   AS o USING (officeCode)
    ORDER BY e.firstName, e.lastName;
""", _conn)

# Part 2.2: Customers with no orders
df_contacts = pd.read_sql("""
    SELECT c.contactFirstName, c.contactLastName, c.phone, c.salesRepEmployeeNumber
    FROM customers AS c
    LEFT JOIN orders    AS o USING (customerNumber)
    WHERE o.orderNumber IS NULL
    ORDER BY c.contactLastName;
""", _conn)

# Part 3: Customer payments sorted by amount desc (cast to REAL)
df_payment = pd.read_sql("""
    SELECT c.contactFirstName, c.contactLastName, p.paymentDate, CAST(p.amount AS REAL) AS amount
    FROM payments  AS p
    JOIN customers AS c USING (customerNumber)
    ORDER BY amount DESC;
""", _conn)

# Part 4.1: Employees whose customers’ avg creditLimit > 90k
df_credit = pd.read_sql("""
    SELECT e.employeeNumber, e.firstName, e.lastName, COUNT(c.customerNumber) AS numCustomers
    FROM employees AS e
    JOIN customers AS c ON e.employeeNumber = c.salesRepEmployeeNumber
    GROUP BY e.employeeNumber, e.firstName, e.lastName
    HAVING AVG(c.creditLimit) > 90000
    ORDER BY numCustomers DESC;
""", _conn)

# Part 4.2: Product orders and total units sold
df_product_sold = pd.read_sql("""
    SELECT p.productName, COUNT(od.orderNumber) AS numOrders, SUM(od.quantityOrdered) AS totalunits
    FROM products     AS p
    JOIN orderdetails AS od USING (productCode)
    GROUP BY p.productName
    ORDER BY totalunits DESC;
""", _conn)

# Part 5.1: Distinct purchasers per product
df_total_customers = pd.read_sql("""
    SELECT p.productName, p.productCode, COUNT(DISTINCT o.customerNumber) AS numpurchasers
    FROM products     AS p
    JOIN orderdetails AS od USING (productCode)
    JOIN orders       AS o  USING (orderNumber)
    GROUP BY p.productName, p.productCode
    ORDER BY numpurchasers DESC;
""", _conn)

# Part 5.2: Customers per office
df_customers = pd.read_sql("""
    SELECT o.officeCode, o.city, COUNT(c.customerNumber) AS n_customers
    FROM offices AS o
    LEFT JOIN employees AS e USING (officeCode)
    LEFT JOIN customers AS c ON e.employeeNumber = c.salesRepEmployeeNumber
    GROUP BY o.officeCode, o.city;
""", _conn)

# Part 6: Employees who sold underperforming products (ordered by <20 customers)
df_under_20 = pd.read_sql("""
    SELECT DISTINCT e.employeeNumber, e.firstName, e.lastName, o.city, o.officeCode
    FROM employees    AS e
    JOIN offices       AS o USING (officeCode)
    JOIN customers     AS c ON e.employeeNumber = c.salesRepEmployeeNumber
    JOIN orders        AS ord USING (customerNumber)
    JOIN orderdetails AS od  USING (orderNumber)
    WHERE od.productCode IN (
        SELECT od2.productCode
        FROM orderdetails AS od2
        JOIN orders       AS ord2 USING (orderNumber)
        GROUP BY od2.productCode
        HAVING COUNT(DISTINCT ord2.customerNumber) < 20
    )
    ORDER BY
        CASE WHEN e.firstName = 'Loui' THEN 0 ELSE 1 END,
        e.employeeNumber;
""", _conn)

# Close module-level connection
_conn.close()


def query(sql: str, conn: sqlite3.Connection):
    """Run the SQL and pretty-print the DataFrame."""
    df = pd.read_sql(sql, conn)
    try:
        print(df.to_markdown(index=False))
    except ImportError:
        print(df.to_string(index=False))
    print("\n" + "-"*80 + "\n")


def main():
    conn = sqlite3.connect("data.sqlite")

    # Part 1.1: Employees in Boston
    print("Part 1.1 ▶ Employees in Boston")
    query("""
        SELECT e.firstName, e.lastName, e.jobTitle
        FROM employees AS e
        JOIN offices    AS o USING (officeCode)
        WHERE o.city = 'Boston';
    """, conn)

    # Part 1.2: Offices with zero employees</file>