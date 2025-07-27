import sqlite3
import pandas as pd

def query(sql: str, conn: sqlite3.Connection):
    """Run the SQL and pretty-print the DataFrame."""
    df = pd.read_sql(sql, conn)
    print(df.to_markdown(index=False))
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

    # Part 1.2: Offices with zero employees
    print("Part 1.2 ▶ Offices with zero employees")
    query("""
        SELECT o.officeCode, o.city
        FROM offices AS o
        LEFT JOIN employees AS e USING (officeCode)
        GROUP BY o.officeCode, o.city
        HAVING COUNT(e.employeeNumber) = 0;
    """, conn)

    # Part 2.1: All employees + their office city/state
    print("Part 2.1 ▶ All employees with office location")
    query("""
        SELECT e.firstName, e.lastName, o.city, o.state
        FROM employees AS e
        LEFT JOIN offices   AS o USING (officeCode)
        ORDER BY e.firstName, e.lastName;
    """, conn)

    # Part 2.2: Customers who have not placed any orders
    print("Part 2.2 ▶ Customers with no orders")
    query("""
        SELECT c.contactFirstName, c.contactLastName, c.phone, c.salesRepEmployeeNumber
        FROM customers AS c
        LEFT JOIN orders    AS o USING (customerNumber)
        WHERE o.orderNumber IS NULL
        ORDER BY c.contactLastName;
    """, conn)

    # Part 3: Payments sorted by amount desc (cast to REAL)
    print("Part 3 ▶ Customer payments sorted by amount")
    query("""
        SELECT c.contactFirstName,
               c.contactLastName,
               p.paymentDate,
               p.checkNumber,
               CAST(p.amount AS REAL) AS amount
        FROM payments  AS p
        JOIN customers AS c USING (customerNumber)
        ORDER BY amount DESC;
    """, conn)

    # Part 4.1: Employees whose customers’ avg creditLimit > 90k
    print("Part 4.1 ▶ Top employees by avg customer credit limit")
    query("""
        SELECT e.employeeNumber,
               e.firstName,
               e.lastName,
               COUNT(c.customerNumber) AS numCustomers
        FROM employees AS e
        JOIN customers AS c
          ON e.employeeNumber = c.salesRepEmployeeNumber
        GROUP BY e.employeeNumber, e.firstName, e.lastName
        HAVING AVG(c.creditLimit) > 90000
        ORDER BY numCustomers DESC;
    """, conn)

    # Part 4.2: Products → numorders & totalunits sold
    print("Part 4.2 ▶ Product orders and total units sold")
    query("""
        SELECT p.productName,
               COUNT(od.orderNumber)       AS numOrders,
               SUM(od.quantityOrdered)     AS totalUnits
        FROM products    AS p
        JOIN orderdetails AS od USING (productCode)
        GROUP BY p.productName
        ORDER BY totalUnits DESC;
    """, conn)

    # Part 5.1: How many distinct customers ordered each product?
    print("Part 5.1 ▶ Distinct purchasers per product")
    query("""
        SELECT p.productName,
               p.productCode,
               COUNT(DISTINCT o.customerNumber) AS numPurchasers
        FROM products    AS p
        JOIN orderdetails AS od USING (productCode)
        JOIN orders       AS o  USING (orderNumber)
        GROUP BY p.productName, p.productCode
        ORDER BY numPurchasers DESC;
    """, conn)

    # Part 5.2: Number of customers per office
    print("Part 5.2 ▶ Customers per office")
    query("""
        SELECT o.officeCode,
               o.city,
               COUNT(c.customerNumber) AS n_customers
        FROM offices   AS o
        LEFT JOIN employees AS e USING (officeCode)
        LEFT JOIN customers AS c
          ON e.employeeNumber = c.salesRepEmployeeNumber
        GROUP BY o.officeCode, o.city;
    """, conn)

    # Part 6: Employees who sold products ordered by <20 customers
    print("Part 6 ▶ Employees who sold underperforming products")
    query("""
        SELECT DISTINCT
               e.employeeNumber,
               e.firstName,
               e.lastName,
               o.city,
               o.officeCode
        FROM employees    AS e
        JOIN offices       AS o  USING (officeCode)
        JOIN customers     AS c
          ON e.employeeNumber = c.salesRepEmployeeNumber
        JOIN orders        AS ord USING (customerNumber)
        JOIN orderdetails AS od  USING (orderNumber)
        WHERE od.productCode IN (
            SELECT od2.productCode
            FROM orderdetails AS od2
            JOIN orders       AS ord2 USING (orderNumber)
            GROUP BY od2.productCode
            HAVING COUNT(DISTINCT ord2.customerNumber) < 20
        );
    """, conn)

    conn.close()

if __name__ == "__main__":
    main()