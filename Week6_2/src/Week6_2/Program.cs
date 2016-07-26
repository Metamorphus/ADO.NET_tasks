using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;

namespace Week6._2
{
    public class Program
    {
        public static void Main(string[] args)
        {
            QueryHandler handler = new QueryHandler("northwind.db");
            handler.PerformAllWork();
        }

        class QueryHandler
        {
            private string path;
            private SqliteConnection connection;

            public QueryHandler(string path)
            {
                this.path = path;
            }

            public void PerformAllWork()
            {
                using (connection = new SqliteConnection("Data Source="+path))
                {
                    connection.Open();

                    GetCustomersFromD();
                    ConvertNamesToUpper();
                    GetDistinctCountries();
                    GetLondonSalesmen();
                    GetOrdersWithTofu();
                    //GetGermanProducts();
                    //GetIkuraBuyers();
                    GetEmployeesLeft();
                    GetEmployeesInner();
                    GetAllPhones();
                    GetCustomersCountByCity();
                    GetActiveCustomers();
                    GetCorrectPhones();
                    GetMostActiveCustomer();
                    GetFamiaFollowers();
                }
            }

            private void GetCustomersFromD()
            {
                Console.WriteLine("*************CUSTOMERS WITH NAME STARTING WITH D*************");
                var command = connection.CreateCommand();
                command.CommandText = "SELECT ContactName FROM Customers WHERE ContactName LIKE 'D%'";
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        Console.WriteLine(reader["ContactName"]);
                    }
                }
                Console.WriteLine();
            }

            private void ConvertNamesToUpper()
            {
                using (var transaction = connection.BeginTransaction())
                {
                    var command = connection.CreateCommand();
                    command.CommandText = "UPDATE Customers SET ContactName = UPPER(ContactName)";
                    command.Transaction = transaction;
                    command.ExecuteNonQuery();

                    transaction.Commit();
                }
            }

            private void GetDistinctCountries()
            {
                Console.WriteLine("*************DISTINCT COUNTRIES*************");
                var command = connection.CreateCommand();
                command.CommandText = "SELECT DISTINCT Country FROM Customers";
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        Console.WriteLine(reader["Country"]);
                    }
                }
                Console.WriteLine();
            }

            private void GetLondonSalesmen()
            {
                Console.WriteLine("*************LONDON SALESMEN*************");
                var command = connection.CreateCommand();
                command.CommandText = @"SELECT ContactName FROM Customers WHERE City='London' 
                    AND (ContactTitle LIKE 'Sales%')";
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        Console.WriteLine(reader["ContactName"]);
                    }
                }
                Console.WriteLine();
            }

            private void GetOrdersWithTofu()
            {
                Console.WriteLine("*************ORDERS WITH TOFU*************");
                var command = connection.CreateCommand();
                command.CommandText = @"SELECT OrderID 
                    FROM [Order Details] INNER JOIN Products ON [Order Details].ProductID = Products.ProductID
                    WHERE Products.ProductName = 'Tofu'";
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        Console.WriteLine(reader["OrderID"]);
                    }
                }
                Console.WriteLine();
            }

            private void GetGermanProducts()
            {
                Console.WriteLine("*************GERMAN PRODUCTS*************");
                var command = connection.CreateCommand();
                command.CommandText = @"SELECT ProductName
                    FROM Orders 
                        INNER JOIN [Order Details] ON Orders.OrderID = [Order Details].OrderID
                        INNER JOIN Products ON [Order Details].ProductID = Products.ProductID
                    WHERE ShipCountry='Germany'";
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        Console.WriteLine(reader["ProductName"]);
                    }
                }
                Console.WriteLine();
            }

            private void GetIkuraBuyers()
            {
                Console.WriteLine("*************IKURA BYUERS*************");
                var command = connection.CreateCommand();
                command.CommandText = @"SELECT ContactName
                    FROM Orders INNER JOIN Customers ON Orders.CustomerID = Customers.CustomerID
                    WHERE OrderID IN (SELECT OrderID 
                        FROM [Order Details] INNER JOIN Products ON [Order Details].ProductID = Products.ProductID
                        WHERE Products.ProductName = 'Ikura')";
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        Console.WriteLine(reader["ContactName"]);
                    }
                }
                Console.WriteLine();
            }

            private void GetEmployeesLeft()
            {
                Console.WriteLine("*************EMPLOYEES LEFT JOIN*************");
                var command = connection.CreateCommand();
                command.CommandText = @"SELECT Employees.EmployeeID, OrderID
                    FROM Employees LEFT JOIN Orders ON Employees.EmployeeID = Orders.EmployeeID";
                uint resultsCount = 0;
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        ++resultsCount;
                    }
                }
                Console.WriteLine("total: {0}", resultsCount);
                Console.WriteLine();
            }

            private void GetEmployeesInner()
            {
                Console.WriteLine("*************EMPLOYEES INNER JOIN*************");
                var command = connection.CreateCommand();
                command.CommandText = @"SELECT Employees.EmployeeID, OrderID
                    FROM Employees INNER JOIN Orders ON Employees.EmployeeID = Orders.EmployeeID";
                uint resultsCount = 0;
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        ++resultsCount;
                    }
                }
                Console.WriteLine("total: {0}", resultsCount);
                Console.WriteLine();
            }

            private void GetAllPhones()
            {
                Console.WriteLine("*************ALL PHONES*************");
                var command = connection.CreateCommand();
                command.CommandText = @"SELECT Phone FROM Shippers UNION ALL
                    SELECT Phone From Suppliers";
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        Console.WriteLine(reader["Phone"]);
                    }
                }
                Console.WriteLine();
            }

            private void GetCustomersCountByCity()
            {
                Console.WriteLine("*************Coustomers count by city*************");
                var command = connection.CreateCommand();
                command.CommandText = @"SELECT City, COUNT(CustomerID) FROM Customers GROUP BY City";
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        Console.WriteLine(reader[0] + " " + reader[1]);
                    }
                }
                Console.WriteLine();
            }

            private void GetActiveCustomers()
            {
                Console.WriteLine("*************ACTIVE CUSTOMERS*************");
                var command = connection.CreateCommand();
                command.CommandText = @"SELECT CustomerID, COUNT(Orders.OrderID) AS cnt, AVG(UnitPrice) AS mean
                    FROM Orders INNER JOIN [Order Details] ON Orders.OrderID = [Order Details].OrderID
                    GROUP BY CustomerID
                    HAVING cnt > 9 AND mean < 17";
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        Console.WriteLine("{0} {1} {2}", reader["CustomerID"], reader["cnt"], reader["mean"]);
                    }
                }
                Console.WriteLine();
            }

            private void GetCorrectPhones()
            {
                Console.WriteLine("*************CORRECT PHONES*************");
                var command = connection.CreateCommand();
                command.CommandText = @"SELECT CustomerID
                    FROM Customers
                    WHERE Phone LIKE '____-____'";
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        Console.WriteLine(reader["CustomerID"]);
                    }
                }
                Console.WriteLine();
            }

            private void GetMostActiveCustomer()
            {
                Console.WriteLine("*************MOST ACTIVE CUSTOMER*************");
                var command = connection.CreateCommand();
                command.CommandText = @"SELECT CustomerID
                    FROM Orders
                    GROUP BY CustomerID
                    ORDER BY COUNT(OrderID) DESC LIMIT 1";
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        Console.WriteLine(reader["CustomerID"]);
                    }
                }
                Console.WriteLine();
            }

            private void GetFamiaFollowers()
            {
                Console.WriteLine("*************FAMIA FOLLOWERS*************");
                var command = connection.CreateCommand();
                command.CommandText = @"SELECT DISTINCT ProductID
                    FROM Orders INNER JOIN [Order Details] ON Orders.OrderID = [Order Details].OrderID
                    WHERE CustomerID = 'FAMIA'";
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        Console.WriteLine(reader["ProductID"]);
                    }
                }
                Console.WriteLine();
            }
        }
    }
}
