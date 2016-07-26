using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using System.IO;
using Newtonsoft.Json;
using System.Text;

namespace Week6
{
    class Company
    {
        public string Country { get; set; }
        public string Title { get; set; }
        public DateTime AddedDate { get; set; }
    }

    class DatabaseHandler
    {
        private string path;
        private string connectionString;
        private SqliteConnection connection;
        private SqliteTransaction transaction;

        public DatabaseHandler(string path)
        {
            var file = File.Create(path);
            connectionString = "Data Source="+path;
            using (connection = new SqliteConnection(connectionString))
            {
                connection.Open();

                using (transaction = connection.BeginTransaction())
                {
                    ClearDatabase(); //idk why after deleting database file the database is persisted
                    CreateTable();
                    FillTable();
                    FindWithMaxID();
                    ChangeCitizenship();
                    DeleteNonAmericans();

                    PrintTotalCount();

                    transaction.Commit();

                    ReadEverything();
                }

                using (transaction = connection.BeginTransaction())
                {
                    try
                    {
                        while (true)
                        {
                            string jsonString = Console.ReadLine();
                            if (jsonString[0] == 'q')
                            {
                                break;
                            }
                            else
                            {
                                if (jsonString[0] != '{')
                                    throw new JsonReaderException();
                                else
                                {
                                    Company company = JsonConvert.DeserializeObject<Company>(jsonString);
                                    AddCompany(company);
                                }
                            }
                        }
                        transaction.Commit();
                    }
                    catch (JsonReaderException)
                    {
                        Console.WriteLine("Wrong JSON format");
                        transaction.Rollback();
                    }
                    catch (SqliteException)
                    {
                        Console.WriteLine("Bad SQL query");
                        transaction.Rollback();
                    }
                    catch (Exception)
                    {
                        Console.WriteLine("Something bad happened");
                        transaction.Rollback();
                    }
                    ReadEverything();
                }
            }
        }

        public void ClearDatabase()
        {
            var clearCommand = connection.CreateCommand();
            clearCommand.Transaction = transaction;
            clearCommand.CommandText = "DROP TABLE Companies";
            clearCommand.ExecuteNonQuery();
        }

        public void CreateTable()
        {
            var createCommand = connection.CreateCommand();
            createCommand.Transaction = transaction;
            createCommand.CommandText = @"CREATE TABLE Companies
                    (
                        Id INTEGER PRIMARY KEY,
                        Title varchar(30) NOT NULL,
                        Country vachar(15) NOT NULL,
                        AddedDate date NOT NULL
                    );";
            createCommand.ExecuteNonQuery();
        }

        public void AddCompany(Company company)
        {
            var insertCommand = connection.CreateCommand();
            insertCommand.Transaction = transaction;
            insertCommand.CommandText = @"INSERT INTO Companies (Title, Country, AddedDate) VALUES
                    (@title, @country, @date)";
            insertCommand.Parameters.AddWithValue("title", company.Title);
            insertCommand.Parameters.AddWithValue("country", company.Country);
            insertCommand.Parameters.AddWithValue("date", company.AddedDate);
            insertCommand.ExecuteNonQuery();
        }


        public void FillTable()
        {
            List<Company> companies = new List<Company> {
                new Company { Title = "NewtonIdeas", Country = "Ukraine", AddedDate = DateTime.Parse("2016-07-21") },
                new Company { Title = "EPAM", Country = "Ukraine", AddedDate = DateTime.Parse("2016-07-21") },
                new Company { Title = "Ciklum", Country = "Ukraine", AddedDate = DateTime.Parse("2016-07-22") },
                new Company { Title = "Softserve", Country = "Ukraine", AddedDate = DateTime.Parse("2016-07-22") },
                new Company { Title = "3Shape", Country = "Ukraine", AddedDate = DateTime.Parse("2016-07-22") },
                new Company { Title = "Yandex", Country = "Russia", AddedDate = DateTime.Parse("2016-07-22") },
                new Company { Title = "Mail.ru", Country = "Russia", AddedDate = DateTime.Parse("2016-07-23") },
                new Company { Title = "Google", Country = "USA", AddedDate = DateTime.Parse("2016-07-21") },
                new Company { Title = "Facebook", Country = "USA", AddedDate = DateTime.Parse("2016-07-23") },
                new Company { Title = "3Shape", Country = "USA", AddedDate = DateTime.Parse("2016-07-22") }
            };
            foreach (var company in companies)
            {
                AddCompany(company);
            }
        }

        public void FindWithMaxID()
        {
            var maxIdCommand = connection.CreateCommand();
            maxIdCommand.CommandText = "SELECT Id, MAX(Id), Title FROM Companies";
            var reader = maxIdCommand.ExecuteReader();
            reader.Read();
            Console.WriteLine("Company with max ID {0} is {1}", reader["Id"], reader["Title"]);
        }

        public void ChangeCitizenship()
        {
            var updateCommand = connection.CreateCommand();
            updateCommand.Transaction = transaction;
            updateCommand.CommandText = "UPDATE Companies SET Country=@newCountry WHERE Country=@oldCountry";
            updateCommand.Parameters.AddWithValue("newCountry", "USA");
            updateCommand.Parameters.AddWithValue("oldCountry", "Ukraine");
            updateCommand.ExecuteNonQuery();
        }

        public void DeleteNonAmericans()
        {
            var deleteNonAmericansCommand = connection.CreateCommand();
            deleteNonAmericansCommand.Transaction = transaction;
            deleteNonAmericansCommand.CommandText = "DELETE FROM Companies WHERE Country <> @surviverCountry";
            deleteNonAmericansCommand.Parameters.AddWithValue("surviverCountry", "USA");
            deleteNonAmericansCommand.ExecuteNonQuery();
        }

        public void PrintTotalCount()
        {
            var totalCountCommand = connection.CreateCommand();
            totalCountCommand.CommandText = "SELECT COUNT(Id) AS total FROM Companies";
            var reader = totalCountCommand.ExecuteReader();
            reader.Read();
            Console.WriteLine("There are {0} companies in the table", reader["total"]);
        }

        public void ReadEverything()
        {
            Console.WriteLine();
            var readeverythingCommand = connection.CreateCommand();
            readeverythingCommand.CommandText = "SELECT * FROM Companies";
            using (var reader = readeverythingCommand.ExecuteReader())
            {
                while (reader.Read())
                {
                    Console.WriteLine("{0} {1} {2} {3}", reader["Id"],reader["Title"],reader["Country"],reader["AddedDate"]);
                }
            }
            Console.WriteLine();
        }
    }

    public class Program
    {
        public static void Main(string[] args)
        {
            DatabaseHandler handler = new DatabaseHandler("companies.db");
        }


    }
}
