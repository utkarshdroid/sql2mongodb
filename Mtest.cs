using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data;
using MongoDB.Bson;
using MongoDB.Driver;
using System.IO;

namespace MongoMigration
{
    public class Name
    {
        public string FirstName { get; set; }
        public string LastName { get; set; }
    }
    class Mtest
    {
        public static SqlConnection connection;
        
        public Mtest()
        {

        }
        static void DataSync()
        {
            string connetionString = null;
            string sql = null;
            connetionString = "Data Source=10.68.24.3;Initial Catalog=SSPBSARDI;User ID=spineuser;Password=spineuser";
            sql = "Select * from roles";
            connection = new SqlConnection(connetionString);

            var connectionString = "mongodb://localhost:27017";
            var client = new MongoClient(connectionString);
            var db = client.GetDatabase("test");
            var col = db.GetCollection<BsonDocument>("roles");
            try
            {
                connection.Open();
                SqlDataAdapter adpt = new SqlDataAdapter(sql, connetionString);
                DataTable dt = new DataTable();
                adpt.Fill(dt);
                foreach (DataRow dr in dt.Rows)
                {
                    BsonDocument bson = new BsonDocument();
                    for (int i = 0; i < dr.ItemArray.Count(); i++)
                    {
                        bson.Add(dr.Table.Columns[i].ColumnName, dr[i].ToString());
                    }
                    col.InsertOne(bson);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Can not open connection ! " + ex);
            }
            finally
            {
                connection.Close();
            }

            Console.WriteLine("Success!!!");
        }

        public static void ConvertCSVtoDataTable(string strFilePath)
        {
            string[] name = strFilePath.Split('\\');
            string FileName = (name[name.Length - 1]).Substring(0, (name[name.Length - 1]).Length - 4);

            DataTable dt = new DataTable();
            using (StreamReader sr = new StreamReader(strFilePath))
            {
                string[] headers = sr.ReadLine().Split(',');
                foreach (string header in headers)
                {
                    dt.Columns.Add(header);
                }
                while (!sr.EndOfStream)
                {
                    string[] rows = sr.ReadLine().Split(',');
                    DataRow dr = dt.NewRow();
                    for (int i = 0; i < headers.Length; i++)
                    {
                        dr[i] = rows[i];
                    }
                    dt.Rows.Add(dr);
                }

            }
            SaveDataTableToCollection(dt, FileName);
        }

        public static void SaveDataTableToCollection(DataTable dt, string filename)
        {
            var connectionString = "mongodb://localhost:27017";
            var client = new MongoClient(connectionString);
            var db = client.GetDatabase("test");
            var col = db.GetCollection<BsonDocument>("users");
            DataTable tempdt = dt;
            if (filename == "users")
            {
                ConvertCSVtoDataTable(@"C:\Users\rajat.garg\Desktop\posts.csv");
            }

            List<BsonDocument> batch = new List<BsonDocument>();
            foreach (DataRow dr in dt.Rows)
            {

                var dictionary = dr.Table.Columns.Cast<DataColumn>().ToDictionary(col1 => col1.ColumnName, col1 => dr[col1.ColumnName]);
                batch.Add(new BsonDocument(dictionary));
            }

            col.InsertMany(batch.AsEnumerable());

            Console.WriteLine("Success!!!");
        }
        

        public static void Schema_DeNormalisation()
        {
            using (StreamWriter writer = new StreamWriter(@"C:\Users\RAJAT\Desktop\Mongo Migration\MongoSP.txt"))
            {
                writer.WriteLine("db.system.js.save({");
                // writer.WriteLine("test");

                //Console.WriteLine("success");

                //foreach (string line in File.ReadLines(@"C:\Users\RAJAT\Desktop\Mongo Migration\SP.txt"))
                //{
                //if (line.StartsWith("ALTER PROCEDURE", StringComparison.OrdinalIgnoreCase) || line.StartsWith("create PROCEDURE", StringComparison.OrdinalIgnoreCase))
                //{
                //    string ProcName = line.Split(' ')[2].Split('(')[0];

                //}
                //}


                // This text is added only once to the file.
                //if (!File.Exists(path))
                //{
                //    // Create a file to write to.
                //    //string createText = "Hello and Welcome" + Environment.NewLine;
                //    //File.WriteAllText(path, createText);
                //    Console.WriteLine("No such file");
                //    return;
                //}

                

            }
        }

        public static void CSVToJSON()
        {
            StringBuilder csvContent = new StringBuilder();

            // Adding Header Or Column in the First Row of CSV
            csvContent.AppendLine("Lajapathy,Arun");
            csvContent.AppendLine("Anand,Babu");
            csvContent.AppendLine("Sathiya,Seelan");

            string textPath = @"D:\CSVTextFile.txt";

            //Here we delete the exisitng file to avoid duplicate records.
            if (File.Exists(textPath))
            {
                File.Delete(textPath);
            }

            // Save or upload CSV format string to Text File (.txt)
            File.AppendAllText(textPath, csvContent.ToString());

            //Download or read all Text within the Text file.
            string csvContentStr = File.ReadAllText(textPath);

            var csvpath = @"D:\CSVFileName.csv";
            //Here we delete the exisitng file to avoid duplicate records.
            if (File.Exists(csvpath))
            {
                File.Delete(csvpath);
            }

            //This saves content as CSV File.
            File.WriteAllText(csvpath, csvContentStr);

            var nameList = new List<Name>();

            //getting full file path of file  
            string CSVFilePath = csvpath;
            //Reading All text  
            string ReadCSV = File.ReadAllText(CSVFilePath);

            Console.WriteLine("CSV Content:");
            Console.WriteLine(ReadCSV);

            //spliting row after new line  
            foreach (string csvRow in ReadCSV.Split('\n'))
            {
                if (!string.IsNullOrEmpty(csvRow))
                {
                    var fileRec = csvRow.Split(',');
                    nameList.Add(
                        new Name { FirstName = fileRec[0], LastName = fileRec[1].Replace("\r", "") }
                        );
                }
            }
            //string json = Newtonsoft.Json.JsonConvert.SerializeObject(nameList);
            Console.WriteLine("JSON String");
            //Console.WriteLine(json);
        }


        static void Main(string[] args)
        {
            try
            {
                // DataSync();
                //ConvertCSVtoDataTable(@"C:\Users\rajat.garg\Desktop\users.csv");
                //Schema_DeNormalisation();
                //CSVToJSON();

                string path = @"C:\Users\RAJAT\Desktop\Mongo Migration\SP.txt";
                string output = @"C:\Users\RAJAT\Desktop\Mongo Migration\SP-output.txt";



                Migrator migrator = new Migrator();

                // Open the file to read from.
                string storedProc = File.ReadAllText(path).Trim();

                string procName = migrator.getProcName(storedProc);
                List<Param> paramsList = migrator.getParams(storedProc);
                var blocks = migrator.getBlocks(storedProc);


                //Console.WriteLine(procName);
                //foreach(Param p in paramsList)
                //{
                //    Console.WriteLine(p.Name + " " + p.DataType + " " + p.Type);
                //}
                foreach (var block in blocks)
                {
                    Console.WriteLine(block);
                }

                string fx_output = migrator.combiner(migrator.segreggator(blocks), paramsList);
                string storedJsOutput = migrator.storedJS(procName, paramsList, fx_output);

                Console.WriteLine(storedJsOutput);

                File.WriteAllText(output, storedJsOutput);

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }

    }
}
