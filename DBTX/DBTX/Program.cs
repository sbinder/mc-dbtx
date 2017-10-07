using MySql.Data.MySqlClient;
using System;
using System.Data;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DBTX
{
    class Program
    {
        static void Main(string[] args)
        {
            var MSconnectionString = @"Data Source=s11.winhost.com;Initial Catalog=DB_77982_tutor;Persist Security Info=True;User ID=DB_77982_tutor_user;Password=SPxLv4J;MultipleActiveResultSets=True";
            var MYconnectionString = @"server=localhost;user id=mcuser;password=xT87$nXIaZf0;persistsecurityinfo=True;database=mc";
            var srcConnection = new SqlConnection(MSconnectionString);
            var destConnection = new MySqlConnection(MYconnectionString);
            srcConnection.Open();
            destConnection.Open();
            TransferStudents(destConnection, srcConnection);

            Console.Write("Press any key");
            Console.ReadKey();
        }

        static bool TransferStudents(MySqlConnection dest, SqlConnection src)
        {

            var pdict = new Dictionary<uint, uint>();
            Console.WriteLine("Students:");
            var scmd = src.CreateCommand();
            var pcmd = src.CreateCommand();
            var Dpcmd = dest.CreateCommand();

            scmd.CommandText = "select * from dbo.student where Target > '2017-09-01' order by Target";
            pcmd.CommandText = "select * from dbo.parent where pid = @pid";
            pcmd.Parameters.Add(new SqlParameter("@pid", 0));

            Dpcmd.CommandText = "insert into parent (org, title1, lname1, fname1, email1, " +
                "title2, lname2, fname2, email2, address1, address2, city, state, zip, comment) " +
                "values (1, @title1, @lname1, @fname1, @email1, @title2, @lname2, @fname2, " +
                "@email2, @address1, @address2, @city, @state, @zip, @comment)" +
                "; select last_insert_id()";
            Dpcmd.Parameters.Add(new MySqlParameter("@title1", SqlDbType.VarChar));
            Dpcmd.Parameters.Add(new MySqlParameter("@lname1", SqlDbType.VarChar));
            Dpcmd.Parameters.Add(new MySqlParameter("@fname1", SqlDbType.VarChar));
            Dpcmd.Parameters.Add(new MySqlParameter("@email1", SqlDbType.VarChar));
            Dpcmd.Parameters.Add(new MySqlParameter("@title2", SqlDbType.VarChar));
            Dpcmd.Parameters.Add(new MySqlParameter("@lname2", SqlDbType.VarChar));
            Dpcmd.Parameters.Add(new MySqlParameter("@fname2", SqlDbType.VarChar));
            Dpcmd.Parameters.Add(new MySqlParameter("@email2", SqlDbType.VarChar));
            Dpcmd.Parameters.Add(new MySqlParameter("@address1", SqlDbType.VarChar));
            Dpcmd.Parameters.Add(new MySqlParameter("@address2", SqlDbType.VarChar));
            Dpcmd.Parameters.Add(new MySqlParameter("@city", SqlDbType.VarChar));
            Dpcmd.Parameters.Add(new MySqlParameter("@state", SqlDbType.VarChar));
            Dpcmd.Parameters.Add(new MySqlParameter("@zip", SqlDbType.VarChar));
            Dpcmd.Parameters.Add(new MySqlParameter("@comment", SqlDbType.VarChar));

            var Dscmd = dest.CreateCommand();
            Dscmd.CommandText = "insert into student (org, target, lname, fname, parent, teacher, " +
                "email, note, username, password, expires, male, trial, liturgy) " +
                "values(1, @target, @lname, @fname, @parent, @teacher, " +
                "@email, @note, @username, @password, @expires, @male, @trial, @liturgy); select last_insert_id()";
            Dscmd.Parameters.Add(new MySqlParameter("@target", SqlDbType.DateTime));
            Dscmd.Parameters.Add(new MySqlParameter("@lname", SqlDbType.VarChar));
            Dscmd.Parameters.Add(new MySqlParameter("@fname", SqlDbType.VarChar));
            Dscmd.Parameters.Add(new MySqlParameter("@parent", SqlDbType.Int));
            Dscmd.Parameters.Add(new MySqlParameter("@teacher", SqlDbType.Int));
            Dscmd.Parameters.Add(new MySqlParameter("@email", SqlDbType.VarChar));
            Dscmd.Parameters.Add(new MySqlParameter("@note", SqlDbType.VarChar));
            Dscmd.Parameters.Add(new MySqlParameter("@username", SqlDbType.VarChar));
            Dscmd.Parameters.Add(new MySqlParameter("@password", SqlDbType.VarChar));
            Dscmd.Parameters.Add(new MySqlParameter("@expires", SqlDbType.DateTime)); // new DateTime(2099, 1, 1)));
            Dscmd.Parameters.Add(new MySqlParameter("@male", SqlDbType.Bit));
            Dscmd.Parameters.Add(new MySqlParameter("@trial", SqlDbType.Bit));
            Dscmd.Parameters.Add(new MySqlParameter("@liturgy", SqlDbType.Int));

            using (var qreader = scmd.ExecuteReader())
            {
                while (qreader.Read())
                {
                    Console.WriteLine(qreader["Target"].ToString() + " " + 
                        qreader["LName"] + ", " + qreader["FName"]);

                    // Parent Record
                    uint npid;
                    uint nstid;
                    var pid = uint.Parse(qreader["Parent"].ToString());
                    if (pdict.TryGetValue(pid, out npid))
                    {
                        Console.WriteLine("   PARENT EXISTS. PID:" + npid);
                        continue;
                    } else
                    {
                        // pcmd.Parameters.Clear();
                        pcmd.Parameters["@pid"].Value = pid;    // .AddWithValue("@pid", pid);
                        using (var preader = pcmd.ExecuteReader())
                        {
                            preader.Read();
                            Console.WriteLine("=> Found parent record: " + preader["Lname1"] + ", " + preader["Fname1"] );

                            Dpcmd.Parameters.Clear();

                            Dpcmd.Parameters.AddWithValue("@title1", preader["Title1"]);
                            Dpcmd.Parameters.AddWithValue("@lname1", preader["Lname1"]);
                            Dpcmd.Parameters.AddWithValue("@fname1", preader["Fname1"]);
                            Dpcmd.Parameters.AddWithValue("@email1", preader["Email1"]);
                            Dpcmd.Parameters.AddWithValue("@title2", preader["Title2"]);
                            Dpcmd.Parameters.AddWithValue("@fname2", preader["Fname2"]);
                            Dpcmd.Parameters.AddWithValue("@lname2", preader["LName2"]);
                            Dpcmd.Parameters.AddWithValue("@email2", preader["Email2"]);
                            Dpcmd.Parameters.AddWithValue("@address1", preader["Address1"]);
                            Dpcmd.Parameters.AddWithValue("@address2", preader["Address2"]);
                            Dpcmd.Parameters.AddWithValue("@city", preader["City"]);
                            Dpcmd.Parameters.AddWithValue("@state", preader["State"]);
                            Dpcmd.Parameters.AddWithValue("@zip", preader["Zip"]);
                            Dpcmd.Parameters.AddWithValue("@comment", preader["Comment"]);

                            //Dpcmd.Parameters["@title1"].Value = preader["Title1"];
                            //Dpcmd.Parameters["@lname1"].Value = preader["Lname1"];
                            //Dpcmd.Parameters["@fname1"].Value = preader["Fname1"];
                            //Dpcmd.Parameters["@email1"].Value = preader["Email1"];
                            //Dpcmd.Parameters["@title2"].Value = preader["Title2"];
                            //Dpcmd.Parameters["@fname2"].Value = preader["Fname2"];
                            //Dpcmd.Parameters["@lname2"].Value = preader["LName2"];
                            //Dpcmd.Parameters["@email2"].Value = preader["Email2"];
                            //Dpcmd.Parameters["@address1"].Value = preader["Address1"];
                            //Dpcmd.Parameters["@address2"].Value = preader["Address2"];
                            //Dpcmd.Parameters["@city"].Value = preader["City"];
                            //Dpcmd.Parameters["@state"].Value = preader["State"];
                            //Dpcmd.Parameters["@zip"].Value = preader["Zip"];
                            //Dpcmd.Parameters["@comment"].Value = preader["Comment"];

                            // INSERT PARENT RECORD
                            npid = uint.Parse(Dpcmd.ExecuteScalar().ToString());

                            pdict.Add(pid, npid); // this should be new PID
                            Console.WriteLine("    Added parent " + npid);
                        }
                        // now npid holds parent

                        Dscmd.Parameters.Clear();

                        Dscmd.Parameters.AddWithValue("@target", qreader["Target"]);
                        Dscmd.Parameters.AddWithValue("@lname", qreader["Lname"]);
                        Dscmd.Parameters.AddWithValue("@fname", qreader["Fname"]);
                        Dscmd.Parameters.AddWithValue("@parent", npid);
                        Dscmd.Parameters.AddWithValue("@teacher", 1);
                        Dscmd.Parameters.AddWithValue("@email", qreader["Email"]);
                        Dscmd.Parameters.AddWithValue("@username", qreader["Username"]);
                        Dscmd.Parameters.AddWithValue("@password", qreader["Password"]);
                        Dscmd.Parameters.AddWithValue("@male", qreader["Gender"].ToString().Trim() == "M");
                        Dscmd.Parameters.AddWithValue("@trial", false);
                        Dscmd.Parameters.AddWithValue("@liturgy", 0);
                        Dscmd.Parameters.AddWithValue("@expires", new DateTime(2099, 1, 1));
                        Dscmd.Parameters.AddWithValue("@note", string.Empty);


                        //Dscmd.Parameters["@target"].Value = qreader["Target"];
                        //Dscmd.Parameters["@lname"].Value = qreader["Lname"];
                        //Dscmd.Parameters["@fname"].Value = qreader["Fname"];
                        //Dscmd.Parameters["@parent"].Value = npid;
                        //Dscmd.Parameters["@teacher"].Value = 1;
                        //Dscmd.Parameters["@email"].Value = qreader["Email"];
                        //Dscmd.Parameters["@username"].Value = qreader["Username"];
                        //Dscmd.Parameters["@password"].Value = qreader["Password"];
                        //Dscmd.Parameters["@male"].Value = (char)qreader["Gender"] == 'M';
                        //Dscmd.Parameters["@trial"].Value = false;
                        //Dscmd.Parameters["@liturgy"].Value = 0;

                        nstid = uint.Parse(Dscmd.ExecuteScalar().ToString());

                        Console.WriteLine("   Added student " + nstid);
                    }
                }
                return true;

            }
        }
    }
}
