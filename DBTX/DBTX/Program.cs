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
            //TransferProgress(destConnection, srcConnection);

            Console.Write("Press any key");
            Console.ReadKey();
        }

        static bool TransferProgress(MySqlConnection dest, SqlConnection src, uint deststid, uint srcstid)
        {

            Dictionary<int, int> prayermap = new Dictionary<int, int>
            {
                { 3, 11 },
                { 4, 10 },
                { 5, 20},
                {6, 21},
                {7, 22},
                {8, 23},
                {11, 25},
                {14, 26},
                {15, 27},
                {16, 28},
                {17, 4},
                {18, 3},
                {19, 2},
                {20, 29},
                {22, 30},
                {23, 1},
                {24, 7},
                {25, 9},
                {26, 12},
                {27, 24},
                {28, 6},
                {29, 8}
            };
            //var scmd = src.CreateCommand();
            var icmd = src.CreateCommand();
            var ocmd = dest.CreateCommand();
            //scmd.CommandText = "select stid from student";
            icmd.CommandText = "select top 1 * from dbo.lessontask where STID = @stid and TaskID = @taskid order by date desc";
            ocmd.CommandText = "insert into progress (tid, stid, taskid, date, rating, tcomment, scomment) " +
                "values (1, @stid, @taskid, @date, @rating, @tcomment, @scomment)";

            ocmd.Parameters.AddWithValue("@stid", deststid);
            ocmd.Parameters.Add("@taskid", MySqlDbType.UInt32);
            ocmd.Parameters.Add("@date", MySqlDbType.Date);
            ocmd.Parameters.Add("@rating", MySqlDbType.Int16);
            ocmd.Parameters.Add("@tcomment", MySqlDbType.VarChar);
            ocmd.Parameters.Add("@scomment", MySqlDbType.VarChar);

            //icmd.Parameters.Add("@stid", SqlDbType.Int);
            icmd.Parameters.AddWithValue("@stid", (int)srcstid);
            icmd.Parameters.Add("@taskid", SqlDbType.Int);

            foreach (KeyValuePair<int, int> entry in prayermap)
            {
                icmd.Parameters["@taskid"].Value = entry.Key;
                using (var ireader = icmd.ExecuteReader())
                {
                    if (ireader.Read())
                    {

                        //var r = ireader.GetInt32(ireader.GetOrdinal("rating"));
                        var r = ireader["rating"].ToString();
                        int rat;
                        if (!int.TryParse(r, out rat)) rat = 0;
                        
                            rat = 10 * rat;
                        
                        ocmd.Parameters["@taskid"].Value = entry.Value;
                        ocmd.Parameters["@date"].Value = ireader.GetDateTime(ireader.GetOrdinal("date")).Date;
                        ocmd.Parameters["@rating"].Value = rat;
                        ocmd.Parameters["@tcomment"].Value = ireader.GetString(ireader.GetOrdinal("tcomment"));
                        ocmd.Parameters["@scomment"].Value = ireader.GetString(ireader.GetOrdinal("scomment"));
                        ocmd.ExecuteNonQuery();
                    }

                }
            }
            return true;
        }

        static bool TransferStudents(MySqlConnection dest, SqlConnection src)
        {

            var pdict = new Dictionary<uint, uint>();
            Console.WriteLine("Students:");
            var scmd = src.CreateCommand();
            var pcmd = src.CreateCommand();
            var Dpcmd = dest.CreateCommand();
            var Dphcmd = dest.CreateCommand();

            scmd.CommandText = "select * from dbo.student where Target > '2017-09-01' order by Target";
            pcmd.CommandText = "select * from dbo.parent where pid = @pid";
            pcmd.Parameters.Add(new SqlParameter("@pid", 0));
            Dphcmd.CommandText = "insert into phones (family, phone, label) values (@family, @phone, @label)";


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
                "email, note, username, password, expires, male, trial, liturgy, torah, haftara) " +
                "values(1, @target, @lname, @fname, @parent, @teacher, " +
                "@email, @note, @username, @password, @expires, @male, @trial, " +
                "@liturgy, @torah, @haftara); select last_insert_id()";
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
            Dscmd.Parameters.Add(new MySqlParameter("@torah", SqlDbType.VarChar));
            Dscmd.Parameters.Add(new MySqlParameter("@haftara", SqlDbType.VarChar));

            Dphcmd.Parameters.Add(new MySqlParameter("@family", SqlDbType.Int));
            Dphcmd.Parameters.Add(new MySqlParameter("@phone", MySqlDbType.VarChar));
            Dphcmd.Parameters.Add(new MySqlParameter("@label", MySqlDbType.VarChar));


            using (var qreader = scmd.ExecuteReader())
            {
                while (qreader.Read())
                {
                    Console.WriteLine(qreader["Target"].ToString() + " " +
                        qreader["LName"] + ", " + qreader["FName"]);

                    // Parent Record
                    uint npid;
                    uint nstid;
                    var orig_stid = uint.Parse(qreader["STID"].ToString());
                    var pid = uint.Parse(qreader["Parent"].ToString());
                    if (pdict.TryGetValue(pid, out npid))
                    {
                        Console.WriteLine("   PARENT EXISTS. PID:" + npid);
                        continue;
                    }
                    else
                    {
                        // pcmd.Parameters.Clear();
                        pcmd.Parameters["@pid"].Value = pid;    // .AddWithValue("@pid", pid);
                        using (var preader = pcmd.ExecuteReader())
                        {
                            preader.Read();
                            Console.WriteLine("=> Found parent record: " + preader["Lname1"] + ", " + preader["Fname1"]);

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


                            // INSERT PARENT RECORD
                            npid = uint.Parse(Dpcmd.ExecuteScalar().ToString());                            

                            pdict.Add(pid, npid); // this should be new PID
                            Console.WriteLine("    Added parent " + npid);

                            // ADD PHONES
                            Console.Write("Phones:");
                            try
                            {
                                Dphcmd.Parameters["@family"].Value = npid;
                                var used = new List<string>();
                                for (int pn = 1; pn < 5; pn++)
                                {
                                    var plabel = preader["Phone" + pn + "Name"].ToString();
                                    var pnum = DigitsIn(preader["Phone" + pn].ToString());
                                    if (pnum.Length > 6 && !used.Contains(pnum))
                                    {
                                        used.Add(pnum);
                                        Console.Write(" " + pnum);
                                        Dphcmd.Parameters["@phone"].Value = pnum;
                                        Dphcmd.Parameters["@label"].Value = plabel;
                                        Dphcmd.ExecuteNonQuery();
                                    }
                                }
                                Console.WriteLine();
                            } catch (Exception ex)
                            {

                                Console.WriteLine(ex.ToString());
                            }

                        } // end of preader
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
                        Dscmd.Parameters.AddWithValue("@torah", qreader["Aux1"]);
                        Dscmd.Parameters.AddWithValue("@haftara", qreader["Aux2"]);

                        // INSERT STUDENT RECORD
                        nstid = uint.Parse(Dscmd.ExecuteScalar().ToString());
                        //nstid = 2;

                        Console.WriteLine("   Added student " + nstid);
                        TransferProgress(dest, src, nstid, orig_stid);
                    }
                }
                return true;
            }
        }
        static string DigitsIn(string pn)
        {
            var n = string.Empty;
            for (int q = 0; q < pn.Length; q++)
            {
                if (char.IsDigit(pn[q]))  n += pn[q]; 
            }
            return n;
        }
    }
    class Prayer
    {
        int oid;
        int pid;

        Prayer(int o, int p)
        {
            this.oid = o;
            this.pid = p;
        }
    }
}
