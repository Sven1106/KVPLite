using System.Collections.Generic;
using System.Data.SQLite;
using System.IO;
using System.Reflection;

namespace KVPLite
{
    public class Database
    {
        public string Filepath { get; }
        public string PragmaSettings { get;}
        private SQLiteConnection DbConnection { get; set; }
        public Database(bool optimisedForSpeed = false)
        {
            Filepath = Path.Combine(Path.GetDirectoryName(Assembly.GetEntryAssembly().Location), "KVPLite.sqlite");
            if (optimisedForSpeed)
            {
                PragmaSettings = "PRAGMA auto_vacuum = FULL; PRAGMA synchronous = OFF; PRAGMA journal_mode = MEMORY;";
            }
            else
            {
                PragmaSettings = "PRAGMA auto_vacuum = FULL; PRAGMA journal_mode = WAL;";
            }

            if (DbConnection == null)
            {
                DbConnection = new SQLiteConnection("Data Source=" + Filepath + ";Version=3;");
                if (System.IO.File.Exists(Filepath) == false)
                {
                    SQLiteConnection.CreateFile(Filepath);
                    DbConnection.Open();
                    using (var cmd = new SQLiteCommand(PragmaSettings, DbConnection))
                    {
                        cmd.CommandText += "CREATE TABLE Kvp (key char(8) primary key, value varchar(20000));";
                        cmd.ExecuteNonQuery();
                    }
                }
                else
                {
                    DbConnection.Open();
                    using (var cmd = new SQLiteCommand(PragmaSettings, DbConnection))
                    {
                        cmd.ExecuteNonQuery();
                        cmd.CommandText = "SELECT COUNT(*) as COUNT FROM sqlite_master;";
                        if ((long)cmd.ExecuteScalar() == 0)
                        {
                            cmd.CommandText = "CREATE TABLE Kvp (key char(8) primary key, value varchar(20000));";
                            cmd.ExecuteNonQuery();
                        }
                    }
                }
            }
        }
        ~Database()
        {
          DbConnection.Dispose();
        }

        public bool SetKvp(KeyValuePair<string, string> keyValuePair)
        {
            if (KvpExists(keyValuePair.Key))
            {
                return false;
            }
            using (SQLiteCommand cmd = new SQLiteCommand("INSERT INTO Kvp (key, value) values (@key, @value)", DbConnection))
            {
                cmd.Parameters.AddWithValue("@key", keyValuePair.Key);
                cmd.Parameters.AddWithValue("@value", keyValuePair.Value);
                cmd.ExecuteNonQuery();
            }
            return true;
        }
        public bool RemoveKvp(string key)
        {
            if (KvpExists(key) == false)
            {
                return false;
            }
            using (SQLiteCommand cmd = new SQLiteCommand("DELETE FROM Kvp WHERE key=@key", DbConnection))
            {
                cmd.Parameters.AddWithValue("@key", key);
                cmd.ExecuteNonQuery();
            }
            return true;
        }
        public bool RemoveAllKvp()
        {
            using (SQLiteCommand cmd = new SQLiteCommand("DELETE FROM Kvp", DbConnection))
            {
                cmd.ExecuteNonQuery();
                cmd.CommandText = "SELECT COUNT(*) as COUNT FROM Kvp;";
                if ((long)cmd.ExecuteScalar() > 0)
                {
                    return false;
                }
            }
            return true;
        }
        public KeyValuePair<string, string> GetKvp(string key)
        {
            KeyValuePair<string, string> keyValuePair = new KeyValuePair<string, string>();
            if (KvpExists(key) == false)
            {
                return keyValuePair;
            }
            using (SQLiteCommand cmd = new SQLiteCommand("SELECT * FROM Kvp WHERE key=@key", DbConnection))
            {
                cmd.Parameters.AddWithValue("@key", key);
                using (var dbresult = cmd.ExecuteReader())
                {
                    dbresult.Read();
                    keyValuePair = new KeyValuePair<string, string>(dbresult.GetString(0), dbresult.GetString(1));
                }
            }
            return keyValuePair;
        }
        private bool KvpExists(string key)
        {
            bool exists = false;
            using (SQLiteCommand cmd = new SQLiteCommand("SELECT EXISTS(SELECT 1 FROM Kvp WHERE key=@key LIMIT 1);", DbConnection))
            {
                cmd.Parameters.AddWithValue("@key", key);
                exists = (long)cmd.ExecuteScalar() == 1;
            }
            return exists;
        }
    }
}
