using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;

namespace KVPLite.Tests
{
    [TestFixture]
    public class Tests
    {
        Database Database { get; set; }

        public Tests()
        {
            this.Database = new Database();
            this.Database.RemoveAllKvp();
        }


        [Test]
        public void InsertMany_Success()
        {
            Database database = new Database();
            for (int i = 0; i < 10000; i++)
            {
                var kvp = new KeyValuePair<string, string>(Guid.NewGuid().ToString(), i.ToString());
                database.SetKvp(kvp);
            }
        }
    }
}