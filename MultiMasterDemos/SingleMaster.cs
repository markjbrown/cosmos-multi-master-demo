using System;
using System.Configuration;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using System.Diagnostics;

namespace MultiMasterDemos
{
    class SingleMaster
    {
        private string endpoint;
        private string key;
        private string databaseName;
        private string collectionName;
        private string[] regions;
        private Uri databaseUri;
        private Uri collectionUri;

        private DocumentClient client;
        
        public SingleMaster()
        {
            endpoint = ConfigurationManager.AppSettings["endpointSM"];
            key = ConfigurationManager.AppSettings["keySM"];
            regions = ConfigurationManager.AppSettings["regions"].Split(new string[] { ";" }, StringSplitOptions.RemoveEmptyEntries);
            databaseName = ConfigurationManager.AppSettings["database"];
            collectionName = ConfigurationManager.AppSettings["collection"];

            databaseUri = UriFactory.CreateDatabaseUri(databaseName);
            collectionUri = UriFactory.CreateDocumentCollectionUri(databaseName, collectionName);

            ConnectionPolicy policy = new ConnectionPolicy
            {
                ConnectionMode = ConnectionMode.Direct,
                ConnectionProtocol = Protocol.Tcp
            };
            Console.WriteLine("Single Master Demo: Read and Write latency tests...");
            Console.WriteLine("---------------------------------------------------\r\n...");

            Console.WriteLine($"Creating DocumentClient for Single Master database with the following regional preferences...");

            foreach (string region in regions)
            {
                policy.PreferredLocations.Add(region);
                Console.WriteLine($"Region: {region}");
            }
            Console.WriteLine();
            Console.WriteLine("Press any key to continue...");
            Console.ReadKey(true);

            client = new DocumentClient(new Uri(endpoint), key, policy, ConsistencyLevel.Eventual);
        }
        public async Task Initalize()
        {
            //create the database
            await client.CreateDatabaseIfNotExistsAsync(new Database { Id = databaseName });

            //Create the collection
            RequestOptions options = new RequestOptions { ConsistencyLevel = ConsistencyLevel.Eventual, OfferThroughput = 9900 };
            PartitionKeyDefinition partKey = new PartitionKeyDefinition();
            partKey.Paths.Add("/postalcode");
            DocumentCollection col = new DocumentCollection { Id = collectionName, PartitionKey = partKey };
            await client.CreateDocumentCollectionIfNotExistsAsync(databaseUri, col, options);

            //Allow time to replicate
            await Task.Delay(5000);
        }

        public void TestReadLatency()
        {
            //Insert a sample document to use for Read Latency Test
            SampleDoc document = new SampleDoc
            {
                Name = "Scott Guthrie",
                City = "Redmond",
                PostalCode = "98052",
                UserDefinedId = 0, //not used in this demo
                Region = "Southeast Asia"
            };
            //insert the document
            client.CreateDocumentAsync(collectionUri, document).GetAwaiter().GetResult();
            

            string region = client.ConnectionPolicy.PreferredLocations[0].ToString();

            Console.WriteLine();
            Console.WriteLine($"Test for 100 reads against region: {region}\r\nPress any key to continue\r\n...");
            Console.ReadKey(true);

            Stopwatch stopwatch = new Stopwatch();

            for (int i = 0; i < 100; i++)
            {
                stopwatch.Start();
                    var sql = "SELECT TOP 1 * FROM c WHERE c.postalcode = '98052'";
                    var resp = client.CreateDocumentQuery(collectionUri, sql).ToList();
                stopwatch.Stop();

                Console.WriteLine($"Read operation {i} of 100. From region: {region}, elapsed time: {stopwatch.ElapsedMilliseconds} ms");
                stopwatch.Reset();
            }
        }
        public async Task TestWriteLatency()
        {
            string region = client.ConnectionPolicy.PreferredLocations[0].ToString();

            Console.WriteLine();
            Console.WriteLine($"Test for 100 writes against region: {region}\r\nPress any key to continue\r\n...");
            Console.ReadKey(true);

            SampleDoc document = new SampleDoc
            {
                Name = "Scott Guthrie",
                City = "Redmond",
                PostalCode = "98052",
                UserDefinedId = 9, //not used in this demo
                Region = region
            };

            Stopwatch stopwatch = new Stopwatch();

            for (int i = 0; i < 100; i++)
            {
                stopwatch.Start();
                    //insert the document
                    await client.CreateDocumentAsync(collectionUri, document);
                stopwatch.Stop();

                Console.WriteLine($"Write {i} of 100. Insert to region: {region}, elapsed time: {stopwatch.ElapsedMilliseconds} ms");
                stopwatch.Reset();
            }
        }
        
        public async Task CleanUp()
        {
            string sql = "SELECT * FROM c";
            FeedOptions options = new FeedOptions { EnableCrossPartitionQuery = true };
            var docs = client.CreateDocumentQuery(collectionUri, sql, options).ToList();

            foreach (var doc in docs)
            {
                var requestOptions = new RequestOptions { PartitionKey = new PartitionKey(doc.postalcode) };
                await client.DeleteDocumentAsync(doc._self, requestOptions);
            }
        }
    }
}
