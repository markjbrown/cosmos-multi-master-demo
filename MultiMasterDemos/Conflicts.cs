using System;
using System.Configuration;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;

namespace MultiMasterDemos
{
    class Conflicts
    {
        private string endpoint;
        private string key;
        private string databaseName;
        private string collectionNameLWW;
        private string collectionNameAsync;
        private string[] regions;
        private Uri databaseUri;
        private Uri collectionUriLWW;
        private Uri collectionUriAsync;

        private DocumentClient client;

        public Conflicts()
        {
            endpoint = ConfigurationManager.AppSettings["endpointMM"];
            key = ConfigurationManager.AppSettings["keyMM"];
            regions = ConfigurationManager.AppSettings["regions"].Split(new string[] { ";" }, StringSplitOptions.RemoveEmptyEntries);
            databaseName = ConfigurationManager.AppSettings["database"];
            collectionNameLWW = ConfigurationManager.AppSettings["Lwwcollection"];
            collectionNameAsync = ConfigurationManager.AppSettings["Asynccollection"];


            databaseUri = UriFactory.CreateDatabaseUri(databaseName);
            collectionUriLWW = UriFactory.CreateDocumentCollectionUri(databaseName, collectionNameLWW);
            collectionUriAsync = UriFactory.CreateDocumentCollectionUri(databaseName, collectionNameAsync);

            //New Connection Policy property for Multi-Master
            ConnectionPolicy policy = new ConnectionPolicy
            {
                ConnectionMode = ConnectionMode.Direct,
                ConnectionProtocol = Protocol.Tcp,
                UseMultipleWriteLocations = true //Multiple write locations
            };

            Console.WriteLine("Multi Master Demo: Conflict Resolution...");
            Console.WriteLine("---------------------------------------------------\r\n...");
            Console.WriteLine($"Creating DocumentClient for Multi Master database with the following regional preference...");

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

            //Create the collections
            RequestOptions options = new RequestOptions
            {
                ConsistencyLevel = ConsistencyLevel.Eventual,
                OfferThroughput = 9900
            };

            //For LWW Collection, use the Default policy, include a path to resolve conflicts
            ConflictResolutionPolicy policyLWW = new ConflictResolutionPolicy
            {
                Mode = ConflictResolutionMode.LastWriterWins,
                ConflictResolutionPath = "/userdefinedid"
            };

            PartitionKeyDefinition partKey = new PartitionKeyDefinition();
            partKey.Paths.Add("/postalcode");

            DocumentCollection colLWW = new DocumentCollection
            {
                Id = collectionNameLWW,
                PartitionKey = partKey,
                ConflictResolutionPolicy = policyLWW
            };

            await client.CreateDocumentCollectionIfNotExistsAsync(databaseUri, colLWW, options);

            //Allow time to replicate
            await Task.Delay(5000);


            //For Async Collection, use the Custom policy
            ConflictResolutionPolicy policyAsync = new ConflictResolutionPolicy
            {
                Mode = ConflictResolutionMode.Custom
            };

            DocumentCollection colAsync = new DocumentCollection
            {
                Id = collectionNameAsync,
                PartitionKey = partKey,
                ConflictResolutionPolicy = policyAsync
            };

            await client.CreateDocumentCollectionIfNotExistsAsync(databaseUri, colAsync, options);

            //Allow time to replicate
            await Task.Delay(5000);
        }

        public async Task GenerateConflicts()
        {
            //Create a new conflict generator
            ConflictGenerator generator = new ConflictGenerator(endpoint, key, regions, databaseName);

            //Last Writer Wins
            Console.WriteLine();
            Console.WriteLine("Generate conflicts in collection using Last Writer Wins resolution.");
            Console.WriteLine("...................................................................\r\n");

            //create some insert conflicts on Collection with Last Writer Wins Resolution
            await generator.GenerateInsertConflicts(collectionUriLWW);


            //Custom Async
            Console.WriteLine();
            Console.WriteLine("Generate conflicts in collection using Manual resolution.");
            Console.WriteLine("...................................................................\r\n");

            //Create a new conflict generator
            await generator.GenerateUpdateConflicts(collectionUriAsync);
        }

        public async Task CleanUp()
        {
            //delete any conflicts that didn't get resolved
            FeedResponse<Conflict> conflicts = await client.ReadConflictFeedAsync(collectionUriAsync);

            foreach (Conflict conflict in conflicts)
            {
                await client.DeleteConflictAsync(conflict.SelfLink);
            }

            //clean out any items from the collections to reset
            string sql = "SELECT * FROM c";
            FeedOptions options = new FeedOptions { EnableCrossPartitionQuery = true };
            var docs = client.CreateDocumentQuery(collectionUriAsync, sql, options).ToList();

            foreach (var doc in docs)
            {
                var requestOptions = new RequestOptions { PartitionKey = new PartitionKey(doc.postalcode) };
                await client.DeleteDocumentAsync(doc._self, requestOptions);
            }

            docs = client.CreateDocumentQuery(collectionUriLWW, sql, options).ToList();

            foreach (var doc in docs)
            {
                var requestOptions = new RequestOptions { PartitionKey = new PartitionKey(doc.postalcode) };
                await client.DeleteDocumentAsync(doc._self, requestOptions);
            }
        }
    }
}
