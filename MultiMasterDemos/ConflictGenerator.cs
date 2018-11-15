using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Configuration;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using System.Diagnostics;
using Newtonsoft.Json;

namespace MultiMasterDemos
{
    class ConflictGenerator
    {
        // Collection of clients with different primary regions used to generate conflicts
        private List<DocumentClient> regionalClients;

        public ConflictGenerator(string endpointUrl, string authorizationKey, string[] regions, string databaseName)
        {
            regionalClients = new List<DocumentClient>();

            //Loop through each region creating a new client
            foreach (string region in regions)
            {
                //Define the connection policy
                ConnectionPolicy policy = new ConnectionPolicy
                {
                    ConnectionMode = ConnectionMode.Direct,
                    ConnectionProtocol = Protocol.Tcp,
                    UseMultipleWriteLocations = true    //specify multiple write locations
                };

                // Set the primary region for the client
                policy.PreferredLocations.Add(region);

                //Add the new client instance to the collection
                regionalClients.Add(new DocumentClient(new Uri(endpointUrl), authorizationKey, policy, ConsistencyLevel.Eventual));
            }
        }

        public async Task GenerateInsertConflicts(Uri collectionUri)
        {
            bool isConflicts = false;

            Console.WriteLine();
            Console.WriteLine($"Insert documents with the same Id in multiple regions.\r\nPress any key to continue...");
            Console.ReadKey(true);

            while (!isConflicts)
            {
                IList<Task<Document>> insertTask = new List<Task<Document>>();
                //Create a new document id to conflict on
                int documentId = RandomGen.Next(0, 1000);

                foreach (DocumentClient client in regionalClients)
                {
                    insertTask.Add(InsertDocument(client, collectionUri, documentId));
                }
                //Return the documents inserted
                Document[] insertedDocuments = await Task.WhenAll(insertTask);

                //Delay to allow data to replicate
                await Task.Delay(1000);
                
                Console.WriteLine($"Multiple items inserted. Continue generating conflicts?...");
                ConsoleKeyInfo result = Console.ReadKey(true);
                if ((result.KeyChar == 'Y') || (result.KeyChar == 'y'))
                {
                    isConflicts = false;
                }
                else
                {
                    isConflicts = true;
                    break;
                }
                Console.WriteLine();
            }
        }
        private async Task<Document> InsertDocument(DocumentClient client, Uri collectionUri, int docId)
        {
            //Regional endpoint this document will be written to
            string region = client.ConnectionPolicy.PreferredLocations[0].ToString();

            //Create a random number between 0-10 for each document. This will be used by UDP stored procedure and LWW samples for conflict resolution.
            int userdefid = RandomGen.Next(0, 10);

            SampleDoc doc = new SampleDoc
            {
                Id = docId.ToString(),
                Name = "Scott Guthrie",
                City = "Redmond",
                PostalCode = "98052",
                UserDefinedId = userdefid,
                Region = region
            };

            Console.WriteLine($"Id: {doc.Id}, Name: {doc.Name}, City: {doc.City}, PostalCode: {doc.PostalCode}, UserDefId: {doc.UserDefinedId}, Region: {doc.Region}");

            try
            {
                return await client.CreateDocumentAsync(collectionUri, doc);
            }
            catch (DocumentClientException ex)
            {
                if (ex.StatusCode == System.Net.HttpStatusCode.Conflict)
                {
                    Console.WriteLine();
                    Console.WriteLine($"Attempted Insert for Doc Id: {doc.Id} from Region: {doc.Region} unsuccessful as previous insert already committed and replicated.");
                    //Conflict has already replicated so return null
                    return null;
                }
                throw;
            }
        }

        public async Task GenerateUpdateConflicts(Uri collectionUri)
        {
            bool isConflicts = false;

            Console.WriteLine();
            Console.WriteLine($"Update a document with same ID in multiple regions to generate conflicts.\r\nPress any key to continue...");
            Console.ReadKey(true);

            while (!isConflicts)
            {
                IList<Task<Document>> updateTask = new List<Task<Document>>();

                //Create a new document id to conflict on
                int documentId = RandomGen.Next(0, 1000);

                Console.WriteLine();
                Console.WriteLine($"Inserting new document to create an update conflict with ID: {documentId} in multiple regions.");

                Document conflictDoc = await InsertDocument(regionalClients[0], collectionUri, documentId);
                await Task.Delay(2000); //Allow the insert to replicate

                foreach (DocumentClient client in regionalClients)
                {
                    updateTask.Add(UpdateDocument(client, collectionUri, conflictDoc));

                }
                //Return the documents updated
                Document[] updatedDocuments = await Task.WhenAll(updateTask);

                //Delay to allow data to replicate
                await Task.Delay(1000);

                Console.WriteLine();
                Console.WriteLine($"Updates from multiple simultaneous regions complete. Continue generating update conflicts?...");
                ConsoleKeyInfo result = Console.ReadKey(true);
                if ((result.KeyChar == 'Y') || (result.KeyChar == 'y'))
                {
                    isConflicts = false;
                }
                else
                {
                    isConflicts = true;
                    break;
                }
                Console.WriteLine();
            }
        }
        private async Task<Document> UpdateDocument(DocumentClient client, Uri collectionUri, Document document)
        {
            //The region property for the committed document replicated to all regions.
            //The update we will make will update that to be the region this document will be updated in.
            string region = client.ConnectionPolicy.PreferredLocations[0].ToString();
            //update the region property
            document.SetPropertyValue("region", region);

            SampleDoc doc = JsonConvert.DeserializeObject<SampleDoc>(document.ToString());
            Console.WriteLine($"Updating document, Id: {doc.Id} from Region: {doc.Region} to Region: {region}");

            try
            {
                return await client.ReplaceDocumentAsync(document.SelfLink, document, new RequestOptions
                {
                    AccessCondition = new AccessCondition
                    {
                        Type = AccessConditionType.IfMatch,
                        Condition = document.ETag
                    }
                });
            }
            catch (DocumentClientException ex)
            {
                if (ex.StatusCode == System.Net.HttpStatusCode.PreconditionFailed ||
                    ex.StatusCode == System.Net.HttpStatusCode.NotFound)
                {
                    Console.WriteLine();
                    Console.WriteLine($"Attempted Update on Doc Id: {doc.Id} from Region: {doc.Region} unsuccessful as previous update already committed and replicated.");
                    //Lost synchronously or not replicated yet. No conflict is induced.
                    return null;
                }
                throw;
            }
        }

        public async Task GenerateDeleteConflicts(Uri collectionUri)
        {
            //Debugger.Break();

            bool isConflicts = false;

            Console.WriteLine();
            Console.WriteLine($"Asynchronously update a deleted document with the same ID in multiple regions to generate conflicts. \r\nPress any key to continue...");
            Console.ReadKey(true);

            while (!isConflicts)
            {
                IList<Task<Document>> deleteTask = new List<Task<Document>>();

                //Create a new document id to conflict on
                int documentId = RandomGen.Next(0, 1000);

                Console.WriteLine();
                Console.WriteLine($"Inserting new document to create an delete conflict with ID: {documentId} in multiple regions.");
                Console.WriteLine("-------------------------------------------------------------------------");

                Document conflictDoc = await InsertDocument(regionalClients[0], collectionUri, documentId);
                await Task.Delay(1000); //Allow the insert to replicate

                //Flag to flip the task between deletes and updates
                bool isDeleteTask = true;

                foreach (DocumentClient client in regionalClients)
                {
                    if (isDeleteTask)
                    {
                        //perform the delete task
                        deleteTask.Add(DeleteDocument(client, collectionUri, conflictDoc));
                        isDeleteTask = false;
                    }
                    else
                    {
                        //perform update task 
                        deleteTask.Add(UpdateDocument(client, collectionUri, conflictDoc));
                        isDeleteTask = true;
                    }
                }
                //Return the documents updated
                Document[] deletedDocuments = await Task.WhenAll(deleteTask);


                //Delay to allow data to replicate
                await Task.Delay(1000);

                //Check for conflicts. If 1 or greater commits then conflicts.
                foreach (Document doc in deletedDocuments)
                {
                    if (doc != null)
                    {
                        //Check the Conflicts Feed
                        FeedResponse<Conflict> conflicts = await regionalClients[0].ReadConflictFeedAsync(collectionUri);
                        //If > 0 then we have conflicts
                        if (conflicts.Count > 0)
                        {
                            isConflicts = true;
                            break;
                        }
                    }
                }

                //else no Conflicts, retry to induce conflicts
                Console.WriteLine($"No conflicts in Conflicts feed. Continue generating conflicts...");
                Console.WriteLine();
            }
        }
        private async Task<Document> DeleteDocument(DocumentClient client, Uri collectionUri, Document document)
        {
            try
            {
                Console.WriteLine($"Deleting document, Id: {document.Id}");
                return await client.DeleteDocumentAsync(document.SelfLink, new RequestOptions
                {
                    AccessCondition = new AccessCondition
                    {
                        Type = AccessConditionType.IfMatch,
                        Condition = document.ETag
                    }
                });
            }
            catch (DocumentClientException ex)
            {
                if (ex.StatusCode == System.Net.HttpStatusCode.PreconditionFailed ||
                    ex.StatusCode == System.Net.HttpStatusCode.NotFound)
                {
                    //Lost synchronously. No conflict is induced.
                    return null;
                }
                throw;
            }
        }
    }
    public class SampleDoc
    {
        [JsonProperty(PropertyName = "id")]
        public string Id { get; set; }

        [JsonProperty(PropertyName = "name")]
        public string Name { get; set; }

        [JsonProperty(PropertyName = "city")]
        public string City { get; set; }

        [JsonProperty(PropertyName = "postalcode")]
        public string PostalCode { get; set; }

        [JsonProperty(PropertyName = "userdefinedid")]
        public int UserDefinedId { get; set; }

        [JsonProperty(PropertyName = "region")]
        public string Region { get; set; }
    }

    public static class RandomGen
    {
        private static Random _global = new Random();
        [ThreadStatic]
        private static Random _local;
        public static int Next()
        {
            Random inst = _local;
            if (inst == null)
            {
                int seed;
                lock (_global) seed = _global.Next();
                _local = inst = new Random(seed);
            }
            return inst.Next();
        }
        public static int Next(int minValue, int maxValue)
        {
            Random inst = _local;
            if (inst == null)
            {
                int seed;
                lock (_global) seed = _global.Next(minValue, maxValue);
                _local = inst = new Random(seed);
            }
            return inst.Next(minValue, maxValue);
        }
    }
}
