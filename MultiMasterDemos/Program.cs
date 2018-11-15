using System;
using System.Threading.Tasks;

namespace MultiMasterDemos
{
    class Program
    {      
        public static async Task Main(string[] args)
        {
            /*
             * Resources needed for this demo:
             * - Cosmos DB account (multi-master = disabled), SouthEast Asia, West US 2, North Europe
             * - Cosmos DB account (multi-master = enabled), SouthEast Asia, West US 2, North Europe
             * - Windows VM, West US 2, Standard B4 (4 core, 16GB), RDP enabled.
			 * - Copy the project to the VM in West US 2 and run from desktop.
             * 
             * - Fill endpoints and keys from both Cosmos DB accounts in app.config
			 * - Uncomment InitializeDemo() below to create the database and collections for both accounts
            */

            Demo demo = new Demo();
            await demo.RunDemo();
        }
    }
    class Demo
    {
        SingleMaster single;
        MultiMaster multi;
        Conflicts conflicts;
        public async Task RunDemo()
        {
            //Only run this once to setup databases and collections
            //await InitializeDemo();

            single = new SingleMaster();
            single.TestReadLatency();
            await single.TestWriteLatency();

            Console.WriteLine("Press any key to continue...");
            Console.ReadKey(true);

            multi = new MultiMaster();
            multi.TestReadLatency();
            await multi.TestWriteLatency();

            Console.WriteLine("Press any key to continue...");
            Console.ReadKey(true);

            conflicts = new Conflicts();
            await conflicts.GenerateConflicts();

            Console.WriteLine("Press any key to exit...");
            Console.ReadKey(true);

            await Cleanup();
        }
        private async Task Cleanup()
        {
            await single.CleanUp();
            await multi.CleanUp();
            await conflicts.CleanUp();
        }
        private async Task InitializeDemo()
        {
            //Run this before executing the demo to set up the database and collections.

            SingleMaster single = new SingleMaster();
            MultiMaster multi = new MultiMaster();
            Conflicts conflicts = new Conflicts();

            await single.Initalize();
            await multi.Initalize();
            await conflicts.Initalize();
        }

    }
}
