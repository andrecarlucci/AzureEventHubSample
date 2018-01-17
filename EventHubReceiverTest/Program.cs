using Microsoft.Azure.EventHubs;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace EventHubReceiverTest {
    partial class Program {

        private const string _connectionString = "foo";
        private static bool _receive = true;

        static async Task Main(string[] args) {
            Console.WriteLine("Start!");

            var client = EventHubClient.CreateFromConnectionString(_connectionString);
            var info = await client.GetRuntimeInformationAsync();

            var receiver = client.CreateReceiver(PartitionReceiver.DefaultConsumerGroupName,
                                                 info.PartitionIds[0],
                                                 "");


            receiver.SetReceiveHandler(new PartitionReceiveHandler());

            Task.Factory.StartNew(async () => {
                while (_receive) {
                    Console.WriteLine("Waiting");
                    var msgs = await receiver.ReceiveAsync(200);
                    msgs.ConsoleWrite();
                }
            });
            
            Console.WriteLine("Press any key to stop");
            Console.ReadLine();
            await Stop(client, receiver);
        }

        private static async Task Stop(EventHubClient client, PartitionReceiver receiver) {
            _receive = false;
            Console.WriteLine("Stoping...");
            await receiver.CloseAsync();
            await client.CloseAsync();
            Console.WriteLine("The end");
        }
    }

    public class PartitionReceiveHandler : IPartitionReceiveHandler {
        public int MaxBatchSize { get; }
        
        public Task ProcessEventsAsync(IEnumerable<EventData> msgs) {
            if(msgs == null) {
                Console.WriteLine("Null :(");
                return Task.CompletedTask;
            }
            msgs.ConsoleWrite();
            return Task.CompletedTask;
        }
        public Task ProcessErrorAsync(Exception error) {
            Console.WriteLine("Error!: " + error);
            return Task.CompletedTask;
        }
    }

    public static class EventDataExtensions {
        public static void ConsoleWrite(this IEnumerable<EventData> msgs) {
            foreach (var msg in msgs) {
                var text = Encoding.UTF8.GetString(msg.Body.ToArray());
                Console.WriteLine($"Message: Seq: {msg.SystemProperties.SequenceNumber} OffSet: {msg.SystemProperties.Offset} Text: {text}");
            }
        }
    }
}
