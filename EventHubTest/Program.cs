using Microsoft.Azure.EventHubs;
using System;
using System.Text;
using System.Threading.Tasks;

namespace EventHubTest {
    class Program {
        private const string _connectionString = "Endpoint=sb://powerhub-event-hub.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=iOBuwz43glBQ0rOhd9I1kC1o4k2ot6HLqKQ8C+Op+4c=;EntityPath=powerhub-timeseries";

        private static EventHubClient _eventHubClient;
        
        static async Task Main(string[] args) {
            _eventHubClient = EventHubClient.CreateFromConnectionString(_connectionString);

            await SendMessagesToEventHub(100);
            await _eventHubClient.CloseAsync();
            Console.WriteLine("Press ENTER to exit.");
            Console.ReadLine();
        }

        // Creates an event hub client and sends 100 messages to the event hub.
        private static async Task SendMessagesToEventHub(int numMessagesToSend) {
            for (var i = 0; i < numMessagesToSend; i++) {
                try {
                    var message = $"Message {i} {DateTime.Now} ";
                    Console.WriteLine($"Sending message: {message}");
                    await _eventHubClient.SendAsync(new EventData(Encoding.UTF8.GetBytes(message)));
                }
                catch (Exception exception) {
                    Console.WriteLine($"{DateTime.Now} > Exception: {exception.Message}");
                }

                await Task.Delay(10);
            }

            Console.WriteLine($"{numMessagesToSend} messages sent.");
        }
    }
}
