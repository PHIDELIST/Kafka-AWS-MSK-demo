using System;
using System.Threading.Tasks;
using Confluent.Kafka;

class Program
{
    static async Task Main(string[] args)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = "localhost:29092"
        };

        using var producer = new ProducerBuilder<Null, string>(config).Build();

        for (int i = 0; i < 10; i++)
        {
            var message = $"Message {i}";
            try
            {
                var result = await producer.ProduceAsync("test-topics", new Message<Null, string> { Value = message });
                Console.WriteLine($"Delivered '{result.Value}' to '{result.TopicPartitionOffset}'");
            }
            catch (ProduceException<Null, string> ex)
            {
                Console.WriteLine($"Delivery failed: {ex.Error.Reason}");
            }
        }

        producer.Flush(TimeSpan.FromSeconds(10));
    }
}
