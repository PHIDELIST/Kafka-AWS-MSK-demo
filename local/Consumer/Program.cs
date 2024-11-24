using System;
using System.Threading;
using Confluent.Kafka;

class Program
{
    static void Main(string[] args)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "localhost:29092",
            GroupId = "test-consumer-group",  
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();

        consumer.Subscribe("test-topics");

        CancellationTokenSource cts = new();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true; 
            cts.Cancel();
        };

        try
        {
            while (true)
            {
                Console.WriteLine("Waiting for messages...");
                var cr = consumer.Consume(cts.Token);
                if (cr != null)
                {
                    Console.WriteLine($"Consumed message '{cr.Value}' at: '{cr.TopicPartitionOffset}'.");
                }
            }
        }
        catch (ConsumeException ex)
        {
            Console.WriteLine($"Error consuming message: {ex.Error.Reason}");
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("Shutting down...");
        }
        finally
        {
            consumer.Close(); 
        }
    }
}
