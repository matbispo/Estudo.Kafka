using Confluent.Kafka;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Estudo.Kafka.Producer
{
    class Function
    {
        public static async Task Main()
        {
            var config = new ProducerConfig {BootstrapServers = "localhost:9092" };

            using(var p = new ProducerBuilder<Null, string>(config).Build())
            {
                try
                {
                    int count = 1;
                    while (true)
                    {
                        var dr = await p.ProduceAsync("test-topic", 
                            new Message<Null, string> {Value = $"test: {count}" });

                        Console.WriteLine($"Delived '{dr.Value}' to '{dr.TopicPartitionOffset} | {count}'");

                        count++;
                        Thread.Sleep(3000);
                    }
                }
                catch (ProduceException<Null, string> ex)
                {
                    Console.WriteLine($"Delivery failed: {ex.Error.Reason}");
                }
            }
        }
    }
}
