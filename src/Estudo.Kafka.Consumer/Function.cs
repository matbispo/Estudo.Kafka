using Confluent.Kafka;
using System;
using System.Threading;

namespace Estudo.Kafka.Consumer
{
    class Function
    {
        static void Main()
        {
            var config = new ConsumerConfig
            {
                GroupId = "test-consumer-group",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using(var c = new ConsumerBuilder<Ignore, string>(config).Build())
            {
                c.Subscribe("test-topic");

                var cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true;
                    cts.Cancel();
                };

                try
                {
                    while (true)
                    {
                        try
                        {
                            var cr = c.Consume(cts.Token);
                            Console.WriteLine($"Consumed messagem '{cr.Value}' at: {cr.TopicPartitionOffset}'.");
                        }
                        catch (ConsumeException ce)
                        {
                            Console.WriteLine($"Error occured: {ce.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    c.Close();
                }
            }
        }
    }
}
