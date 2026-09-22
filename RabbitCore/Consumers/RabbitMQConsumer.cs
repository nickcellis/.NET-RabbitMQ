using Backend_Core_with_RabbitMQ.Rabbit.Queues;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

namespace Backend_Core_with_RabbitMQ.Rabbit.Com
{
    public class RabbitMQConsumer : RabbitMQConnectionManager
    {
        public RabbitMQConsumer(string hostname)
            : base(hostname)
        {
        }

        public async Task StartListening(QueueType queueType, CancellationToken stoppingToken, Func<string, Task> onMessageReceived)
        {
            using var channel = await CreateChannelAsync();

            await channel.QueueDeclareAsync(
                queue: queueType.ToString(),
                durable: true,
                exclusive: false,
                autoDelete: false
            );

            var consumer = new AsyncEventingBasicConsumer(channel);

            consumer.ReceivedAsync += async (sender, args) =>
            {
                var body = args.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);

                Console.WriteLine($"Received: {message}");

                await onMessageReceived(message);

                await channel.BasicAckAsync(
                    deliveryTag: args.DeliveryTag,
                    multiple: false
                );
            };

            await channel.BasicConsumeAsync(
                queue: queueType.ToString(),
                autoAck: false,
                consumer: consumer
            );

            Console.WriteLine($"Listening on queue: {queueType.ToString()}");

            while (!stoppingToken.IsCancellationRequested)
            {
                await Task.Delay(1000, stoppingToken);
            }
        }
    }
}