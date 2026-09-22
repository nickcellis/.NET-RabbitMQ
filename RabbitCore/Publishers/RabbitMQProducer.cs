using Backend_Core_with_RabbitMQ.Rabbit.Queues;
using Backend_Core_with_RabbitMQ.RabbitCore.Interfaces;
using RabbitMQ.Client;
using System.Text;
using System.Text.Json;

namespace Backend_Core_with_RabbitMQ.Rabbit.Publishers
{
    public class RabbitMQProducer : IMessageProducer
    {
        private readonly RabbitMQConnectionManager _connectionManager;

        public RabbitMQProducer(RabbitMQConnectionManager connectionManager)
        {
            _connectionManager = connectionManager;
        }

        public async Task SendMessageAsync<T>(QueueType queue, T message)
        {
            // Use the connection manager to get a channel safely
            using var channel = await _connectionManager.CreateChannelAsync();

            await channel.QueueDeclareAsync(
                queue: queue.ToString(),
                durable: true,
                exclusive: false,
                autoDelete: false
            );

            var jsonString = JsonSerializer.Serialize(message);
            var body = Encoding.UTF8.GetBytes(jsonString);
            Console.WriteLine($"Sending raw message: {message}");

            await channel.BasicPublishAsync(
                exchange: "",
                routingKey: queue.ToString(),
                body: body
            );
        }
    }
}