using Backend_Core_with_RabbitMQ.Rabbit.Queues;

namespace Backend_Core_with_RabbitMQ.RabbitCore.Interfaces
{
    public interface IMessageProducer
    {
        Task SendMessageAsync<T>(QueueType queue, T message);
    }
}
