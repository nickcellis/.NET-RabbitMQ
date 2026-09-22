using Backend_Core_with_RabbitMQ.Rabbit;
using Backend_Core_with_RabbitMQ.Rabbit.Queues;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

public class RabbitMQQueueWorker : BackgroundService
{
    private readonly RabbitMQConnectionManager _connectionManager;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly QueueType _queueType;
    private readonly Func<IServiceProvider, string, Task> _messageHandler;
    private readonly ILogger<RabbitMQQueueWorker> _logger;

    public RabbitMQQueueWorker(
        RabbitMQConnectionManager connectionManager,
        IServiceScopeFactory scopeFactory,
        QueueType queueType,
        Func<IServiceProvider, string, Task> messageHandler,
        ILogger<RabbitMQQueueWorker> logger)
    {
        _connectionManager = connectionManager;
        _scopeFactory = scopeFactory;
        _queueType = queueType;
        _messageHandler = messageHandler;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Obtain a channel from our thread-safe manager
        using var channel = await _connectionManager.CreateChannelAsync(stoppingToken);

        await channel.QueueDeclareAsync(
            queue: _queueType.ToString(),
            durable: true,
            exclusive: false,
            autoDelete: false,
            cancellationToken: stoppingToken
        );

        var consumer = new AsyncEventingBasicConsumer(channel);

        consumer.ReceivedAsync += async (sender, args) =>
        {
            var body = args.Body.ToArray();
            var jsonMessage = Encoding.UTF8.GetString(body);

            try
            {
                Console.WriteLine($"Received raw message: {jsonMessage}");

                // safely resolve Scoped services like your EF Core DbContext
                using (var scope = _scopeFactory.CreateScope())
                {
                    await _messageHandler(scope.ServiceProvider, jsonMessage);
                }

                // SUCCESS: Acknowledge the message so RabbitMQ deletes it
                await channel.BasicAckAsync(deliveryTag: args.DeliveryTag, multiple: false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing message from queue {Queue}", _queueType);

                // FAILURE: Reject the message and re-queue it (or send to a dead-letter queue)
                // requeue: false drops it or sends it to a dead-letter exchange if configured
                await channel.BasicNackAsync(deliveryTag: args.DeliveryTag, multiple: false, requeue: true);
            }
        };

        await channel.BasicConsumeAsync(
            queue: _queueType.ToString(),
            autoAck: false, // CRITICAL: Must be false so we manually ACK/NACK
            consumer: consumer,
            cancellationToken: stoppingToken
        );

        _logger.LogInformation("Worker started listening on queue: {Queue}", _queueType);

        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(1000, stoppingToken);
        }
    }
}