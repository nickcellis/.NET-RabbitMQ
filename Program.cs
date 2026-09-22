
using Backend_Core_with_RabbitMQ.Dtos;
using Backend_Core_with_RabbitMQ.Rabbit;
using Backend_Core_with_RabbitMQ.Rabbit.Publishers;
using Backend_Core_with_RabbitMQ.Rabbit.Queues;
using Backend_Core_with_RabbitMQ.RabbitCore.Interfaces;
using System.Text.Json;

namespace Backend_Core_with_RabbitMQ
{
    public class Program
    {

        private static String RABBITMQ_HOSTNAME = "localhost";

        public static void Main(string[] args)
        {
            var builder = WebApplication.CreateBuilder(args);

            // Add services to the container.

            builder.Services.AddControllers();
            // Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
            builder.Services.AddEndpointsApiExplorer();
            builder.Services.AddSwaggerGen();

            builder.Services.AddSingleton(sp => new RabbitMQConnectionManager(RABBITMQ_HOSTNAME));

            builder.Services.AddSingleton<IMessageProducer, RabbitMQProducer>();

            // Register Invoice Worker with safe Scoped DI access
            builder.Services.AddHostedService(sp => new RabbitMQQueueWorker(
                sp.GetRequiredService<RabbitMQConnectionManager>(),
                sp.GetRequiredService<IServiceScopeFactory>(),
                QueueType.INVOICE,
                async (scopedProvider, jsonMessage) =>
                {
                    Console.WriteLine($"Processing Invoice: {jsonMessage}");

                    var invoice = JsonSerializer.Deserialize<CreateInvoiceDto>(jsonMessage);
                    if (invoice != null)
                    {
                        Console.WriteLine($"Processing Invoice #{invoice.InvoiceId} for {invoice.CustomerName}");
                    }
                    // Safe to resolve EF Core DbContext here because of the scope!
                    // var db = scopedProvider.GetRequiredService<MyDbContext>();
                    await Task.CompletedTask;
                },
                sp.GetRequiredService<ILogger<RabbitMQQueueWorker>>()
            ));

            var app = builder.Build();

            // Configure the HTTP request pipeline.
            if (app.Environment.IsDevelopment())
            {
                app.UseSwagger();
                app.UseSwaggerUI();
            }

            app.UseHttpsRedirection();
            app.UseAuthorization();
            app.MapControllers();
            app.Run();
        }
    }
}
