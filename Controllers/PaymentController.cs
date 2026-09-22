using Backend_Core_with_RabbitMQ.Dtos;
using Backend_Core_with_RabbitMQ.Rabbit.Queues;
using Backend_Core_with_RabbitMQ.RabbitCore.Interfaces;
using Microsoft.AspNetCore.Mvc;

[ApiController]
[Route("[controller]")]
public class PaymentController : ControllerBase
{
    private readonly IMessageProducer _producer;


    public PaymentController(IMessageProducer producer)
    {
        _producer = producer;
    }

    [HttpPost]
    public async Task<IActionResult> CreateInvoice()
    {
        var invoice = new CreateInvoiceDto(101, "Example", 450.00m);

        // Pass the object directly!
        await _producer.SendMessageAsync(QueueType.INVOICE, invoice);

        return Ok("Invoice message sent to queue!");
    }
}