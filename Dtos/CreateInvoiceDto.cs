namespace Backend_Core_with_RabbitMQ.Dtos
{
    public record CreateInvoiceDto(int InvoiceId, string CustomerName, decimal Amount);
}
