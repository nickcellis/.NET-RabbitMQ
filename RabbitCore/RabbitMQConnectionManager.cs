using RabbitMQ.Client;

namespace Backend_Core_with_RabbitMQ.Rabbit
{
    public class RabbitMQConnectionManager : IAsyncDisposable
    {
        protected readonly string HostName;
        protected readonly ConnectionFactory _factory;
        private readonly SemaphoreSlim _lock = new(1, 1);
        private IConnection? _connection;
        private bool _disposed;

        public RabbitMQConnectionManager(string hostname)
        {
            HostName = hostname;

            _factory = new ConnectionFactory
            {
                HostName = hostname
            };
        }

        public async Task<IChannel> CreateChannelAsync(CancellationToken cancellationToken = default)
        {
            if (_connection == null || !_connection.IsOpen)
            {
                await _lock.WaitAsync(cancellationToken);
                try
                {
                    // Double-check locking pattern to prevent race conditions on startup
                    if (_connection == null || !_connection.IsOpen)
                    {
                        _connection = await _factory.CreateConnectionAsync(cancellationToken);
                    }
                }
                finally
                {
                    _lock.Release();
                }
            }
            return await _connection.CreateChannelAsync(cancellationToken: cancellationToken);
        }

        public async ValueTask DisposeAsync()
        {
            if (!_disposed)
            {
                if (_connection != null)
                {
                    await _connection.CloseAsync();
                    await _connection.DisposeAsync();
                }
                _disposed = true;
            }
            GC.SuppressFinalize(this);
        }
    }
}