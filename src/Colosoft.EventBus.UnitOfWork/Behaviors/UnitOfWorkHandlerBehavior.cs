using Microsoft.Extensions.Logging;

namespace Colosoft.EventBus.Behaviors
{
    public class UnitOfWorkHandlerBehavior<TIntegrationEvent> : IPipelineHandlerBehavior<TIntegrationEvent>
    {
        protected IUnitOfWorkProvider UnitOfWorkProvider { get; }
        private readonly ILogger<UnitOfWorkHandlerBehavior<TIntegrationEvent>> logger;

        public UnitOfWorkHandlerBehavior(
            IUnitOfWorkProvider unitOfWorkProvider,
            ILogger<UnitOfWorkHandlerBehavior<TIntegrationEvent>> logger)
        {
            this.UnitOfWorkProvider = unitOfWorkProvider ?? throw new ArgumentNullException(nameof(unitOfWorkProvider));
            this.logger = logger ?? throw new ArgumentException(nameof(ILogger));
        }

        protected virtual Task PublishEventsThroughEventBusAsync(Guid transactionId, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        protected virtual IUnitOfWork CreateUnitOfWork()
        {
            return this.UnitOfWorkProvider.Create();
        }

        public async Task Handle(TIntegrationEvent @event, IIntegrationEventContext context, IntegrationEventHandlerDelegate next, CancellationToken cancellationToken)
        {
            var typeName = @event.GetGenericTypeName();

            try
            {
                if (this.UnitOfWorkProvider.GetCurrent() != null)
                {
                    await next(cancellationToken);
                }

                using (var unitOfWork = this.CreateUnitOfWork())
                {
                    var transactionId = unitOfWork.Id;

                    try
                    {
                        using (this.logger.BeginScope(new List<Tuple<string, object>> { new Tuple<string, object>("TransactionContext", transactionId) }))
                        {
                            this.logger.LogInformation("Begin transaction {TransactionId} for {CommandName} ({@Command})", transactionId, typeName, @event);

                            await next(cancellationToken);

                            this.logger.LogInformation("Commit transaction {TransactionId} for {CommandName}", transactionId, typeName);

                            await unitOfWork.CommitAsync(cancellationToken);
                        }
                    }
                    catch
                    {
                        await unitOfWork.RollbackAsync(cancellationToken);
                        throw;
                    }

                    await this.PublishEventsThroughEventBusAsync(transactionId, cancellationToken);
                }
            }
            catch (Exception ex)
            {
                this.logger.LogError(ex, "Error Handling transaction for {CommandName} ({@Command})", typeName, @event);

                throw;
            }
        }
    }
}
