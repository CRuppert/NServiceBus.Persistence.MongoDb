using NServiceBus.Persistence.MongoDB.Repository;
using NServiceBus.Persistence.MongoDB.TimeoutPersistence;

namespace NServiceBus.Persistence.MongoDB.Configuration
{
    public static class ConfigureMongoDbTimeoutPersistence
    {
        public static Configure MongoDbTimeoutPersister(this Configure config)
        {
            if (!config.Configurer.HasComponent<MongoDbTimeoutRepository>())
                config.MongoDbPersistence();
            
            config.Configurer.ConfigureComponent<MongoDbTimeoutPersister>(DependencyLifecycle.SingleInstance);

            return config;
        }
    }
}