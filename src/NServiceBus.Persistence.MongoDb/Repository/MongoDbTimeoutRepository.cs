using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.IdGenerators;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using NServiceBus.Timeout.Core;

namespace NServiceBus.Persistence.MongoDB.Repository
{
    public class MongoDbTimeoutRepository
    {
        private readonly MongoDatabase _db;
        public MongoDbTimeoutRepository(MongoDatabase db)
        {
            _db = db;
        }
        public MongoCollection<TimeoutData> GetCollection()
        {
            return _db.GetCollection<TimeoutData>("timeoutDataStorage");
        }

        public void Insert(TimeoutData timeout)
        {
            var c = GetCollection();
            c.Insert(timeout);
        }

        public bool TryRemove(string timeoutId, out TimeoutData removedData)
        {
            var q = Query<TimeoutData>.EQ(t => t.Id, timeoutId);


            var res = GetCollection().FindAndRemove(new FindAndRemoveArgs() {Query = q});
            removedData = res.GetModifiedDocumentAs<TimeoutData>();

            return res.ModifiedDocument != null;
        }

        public void RemoveForSaga(Guid sagaId)
        {
            var q = Query<TimeoutData>.EQ(t => t.SagaId, sagaId);
            GetCollection().Remove(q);
        }
    }

    public static class RegisterTimeoutDataClassMap
    {
        public static void Register()
        {
            if (!BsonClassMap.IsClassMapRegistered(typeof (TimeoutData)))
            {
                BsonClassMap.RegisterClassMap<TimeoutData>(cm =>
                {
                    cm.AutoMap();
                    cm.SetIdMember(cm.GetMemberMap(c=> c.Id));
                    cm.IdMemberMap.SetIdGenerator(new ObjectIdGenerator());
                });
            }
        }
    }
}
