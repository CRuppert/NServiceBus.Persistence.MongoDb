using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Security.Policy;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Driver;
using MongoDB.Driver.Linq;
using NServiceBus.Logging;
using NServiceBus.Persistence.MongoDB.Repository;
using NServiceBus.Timeout.Core;

namespace NServiceBus.Persistence.MongoDB.TimeoutPersistence
{
    public class MongoDbTimeoutPersister : IPersistTimeouts
    {
        //inspired by the ravendb persister

        private Configure config;
        static readonly ILog Logger = LogManager.GetLogger(typeof(MongoDbTimeoutPersister));
        public string EndpointName { get; set; }
        public TimeSpan CleanupGapFromTimeSliceDuration { get; set; }
        public TimeSpan TriggerCleanupEveryDuration { get; set; }

        private DateTime lastCleanupTime = DateTime.MinValue;

        public MongoDbTimeoutRepository Repository { get; set; }

        public MongoDbTimeoutPersister(Configure config)
        {
            this.config = config;
            this.TriggerCleanupEveryDuration = TimeSpan.FromMinutes(2);
            this.CleanupGapFromTimeSliceDuration = TimeSpan.FromMinutes(1);
        }

        public List<Tuple<string, DateTime>> GetNextChunk(DateTime startSlice, out DateTime nextTimeToRunQuery)
        {
            var now = DateTime.UtcNow;
            List<Tuple<string, DateTime>> results;

            if (lastCleanupTime == DateTime.MinValue || lastCleanupTime.Add(TriggerCleanupEveryDuration) > now)
            {
                //cleanup for old timeouts that may have been missed
                results = GetCleanupChunk(startSlice).ToList();
            }
            else
            {
                results = new List<Tuple<string, DateTime>>();
            }
            nextTimeToRunQuery = DateTime.UtcNow.AddMinutes(10);

            
            var q = GetChunkQuery()
                .Where(t => t.Time > startSlice)
                .Select(t => new {t.Id, t.Time});

            //ravendb has a nice stream model we can use. we have to resort to a simple toList for now
            results.AddRange(from result in q.ToList() let dt = result.Time select new Tuple<string, DateTime>(result.Id, dt));

            if (results.Count == 0)
            {
                nextTimeToRunQuery = now;
            }
            else
            {
                nextTimeToRunQuery = results.Max(t => t.Item2);
            }
            return results;
        }

        private IQueryable<TimeoutData> GetChunkQuery()
        {
            return GetCollection().AsQueryable()
                .Where(
                    t =>
                        t.OwningTimeoutManager == String.Empty ||
                        t.OwningTimeoutManager == EndpointName)
                .OrderBy(t => t.Time);

        }

        private MongoCollection<TimeoutData> GetCollection()
        {
            return Repository.GetCollection();
        }
        private IEnumerable<Tuple<string, DateTime>> GetCleanupChunk(DateTime startSlice)
        {
            //query mongo for
            var chunk = GetChunkQuery().Where(t => t.Time <= startSlice.Subtract(CleanupGapFromTimeSliceDuration))
                        .Select(t => new {t.Id, t.Time})
                        .Take(1024)
                        .ToList().Select(arg => new Tuple<string, DateTime>(arg.Id, arg.Time));
            lastCleanupTime = DateTime.UtcNow;
            return chunk;
        }

        public void Add(TimeoutData timeout)
        {
            Repository.Insert(timeout);
        }


        public bool TryRemove(string timeoutId, out TimeoutData timeoutData)
        {
            return Repository.TryRemove(timeoutId, out timeoutData);
        }

        public void RemoveTimeoutBy(Guid sagaId)
        {
            Repository.RemoveForSaga(sagaId);
        }
    }
}
