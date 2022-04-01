using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Formats.Asn1;
using System.Linq;
using System.Threading.Tasks;
using ClusterClient.Clients;
using log4net;

namespace ClusterTests
{

    public class ParallelClusterClient : ClusterClientBase
    {
        public ParallelClusterClient(string[] replicaAddresses)
            : base(replicaAddresses)
        {
            
        }

        public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
        {
            var timer = new Stopwatch();
            var exceptions = new List<Exception>();
            timer.Start();
            var requests = ReplicaAddresses
                .Select(address => ProcessRequestAsync(CreateRequest($"{address}?query={query}")))
                .ToList();

            while (requests.Any())
            {
                var timeLimit = timeout - timer.Elapsed;
                if(timeLimit.TotalMilliseconds == 0)
                    break;
                var border = Task.Delay(timeLimit);
                await Task.WhenAny(new []{border}.Concat(requests));

                if(border.IsCompleted)
                    break;

                var fastRequest = await Task.WhenAny(requests);
                requests.Remove(fastRequest);
                
                if (fastRequest.IsCompletedSuccessfully)
                    return await fastRequest;

                exceptions.Add(fastRequest.Exception);
            }

            if (exceptions.Any())
            {
                exceptions.Add(new TimeoutException());
                throw new AggregateException(exceptions);
            }

            throw new TimeoutException();
        }

        protected override ILog Log => LogManager.GetLogger(typeof(ParallelClusterClient));
    }
}