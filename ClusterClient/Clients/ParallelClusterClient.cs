using System;
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
            var requests = ReplicaAddresses
                .Select
                    (
                        async add =>
                        {
                            var task = ProcessRequestAsync(CreateRequest($"{add}?query={query}"));
                            await Task.WhenAny(task, Task.Delay(timeout));
                            if (!task.IsCompleted)
                                throw new TimeoutException();
                            return task;
                        }
                    ).ToList();

            while (requests.Any())
            {
                var t = await Task.WhenAny(requests);
                requests.Remove(t);
                var r = await t;
                if(r.Status==TaskStatus.Faulted)
                    continue;
                return await r;
            }

            throw new TimeoutException();
        }

        protected override ILog Log => LogManager.GetLogger(typeof(ParallelClusterClient));
    }
}