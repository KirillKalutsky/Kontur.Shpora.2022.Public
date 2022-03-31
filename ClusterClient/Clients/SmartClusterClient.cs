using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using ClusterClient.Clients;
using log4net;

namespace ClusterTests
{

    public class SmartClusterClient : ClusterClientBase
    {
        private readonly Dictionary<string, List<long>> previous = new ();

        public SmartClusterClient(string[] replicaAddresses)
            : base(replicaAddresses)
        {
            
        }

        public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
        {
            List<Task<Task<string>>> requests = new();

            var dt = timeout / ReplicaAddresses.Length;

            IEnumerable<string> addresses;
            lock (previous)
                addresses = previous.OrderBy(x => x.Value.Sum() / x.Value.Count)
                    .Select(x => x.Key)
                    .Concat(ReplicaAddresses.Where(x => !previous.ContainsKey(x)));

            foreach (var add in addresses)
            {
                var webRequest = CreateRequest(add + "?query=" + query);

                var timer = new Stopwatch();
                timer.Start();
                var resultTask = ProcessRequestAsync(webRequest).ContinueWith(req =>
                {
                    timer.Stop();
                    lock (previous)
                    {
                        if (previous.ContainsKey(add))
                            previous[add].Add(timer.ElapsedMilliseconds);
                        else
                            previous[add] = new List<long> { timer.ElapsedMilliseconds };
                    }
                    return req;
                });

                requests.Add(resultTask);

                var fastRequest = Task.WhenAny(requests);
                await Task.WhenAny(fastRequest, Task.Delay(dt));

                if (fastRequest.IsCompleted)
                {
                    var task = await fastRequest;
                    requests.Remove(task);
                    if (task.Status != TaskStatus.Faulted)
                    {
                        var result = await task;
                        if (result.Status != TaskStatus.Faulted)
                            return await result;
                    }
                }
            }
            throw new TimeoutException();
        }

        protected override ILog Log => LogManager.GetLogger(typeof(SmartClusterClient));
    }
}