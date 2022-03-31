using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ClusterClient.Clients;
using log4net;

namespace ClusterTests
{

    public class RoundRobinClusterClient : ClusterClientBase
    {
        private readonly Dictionary<string, List<long>> previousAttempts = new();
        private readonly Dictionary<string, int> notWorkinh = new();
        private readonly Stopwatch timer = new();
        public RoundRobinClusterClient(string[] replicaAddresses)
            : base(replicaAddresses)
        {
            
        }

        public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
        {
            var dt = timeout / ReplicaAddresses.Length;
            IEnumerable<string> addresses;
           
            lock (previousAttempts)
            {
                addresses = previousAttempts.OrderBy(x => x.Value.Sum() / x.Value.Count)
                    .Select(x => x.Key)
                    .Concat(ReplicaAddresses.Where(x => !previousAttempts.ContainsKey(x)));
            }

            foreach (var add in addresses)
            {
                var webRequest = CreateRequest(add + "?query=" + query);

                Task<string> resultTask = ProcessRequestAsync(webRequest);

                timer.Start();
                await Task.WhenAny(resultTask, Task.Delay(dt));
                timer.Stop();

                var time = timer.ElapsedMilliseconds;
                if (!resultTask.IsCompleted || resultTask.Status == TaskStatus.Faulted)
                    time = (long) timeout.TotalMilliseconds;

                lock (previousAttempts)
                {
                    if (previousAttempts.ContainsKey(add)) 
                        previousAttempts[add].Add(time);
                    
                    else
                        previousAttempts[add] = new List<long> { time };
                }

                if (resultTask.IsCompleted && resultTask.Status != TaskStatus.Faulted)
                {
                    return await resultTask;
                }
                    
            }

            throw new TimeoutException();
        }

        protected override ILog Log => LogManager.GetLogger(typeof(RoundRobinClusterClient));
    }
}