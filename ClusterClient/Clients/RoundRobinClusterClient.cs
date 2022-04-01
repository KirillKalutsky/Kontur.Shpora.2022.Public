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
        private const int historyLength = 100;
        private readonly Dictionary<string, Queue<long>> previousAttempts;

        public RoundRobinClusterClient(string[] replicaAddresses)
            : base(replicaAddresses)
        {
            previousAttempts = new(replicaAddresses.Length);
        }

        public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
        {
            var taskTimer = new Stopwatch();
            var replicaCount = ReplicaAddresses.Length;
            var timeoutTimer = new Stopwatch();
            var exceptions = new List<Exception>();
            var addresses = GetOptimalReplicaOrder();

            timeoutTimer.Start();

            foreach (var address in addresses)
            {
                var timeLimit = (timeout - timeoutTimer.Elapsed) / replicaCount;
                if(timeLimit.TotalMilliseconds == 0)
                    break;
                replicaCount--;

                var webRequest = CreateRequest(address + "?query=" + query);
                var request = ProcessRequestAsync(webRequest);

                taskTimer.Start();
                await Task.WhenAny(request, Task.Delay(timeLimit));
                taskTimer.Stop();

                WriteTime(address, taskTimer.ElapsedMilliseconds);

                if (!request.IsCompleted)
                    continue;

                if (request.IsCompletedSuccessfully)
                    return await request;
                
                exceptions.Add(request.Exception);
            }

            if (exceptions.Any())
            {
                exceptions.Add(new TimeoutException());
                throw new AggregateException(exceptions);
            }

            throw new TimeoutException();
        }

        private void WriteTime(string address, long workTime)
        {
            lock (previousAttempts)
            {
                if (previousAttempts.ContainsKey(address))
                {
                    if(previousAttempts.Count < historyLength)
                        previousAttempts[address].Enqueue(workTime);
                    else
                    {
                        previousAttempts[address].Dequeue();
                        previousAttempts[address].Enqueue(workTime);
                    }
                }
                else
                {
                    var newHistory = new Queue<long>(historyLength);
                    newHistory.Enqueue(workTime);
                    previousAttempts[address] = newHistory;
                }
            }
        }

        private IEnumerable<string> GetOptimalReplicaOrder()
        {
            lock (previousAttempts)
            {
                return previousAttempts
                    .OrderBy(pair => pair.Value.Sum() / pair.Value.Count)
                    .Select(pair => pair.Key)
                    .Concat(ReplicaAddresses.Where(addr => !previousAttempts.ContainsKey(addr)));
            }
        }

        protected override ILog Log => LogManager.GetLogger(typeof(RoundRobinClusterClient));
    }
}