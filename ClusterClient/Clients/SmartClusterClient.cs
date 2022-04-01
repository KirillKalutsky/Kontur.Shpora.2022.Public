using System;
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
        private const int historyLength = 100;
        private readonly Dictionary<string, Queue<long>> previousAttempts;
        private readonly Dictionary<Task, Tuple<long,string>> startLog = new();

        public SmartClusterClient(string[] replicaAddresses)
            : base(replicaAddresses)
        {
            previousAttempts = new(replicaAddresses.Length);
        }

        public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
        {
            var exceptions = new List<Exception>();
            List<Task<string>> requests = new();
            var replicaCount = ReplicaAddresses.Length;
            var timeoutTimer = new Stopwatch();
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

                lock (startLog)
                    startLog[request] = Tuple.Create(timeoutTimer.ElapsedMilliseconds, address);
                requests.Add(request);

                var border = Task.Delay(timeLimit);
                await Task.WhenAny(new [] {border}.Concat(requests));

                if (border.IsCompleted)
                    continue;

                var fastRequest = await Task.WhenAny(requests);
                requests.Remove(fastRequest);

                WriteTime(fastRequest, timeoutTimer.ElapsedMilliseconds);

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


        private void WriteTime(Task fastRequest, long currentTime)
        {
            Tuple<long, string> taskValue;
            lock (startLog)
                taskValue = startLog[fastRequest];

            var address = taskValue.Item2;
            var workTime = currentTime - taskValue.Item1;

            lock (previousAttempts)
            {
                if (previousAttempts.ContainsKey(address))
                {
                    if (previousAttempts.Count < historyLength)
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

        protected override ILog Log => LogManager.GetLogger(typeof(SmartClusterClient));
    }
}