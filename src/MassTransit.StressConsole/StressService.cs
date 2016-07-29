using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

using Magnum.Extensions;

using MassTransit.Transports.RabbitMq;

using RabbitMQ.Client.Exceptions;

using Taskell;

using Topshelf;
using Topshelf.Logging;

namespace MassTransit.StressConsole
{
    internal class StressService : ServiceControl
    {
        private readonly CancellationTokenSource _cancel;

        private readonly bool _cleanUp;

        private readonly IList<Task> _clientTasks;

        private readonly Uri _clientUri;

        private readonly int _consumerLimit;

        private readonly ushort _heartbeat;

        private readonly int _instances;

        private readonly int _iterations;

        private readonly LogWriter _log = HostLogger.Get<StressService>();

        private readonly string _messageContent;

        private readonly int _messageSize;

        private readonly bool _mixed;

        private readonly string _password;

        private readonly int _prefetchCount;

        private readonly int _requestsPerInstance;

        private readonly Uri _serviceBusUri;

        private readonly string _username;

        private int _currentHandlerCount;

        private Stopwatch _generatorStartTime;

        private HostControl _hostControl;

        private int _instanceCount;

        private int _maxHandlerCount;

        private int _mismatchedResponseCount;

        private int _requestCount;

        private int _responseCount;

        private long _responseTime;

        private IServiceBus _serviceBus;

        private int[][] _timings;

        private long _totalTime;

        public StressService(
            Uri serviceBusUri, 
            string username, 
            string password, 
            ushort heartbeat, 
            int iterations, 
            int instances, 
            int messageSize, 
            bool cleanUp, 
            bool mixed, 
            int prefetchCount, 
            int consumerLimit, 
            int requestsPerInstance)
        {
            _username = username;
            _password = password;
            _heartbeat = heartbeat;
            _iterations = iterations;
            _instances = instances;
            _messageSize = messageSize;
            _prefetchCount = prefetchCount;
            _consumerLimit = consumerLimit;
            _requestsPerInstance = requestsPerInstance;
            _cleanUp = cleanUp;
            _mixed = mixed;
            _serviceBusUri = serviceBusUri;
            _messageContent = new string('*', messageSize);

            _clientUri = _serviceBusUri;

            var prefetch = new Regex(@"([\?\&])prefetch=[^\&]+[\&]?");
            var query = _serviceBusUri.Query;

            if (query.IndexOf("prefetch", StringComparison.InvariantCultureIgnoreCase) >= 0)
            {
                query = prefetch.Replace(query, $"prefetch={this._prefetchCount}");
            }
            else if (string.IsNullOrEmpty(query))
            {
                query = $"prefetch={this._prefetchCount}";
            }
            else
            {
                query += $"&prefetch={this._prefetchCount}";
            }

            var builder = new UriBuilder(_serviceBusUri) { Query = query.Trim('?') };

            _serviceBusUri = builder.Uri;

            _cancel = new CancellationTokenSource();
            _clientTasks = new List<Task>();
        }

        public bool Start(HostControl hostControl)
        {
            _hostControl = hostControl;

            _log.InfoFormat("RabbitMQ Stress Test (using MassTransit)");
            _log.InfoFormat("Host: {0}", _serviceBusUri);
            _log.InfoFormat("Username: {0}", _username);
            _log.InfoFormat("Password: {0}", new String('*', _password.Length));
            _log.InfoFormat("Message Size: {0} {1}", _messageSize, _mixed ? "(mixed)" : "(fixed)");
            _log.InfoFormat("Iterations: {0}", _iterations);
            _log.InfoFormat("Clients: {0}", _instances);
            _log.InfoFormat("Requests Per Client: {0}", _requestsPerInstance);
            _log.InfoFormat("Heartbeat: {0}", _heartbeat);
            _log.InfoFormat("Consumer Limit: {0}", _consumerLimit);

            int workerThreads;
            int completionPortThreads;
            ThreadPool.GetMinThreads(out workerThreads, out completionPortThreads);
            var threads = workerThreads + (_instances * _requestsPerInstance + _consumerLimit);
            ThreadPool.SetMinThreads(threads, completionPortThreads);

            _log.InfoFormat("Setting minimum thread count: {0}", threads);

            _log.InfoFormat("Creating {0}", _serviceBusUri);

            _serviceBus = ServiceBusFactory.New(
                x =>
                    {
                        x.UseRabbitMq(
                            r =>
                                {
                                    r.ConfigureHost(
                                        _serviceBusUri, 
                                        h =>
                                            {
                                                h.SetUsername(_username);
                                                h.SetPassword(_password);
                                                h.SetRequestedHeartbeat(_heartbeat);
                                            });
                                });

                        x.ReceiveFrom(_serviceBusUri);
                        x.SetConcurrentConsumerLimit(_consumerLimit);

                        x.Subscribe(
                            s => s.Handler<IStressfulRequest>(
                                (context, message) =>
                                    {
                                        var currentHandlerCount = Interlocked.Increment(ref _currentHandlerCount);
                                        while (currentHandlerCount > _maxHandlerCount) Interlocked.CompareExchange(ref _maxHandlerCount, currentHandlerCount, _maxHandlerCount);

                                        // just respond with the Id
                                        context.Respond(new StressfulResponseMessage(message.RequestId));

                                        Interlocked.Decrement(ref _currentHandlerCount);
                                    }));
                    });

            _generatorStartTime = Stopwatch.StartNew();
            StartStressGenerators().Wait(_cancel.Token);

            return true;
        }

        public bool Stop(HostControl hostControl)
        {
            var wait = Task.WaitAll(_clientTasks.ToArray(), (_iterations * _instances / 100).Seconds());
            if (wait)
            {
                _generatorStartTime.Stop();

                _log.InfoFormat("RabbitMQ Stress Test Completed");
                _log.InfoFormat("Request Count: {0}", _requestCount);
                _log.InfoFormat("Response Count: {0}", _responseCount);
                _log.InfoFormat("Max Handler Count: {0}", _maxHandlerCount);

                if (_mismatchedResponseCount > 0)
                {
                    _log.ErrorFormat("Mismatched Response Count: {0}", _mismatchedResponseCount);
                }

                _log.InfoFormat("Average Resp Time: {0}ms", _responseTime / _responseCount);

                _log.InfoFormat("Max Response Time: {0}ms", _timings.SelectMany(x => x).Max());
                _log.InfoFormat("Min Response Time: {0}ms", _timings.SelectMany(x => x).Min());
                _log.InfoFormat("Med Response Time: {0}ms", (int?)_timings.SelectMany(x => x).Median());
                _log.InfoFormat("95% Response Time: {0}ms", (int?)_timings.SelectMany(x => x).Percentile(95));

                _log.InfoFormat("Elapsed Test Time: {0}", _generatorStartTime.Elapsed);
                _log.InfoFormat("Total Client Time: {0}ms", _totalTime);
                _log.InfoFormat("Per Client Time: {0}ms", _totalTime / _instances);
                _log.InfoFormat("Message Throughput: {0}m/s", (_requestCount + _responseCount) * 1000 / (_totalTime / _instances));

                DrawResponseTimeGraph();
            }

            _cancel.Cancel();

            if (_serviceBus != null)
            {
                _serviceBus.Dispose();
                _serviceBus = null;
            }

            if (_cleanUp)
            {
                CleanUpQueuesAndExchanges();
            }

            return wait;
        }

        private void CleanUpQueuesAndExchanges()
        {
            var address = RabbitMqEndpointAddress.Parse(_serviceBusUri);
            var connectionFactory = address.ConnectionFactory;
            if (string.IsNullOrWhiteSpace(connectionFactory.UserName))
            {
                connectionFactory.UserName = "test";
            }

            if (string.IsNullOrWhiteSpace(connectionFactory.Password))
            {
                connectionFactory.Password = "test";
            }

            using (var connection = connectionFactory.CreateConnection())
            {
                using (var model = connection.CreateModel())
                {
                    model.ExchangeDelete(address.Name);
                    model.QueueDelete(address.Name);

                    for (var i = 0; i < 10000; i++)
                    {
                        var name = $"{address.Name}_client_{i}";
                        try
                        {
                            model.QueueDeclarePassive(name);
                        }
                        catch (OperationInterruptedException)
                        {
                            break;
                        }

                        model.ExchangeDelete(name);
                        model.QueueDelete(name);
                    }
                }
            }
        }

        private void DrawResponseTimeGraph()
        {
            var maxTime = _timings.SelectMany(x => x).Max();
            var minTime = _timings.SelectMany(x => x).Min();

            const int Segments = 10;

            var span = maxTime - minTime;
            var increment = span / Segments;

            var histogram = (from x in _timings.SelectMany(x => x)
                             let key = (x - minTime) * Segments / span
                             where key >= 0 && key < Segments
                             let groupKey = key
                             group x by groupKey
                             into segment
                             orderby segment.Key
                             select new { Value = segment.Key, Count = segment.Count() }).ToList();

            var maxCount = histogram.Max(x => x.Count);

            foreach (var item in histogram)
            {
                var barLength = item.Count * 60 / maxCount;
                _log.InfoFormat("{0,5}ms {2,-60} ({1,7})", minTime + increment * item.Value, item.Count, new string('*', barLength));
            }
        }

        private async Task StartStressGenerators()
        {
            var start = new TaskCompletionSource<bool>();

            var starting = new List<Task>();
            _timings = new int[_instances][];
            for (var i = 0; i < _instances; i++)
            {
                _timings[i] = new int[_requestsPerInstance * _iterations];
                starting.Add(StartStressGenerator(i, start.Task));
            }

            await Task.WhenAll(starting.ToArray());

            start.TrySetResult(true);
        }

        private Task StartStressGenerator(int instance, Task start)
        {
            var ready = new TaskCompletionSource<bool>();

            var composer = new TaskComposer<bool>(_cancel.Token, false);

            var endpointAddress = _serviceBus.Endpoint.Address as IRabbitMqEndpointAddress;
            var queueName = $"{endpointAddress.Name}_client_{instance}";
            var uri = RabbitMqEndpointAddress.Parse(_clientUri).ForQueue(queueName).Uri;

            var uriBuilder = new UriBuilder(uri);
            uriBuilder.Query = _clientUri.Query.Trim('?');

            var address = uriBuilder.Uri;

            composer.Execute(() => { Interlocked.Increment(ref _instanceCount); });

            IServiceBus bus = null;
            composer.Execute(
                () =>
                    {
                        _log.InfoFormat("Creating {0}", address);

                        bus = ServiceBusFactory.New(
                            x =>
                                {
                                    x.UseRabbitMq(
                                        r =>
                                            {
                                                r.ConfigureHost(
                                                    address, 
                                                    h =>
                                                        {
                                                            h.SetUsername(_username);
                                                            h.SetPassword(_password);
                                                            h.SetRequestedHeartbeat(_heartbeat);
                                                        });
                                            });

                                    x.ReceiveFrom(address);
                                });
                    }, 
                false);

            Stopwatch clientTimer = null;

            composer.Execute(
                () =>
                    {
                        ready.TrySetResult(true);
                        return start;
                    });

            composer.Execute(() => clientTimer = Stopwatch.StartNew());

            for (var requestClient = 0; requestClient < _requestsPerInstance; requestClient++)
            {
                var clientIndex = requestClient;

                composer.Execute(
                    () =>
                        {
                            var task = composer.Compose(
                                x =>
                                    {
                                        for (var i = 0; i < _iterations; i++)
                                        {
                                            var iteration = i;
                                            x.Execute(
                                                () =>
                                                    {
                                                        var messageContent = _mixed && iteration % 2 == 0 ? new string('*', 128) : _messageContent;
                                                        var requestMessage = new StressfulRequestMessage(messageContent);

                                                        var taskRequest = bus.PublishRequestAsync<IStressfulRequest>(
                                                            requestMessage, 
                                                            r =>
                                                                {
                                                                    r.Handle<IStressfulResponse>(
                                                                        response =>
                                                                            {
                                                                                Interlocked.Increment(ref _responseCount);

                                                                                var timeSpan = response.Timestamp - requestMessage.Timestamp;
                                                                                Interlocked.Add(ref _responseTime, (long)timeSpan.TotalMilliseconds);
                                                                                _timings[instance][clientIndex * _iterations + iteration] =
                                                                                    (int)timeSpan.TotalMilliseconds;

                                                                                if (response.RequestId != requestMessage.RequestId)
                                                                                {
                                                                                    Interlocked.Increment(ref _mismatchedResponseCount);
                                                                                }
                                                                            });
                                                                });

                                                        Interlocked.Increment(ref _requestCount);

                                                        return taskRequest.Task;
                                                    });
                                        }
                                    });

                            return task;
                        });
            }

            composer.Execute(() => clientTimer.Stop());

            composer.Execute(() => bus.Dispose(), false);

            composer.Compensate(compensation => { return compensation.Handled(); });

            composer.Finally(
                status =>
                    {
                        Interlocked.Add(ref _totalTime, clientTimer.ElapsedMilliseconds);
                        var count = Interlocked.Decrement(ref _instanceCount);
                        if (count == 0)
                        {
                            Task.Factory.StartNew(() => _hostControl.Stop());
                        }
                    }, 
                false);

            _clientTasks.Add(composer.Finish());

            return ready.Task;
        }

        private class StressfulRequestMessage : IStressfulRequest
        {
            public StressfulRequestMessage(string content)
            {
                RequestId = NewId.NextGuid();
                Timestamp = DateTime.UtcNow;
                Content = content;
            }

            public Guid RequestId { get; private set; }

            public DateTime Timestamp { get; private set; }

            public string Content { get; private set; }
        }

        private class StressfulResponseMessage : IStressfulResponse
        {
            public StressfulResponseMessage(Guid requestId)
            {
                ResponseId = NewId.NextGuid();
                Timestamp = DateTime.UtcNow;

                RequestId = requestId;
            }

            public Guid ResponseId { get; private set; }

            public DateTime Timestamp { get; private set; }

            public Guid RequestId { get; private set; }
        }
    }
}