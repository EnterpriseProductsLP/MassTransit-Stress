using System;
using System.Linq;
using System.Threading;

using log4net;
using log4net.Repository.Hierarchy;

using Magnum.Extensions;

using MassTransit.Log4NetIntegration.Logging;

using Topshelf;
using Topshelf.Logging;

namespace MassTransit.StressConsole
{
    internal class Program
    {
        private static int Main()
        {
            Log4NetLogWriterFactory.Use("log4net.config");
            Log4NetLogger.Use();

            var username = "test";
            var password = "test";
            var serviceBusUri = new Uri("rabbitmq://tsestmrmq01corp/loadtest_queue");
            ushort heartbeat = 3;
            var iterations = 10000;
            var instances = 10;
            var requestsPerInstance = 1;
            var messageSize = 128;
            var prefetchCount = 10;
            var consumerLimit = 10;
            var test = "stress";
            var cleanup = true;
            var mixed = false;
            string debug = null;

            int workerThreads;
            int completionPortThreads;
            ThreadPool.GetMinThreads(out workerThreads, out completionPortThreads);
            ThreadPool.SetMinThreads(Math.Max(workerThreads, 100), completionPortThreads);

            return (int)HostFactory.Run(
                hostConfigurator =>
                    {
                        hostConfigurator.SetDescription("Generates a stressful load on RabbitMQ using MassTransit");
                        hostConfigurator.SetDisplayName("MassTransit RabbitMQ Stress Console");
                        hostConfigurator.SetServiceName("MassTransitStressConsole");

                        hostConfigurator.AddCommandLineDefinition("rmqusername", v => username = v);
                        hostConfigurator.AddCommandLineDefinition("rmqpassword", v => password = v);
                        hostConfigurator.AddCommandLineDefinition("uri", v => serviceBusUri = new Uri(v));
                        hostConfigurator.AddCommandLineDefinition("heartbeat", v => heartbeat = ushort.Parse(v));
                        hostConfigurator.AddCommandLineDefinition("iterations", v => iterations = int.Parse(v));
                        hostConfigurator.AddCommandLineDefinition("instances", v => instances = int.Parse(v));
                        hostConfigurator.AddCommandLineDefinition("prefetch", v => prefetchCount = int.Parse(v));
                        hostConfigurator.AddCommandLineDefinition("threads", v => consumerLimit = int.Parse(v));
                        hostConfigurator.AddCommandLineDefinition("requests", v => requestsPerInstance = int.Parse(v));
                        hostConfigurator.AddCommandLineDefinition("test", v => test = v);
                        hostConfigurator.AddCommandLineDefinition("size", v => messageSize = int.Parse(v));
                        hostConfigurator.AddCommandLineDefinition("cleanup", v => cleanup = bool.Parse(v));
                        hostConfigurator.AddCommandLineDefinition("mixed", v => mixed = bool.Parse(v));
                        hostConfigurator.AddCommandLineDefinition("debug", v => debug = v);
                        hostConfigurator.ApplyCommandLine();

                        hostConfigurator.Service(
                            hostSettings =>
                                {
                                    if (!string.IsNullOrWhiteSpace(debug))
                                    {
                                        EnableDebug(debug);
                                    }

                                    if (test == "ingest")
                                    {
                                        return
                                            new SelectService(
                                                new StressIngestService(
                                                    serviceBusUri, 
                                                    username, 
                                                    password, 
                                                    heartbeat, 
                                                    iterations, 
                                                    instances, 
                                                    messageSize, 
                                                    cleanup, 
                                                    mixed, 
                                                    prefetchCount, 
                                                    consumerLimit));
                                    }

                                    return
                                        new SelectService(
                                            new StressService(
                                                serviceBusUri, 
                                                username, 
                                                password, 
                                                heartbeat, 
                                                iterations, 
                                                instances, 
                                                messageSize, 
                                                cleanup, 
                                                mixed, 
                                                prefetchCount, 
                                                consumerLimit, 
                                                requestsPerInstance));
                                });
                    });
        }

        private static void EnableDebug(string debug)
        {
            Console.WriteLine("Enabling debug for {0}", debug);

            var repositories = LogManager.GetAllRepositories();
            foreach (Hierarchy hierarchy in repositories)
            {
                var loggers = hierarchy.GetCurrentLoggers();
                foreach (Logger logger in loggers)
                {
                    if (string.Compare(logger.Name, debug, StringComparison.OrdinalIgnoreCase) == 0)
                    {
                        Console.WriteLine("Setting {0} to DEBUG", logger.Name);
                        logger.Level = hierarchy.LevelMap["DEBUG"];
                        return;
                    }
                }
            }

            Console.WriteLine("Logger not found, adding and updating");

            var log = (Logger)LogManager.GetLogger(debug);
            log.Level = repositories.First().CastAs<Hierarchy>().LevelMap["DEBUG"];
        }
    }
}