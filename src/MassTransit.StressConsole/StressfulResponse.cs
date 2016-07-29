using System;

namespace MassTransit.StressConsole
{
    public interface IStressfulResponse
    {
        Guid ResponseId { get; }

        DateTime Timestamp { get; }

        Guid RequestId { get; }
    }
}