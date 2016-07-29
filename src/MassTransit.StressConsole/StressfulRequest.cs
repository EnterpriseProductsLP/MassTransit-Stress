using System;

namespace MassTransit.StressConsole
{
    public interface IStressfulRequest
    {
        Guid RequestId { get; }

        DateTime Timestamp { get; }

        string Content { get; }
    }
}