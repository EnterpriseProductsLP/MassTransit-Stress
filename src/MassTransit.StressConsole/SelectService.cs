using Topshelf;

namespace MassTransit.StressConsole
{
    public class SelectService : ServiceControl
    {
        private readonly ServiceControl _actualService;

        public SelectService(ServiceControl actualService)
        {
            _actualService = actualService;
        }

        public bool Start(HostControl hostControl)
        {
            return _actualService.Start(hostControl);
        }

        public bool Stop(HostControl hostControl)
        {
            return _actualService.Stop(hostControl);
        }
    }
}