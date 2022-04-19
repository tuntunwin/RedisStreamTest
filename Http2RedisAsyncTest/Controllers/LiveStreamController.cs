using Microsoft.AspNetCore.Mvc;

namespace Http2RedisAsyncTest.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class LiveStreamController : ControllerBase
    {

        private readonly ILogger<LiveStreamController> _logger;

        public LiveStreamController(ILogger<LiveStreamController> logger)
        {
            _logger = logger;
        }

        [HttpGet]
        public LiveStreamInfo Get(string cameraId)
        {
            return new LiveStreamInfo { CameraId = cameraId, StreamUrl = $"wss://server1.videogateway.com/{cameraId}" };
        }
    }
}