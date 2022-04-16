// See https://aka.ms/new-console-template for more information
using StackExchange.Redis;
using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Reactive.Linq;

var connectionMultiplexer = await ConnectionMultiplexer.ConnectAsync("localhost");
var redisDb = connectionMultiplexer.GetDatabase(0);

var tokenSource = new CancellationTokenSource();
var taskKind = Environment.GetCommandLineArgs()[1];
IDisposable task = null;
if (taskKind == "polling") {
    task = CreateTaskFromStreamWithPolling(connectionMultiplexer, redisDb, "log", tokenSource.Token).Subscribe(x =>
    {
        Console.WriteLine(x);
    });
}
else {
    task = CreateTaskFromStreamWithPubSub(connectionMultiplexer, redisDb, "log").Subscribe(x =>
    {
        Console.WriteLine(x);
    });
}
using (task)
    {
    using (RunPublisher(redisDb, connectionMultiplexer.GetSubscriber()))
    {
        Console.ReadLine();
    }
    //Console.ReadLine();
}
    
static IDisposable RunPublisher(IDatabase redisDb, ISubscriber publisher) {
    return Observable.Interval(TimeSpan.FromSeconds(2)).Subscribe(x => {
        var value = new NameValueEntry[]
              {
                    new NameValueEntry("id", Guid.NewGuid().ToString()),
                    new NameValueEntry("timestamp", DateTime.UtcNow.ToString())
              };

        redisDb.StreamAdd("log", value);
        publisher.Publish("log:notify", string.Empty, CommandFlags.None);
    });

}


static IObservable<string> CreateTaskFromStreamWithPubSub(ConnectionMultiplexer connection, IDatabase redisDb, string channel)
{
    var lastReadMessage = "0-0";
    SemaphoreSlim taskFromStreamBlocker = new SemaphoreSlim(1);
    var lastReadMessageData = redisDb.StringGet($"{channel}:lastReadMessage", CommandFlags.None);
    if (string.IsNullOrEmpty(lastReadMessageData))
    {
        redisDb.StringGetSet($"{channel}:lastReadMessage", lastReadMessage);
    }
    else
    {
        lastReadMessage = lastReadMessageData;
    }


    return Observable.Create<string>(obs =>
    {
        var subscriber = connection.GetSubscriber();
        subscriber.Subscribe($"{channel}:notify", async (ch, msg) =>
        {
            var locker = await taskFromStreamBlocker
                .WaitAsync(0)
                .ConfigureAwait(false);

            if (!locker)
            {
                return;
            }

            var messages = await redisDb.StreamReadAsync(channel, lastReadMessage);

            foreach (var message in messages)
            {
                obs.OnNext($"{message.Id} -> {message.Values[0].Name}: {message.Values[0].Value} / {message.Values[1].Name}: {message.Values[1].Value}");
                lastReadMessage = message.Id;
            }

            //redisDb.KeyDelete($"{channel}:lastReadMessage");
            redisDb.StringGetSet($"{channel}:lastReadMessage", lastReadMessage);

            taskFromStreamBlocker.Release();
        });

        return Disposable.Create(() => subscriber.Unsubscribe(channel));
    });
}

static IObservable<string> CreateTaskFromStreamWithPolling(ConnectionMultiplexer connection, IDatabase redisDb, string channel, CancellationToken cancellationToken)
{
    var lastReadMessage = "0-0";

    var lastReadMessageData = redisDb.StringGet($"{channel}:lastReadMessage", CommandFlags.None);
    if (string.IsNullOrEmpty(lastReadMessageData))
    {
        redisDb.StringGetSet($"{channel}:lastReadMessage", lastReadMessage);
    }
    else
    {
        lastReadMessage = lastReadMessageData;
    }

    var instance = ThreadPoolScheduler.Instance;

    return Observable.Create<string>(obs =>
    {
        var disposable = Observable
            .Interval(TimeSpan.FromMilliseconds(200), instance)
            .Subscribe(async _ =>
            {
                var messages = await redisDb.StreamReadAsync(channel, lastReadMessage);

                foreach (var message in messages)
                {
                    obs.OnNext($"{message.Id} -> {message.Values[0].Name}: {message.Values[0].Value} / {message.Values[1].Name}: {message.Values[1].Value}");
                    lastReadMessage = message.Id;
                }
                Console.WriteLine($"{DateTime.Now} {messages.Length} message received");
                //redisDb.KeyDelete($"{channel}:lastReadMessage");
                redisDb.StringGetSet($"{channel}:lastReadMessage", lastReadMessage);
            });
        cancellationToken.Register(() => disposable.Dispose());

        return Disposable.Empty;
    });
}
