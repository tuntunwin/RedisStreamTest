// See https://aka.ms/new-console-template for more information
using RedisStreamTest;
using StackExchange.Redis;
using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;

var connectionMultiplexer = await ConnectionMultiplexer.ConnectAsync("localhost");
var redisDb = connectionMultiplexer.GetDatabase(0);

var tokenSource = new CancellationTokenSource();
var taskKind = Environment.GetCommandLineArgs()[1];
IDisposable task = null;

task = CreateTaskFromStreamWithPubSub(connectionMultiplexer, redisDb, "log").Subscribe(x =>
{
    Console.WriteLine(x);
});

using (task)
    {
    using (RunPublisher(redisDb, connectionMultiplexer.GetSubscriber()))
    {
        Console.ReadLine();
    }
}
    
static IObservable<string> CreateTaskFromStreamWithPubSub(ConnectionMultiplexer connection, IDatabase redisDb, string channel)
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
    var subscriber = connection.GetSubscriber();

    var pubsubObs = Observable.Create<string>(obsPubsub => {
        subscriber.Subscribe($"{channel}:notify", (ch, msg) =>
        {
            obsPubsub.OnNext(DateTime.Now.ToString());
        });
        return Disposable.Create(() => subscriber.Unsubscribe(channel));
    });
    
    var readStreamObs = Observable.Create<string>(obsReadstream => {
        
        return pubsubObs
         .ObserveLatestOn(Scheduler.CurrentThread)
        .Subscribe(msg =>
        {
            Task.Run(async () =>
            {
                var messages = await redisDb.StreamReadAsync(channel, lastReadMessage);

                foreach (var message in messages)
                {
                    obsReadstream.OnNext($"{message.Id} -> {message.Values[0].Name}: {message.Values[0].Value} / {message.Values[1].Name}: {message.Values[1].Value}");
                    lastReadMessage = message.Id;
                }
                Console.WriteLine($"Total Message {messages.Length}");
                await Task.Delay(TimeSpan.FromSeconds(3));
                redisDb.StringGetSet($"{channel}:lastReadMessage", lastReadMessage);
            }).Wait();
            
        });
    });
    return readStreamObs;
}


static IDisposable RunPublisher(IDatabase redisDb, ISubscriber publisher)
{
    return Observable.Interval(TimeSpan.FromSeconds(1)).Subscribe(x => {
        var value = new NameValueEntry[]
              {
                    new NameValueEntry("id", Guid.NewGuid().ToString()),
                    new NameValueEntry("timestamp", DateTime.UtcNow.ToString())
              };

        redisDb.StreamAdd("log", value);
        publisher.Publish("log:notify", string.Empty, CommandFlags.None);
    });

}

