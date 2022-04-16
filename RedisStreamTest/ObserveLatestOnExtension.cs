using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive;
using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RedisStreamTest
{
    static class ObserveLatestOnExtension
    {
        public static IObservable<T> ObserveLatestOn<T>(this IObservable<T> source, IScheduler scheduler)
        {
            return Observable.Create<T>(observer =>
            {
                Notification<T> outsideNotification = null;
                var gate = new object();
                bool active = false;
                var cancelable = new MultipleAssignmentDisposable();
                var disposable = source.Materialize().Subscribe(thisNotification =>
                {
                    bool alreadyActive;
                    lock (gate)
                    {
                        alreadyActive = active;
                        active = true;
                        outsideNotification = thisNotification;
                    }

                    if (!alreadyActive)
                    {
                        cancelable.Disposable = scheduler.Schedule(self =>
                        {
                            Notification<T> localNotification = null;
                            lock (gate)
                            {
                                localNotification = outsideNotification;
                                outsideNotification = null;
                            }
                            localNotification.Accept(observer);
                            bool hasPendingNotification = false;
                            lock (gate)
                            {
                                hasPendingNotification = active = (outsideNotification != null);
                            }
                            if (hasPendingNotification)
                            {
                                self();
                            }
                        });
                    }
                });
                return new CompositeDisposable(disposable, cancelable);
            });
        }
        public static IObservable<TSource> ObserveLatestOn2<TSource>(
    this IObservable<TSource> source,
    IScheduler scheduler)
        {
            return Observable.Create<TSource>(observer =>
            {
                Notification<TSource> pendingNotification = null;
                var cancelable = new MultipleAssignmentDisposable();

                var sourceSubscription = source.Materialize()
                    .Subscribe(notification =>
                    {
                        var previousNotification = Interlocked.Exchange(
                            ref pendingNotification, notification);

                        if (previousNotification != null) return;

                        cancelable.Disposable = scheduler.Schedule(() =>
                        {
                            var notificationToSend = Interlocked.Exchange(
                        ref pendingNotification, null);
                            notificationToSend.Accept(observer);
                        });
                    });
                return new CompositeDisposable(sourceSubscription, cancelable);
            });
        }
    }
}
