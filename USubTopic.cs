using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Collections.Generic;
using System.Collections.ObjectModel;

namespace ZeroMQ
{
    /// <summary>
    /// This subscriber receives all messages. It does not filter them and it does not trim topic header.
    /// </summary>
    public sealed class USubTopic
    {
        private readonly object clrZmqSubscriberSyncObj = new object();
        private ZSocket clrZmqSubscriber;
        private long clrZmqReceivedCounter = 0;

        public readonly ReadOnlyCollection<string> DataPubUrls;
        private readonly byte[] subscriptionTopic;

        private readonly object syncObj4Connect = new object();
        /// <summary>
        /// сигнал о том, что гейт был закрыт/остановлен пользователем и => не надо переподключаться
        /// </summary>
        public readonly ManualResetEvent gateStopped = new ManualResetEvent(false);

        /// <summary>Таймаут ожидания блокировки при установлении соединения (5 сек)</summary>
        private const int waitLockTimeout = 5000;

        private Thread readThread;

        public USubTopic(IList<string> dataPubUrls, byte[] topic)
        {
            if ((dataPubUrls == null) || (dataPubUrls.Count <= 0))
                throw new ArgumentNullException("dataPubUrls", "#1 Please, provide reasonable publisher URL i.e. 'tcp://*:54321'");
            for (int j = 0; j < dataPubUrls.Count; j++)
            {
                if (String.IsNullOrEmpty(dataPubUrls[j]))
                    throw new ArgumentNullException("dataPubUrls", "#3 Please, provide reasonable publisher URL i.e. 'tcp://*:54321'");
            }

            if (topic == null)
                subscriptionTopic = new byte[] { };
            else
            {
                subscriptionTopic = new byte[topic.Length];
                Array.Copy(topic, 0, subscriptionTopic, 0, topic.Length);
            }

            this.DataPubUrls = new ReadOnlyCollection<string>(new List<string>(dataPubUrls));

            ZSocket sub = PrepareSocketSub();

            lock (clrZmqSubscriberSyncObj)
            {
                clrZmqSubscriber = sub;
            }
        }

        /// <summary>
        /// Message counter (it counts full message as one, even if it has many frames)
        /// </summary>
        public long ClrZmqReceivedCounter { get { return clrZmqReceivedCounter; } }

        private static ZSocket PrepareSocketSub()
        {
            ZSocket sub = new ZSocket(ZContext.Current, ZSocketType.SUB);

            // Таймауты по 7 секунд позволят не зависать в блокирующих командах совсем уж навечно
            sub.SetOption(ZSocketOption.SNDTIMEO, 7000);
            sub.SetOption(ZSocketOption.RCVTIMEO, 7000);

            // Описание параметров TcpKeepAliveXXX:
            // http://api.zeromq.org/4-2:zmq-setsockopt
            sub.TcpKeepAlive = TcpKeepaliveBehaviour.Enable;
            sub.TcpKeepAliveIdle = 120; // seconds
            sub.TcpKeepAliveInterval = 30; // seconds
            sub.TcpKeepAliveCount = Int32.MaxValue;

            //Log.Information("[{Host}] pub.ReconnectInterval: {1}; pub.ReconnectIntervalMax: {2}",
            //    Host, pub.ReconnectInterval, pub.ReconnectIntervalMax);
            sub.ReconnectInterval = TimeSpan.FromSeconds(0.2);
            sub.ReconnectIntervalMax = TimeSpan.FromSeconds(60 /* Constants.ReconnectIntervalMaxSec */);
            //pub.LastEndpoint ???

            //Log.Warning("[{Host}] Ставлю таймаут на операцию ПОЛУЧЕНИЯ данных!", Host);
            //pub.ReceiveTimeout = TimeSpan.FromSeconds(17);

            //Log.Warning("[{Host}] Ставлю таймаут на операцию ОТПРАВКИ данных!", Host);
            sub.SendTimeout = TimeSpan.FromSeconds(7);
            sub.ReceiveTimeout = TimeSpan.FromSeconds(7);

            if (!sub.SetOption(ZSocketOption.RCVHWM, 1024))
            {
                //Log.Error("[{Host}] Не смог поменять RCVHWM для SUBSCRIBER???", Host);
            }
            int getHighWatermark;
            if (sub.GetOption(ZSocketOption.RCVHWM, out getHighWatermark))
            {
                //Log.Warning("[{Host}] RCVHWM option of SUBSCRIBER '{1}' is {2} now.", Host, nameof(clrZmqSubscriber), getHighWatermark);
            }

            // Допустим, <add key="DataPub" value="tcp://*:56888" />
            //Log.Information("[{Host}] И привязываю PUBLISHER к адресу '{1}'...", Host, dataPubUrl);
            //pub.Bind(dataPubUrl);

            return sub;
        }

        public void Start()
        {
            gateStopped.Reset();
            connect();
            //reconnectionTimer.Start();
        }

        public void Stop()
        {
            //reconnectionTimer.Stop();
            gateStopped.Set();
            Thread.Sleep(500);
            disconnect();
            Thread.Sleep(500);
        }

        //public void SetSupportedSections(IEnumerable<API.Shared.Entities.ExchangeSection> sections)
        //{
        //    supportedSections.Clear();

        //    foreach(var section in sections)
        //    {
        //        supportedSections[section.SectionCode] = section;
        //    }

        //    if (supportedSections.Count <= 0)
        //        throw new InvalidOperationException($"Поставщик ОБЯЗАН обслуживать хотя бы какие-то секции!");
        //}

        private bool connect()
        {
            bool res = false;

            if (Monitor.TryEnter(syncObj4Connect, waitLockTimeout))
            {
                try
                {
                    //nanoSubscriber.Connect(primaryUrl);
                    //nanoSubscriber.Subscribe(""); // All topics
                    //nanoSubscriber.Subscribe(new byte[] { });

                    for (int j = 0; j < DataPubUrls.Count; j++)
                    {
                        clrZmqSubscriber.Connect(DataPubUrls[j]);
                    }
                    //Log.Warning("[{Alias}] Connected to primaryUrl '{primaryUrl}'!", Alias, primaryUrl);

                    clrZmqSubscriber.Subscribe(subscriptionTopic);

                    //onDataPortalConnected();

                    if ((readThread == null) || (!readThread.IsAlive))
                    {
                        #region Ini readThread
                        readThread = new Thread(ReadThreadImpl);
                        readThread.Name = "Read";
                        readThread.Priority = ThreadPriority.Highest;
                        #endregion Ini readThread

                        readThread.Start();
                    }

                    res = true;
                }
                catch (ZException zex)
                {
                    // Invalid arguments and other errors must be reported
                    throw;
                }
                catch (System.Exception ex)
                {
                    //if (errorCounter < MaxErrors)
                    //    Log.Warning("[{Alias}] {exMessage}", Alias, ex.Message);
                    //else if (errorCounter % 20 == 0)
                    //{
                    //    Log.Warning("[{Alias}] Still trying to reconnect...", Alias);
                    //}

                    // Invalid arguments and other errors must be reported
                    throw;
                }
                //catch (Exception e)
                //{
                //    Log.Error(e);
                //}
                finally
                {
                    Monitor.Exit(syncObj4Connect);
                }
            }
            else
            {
                //Log.Warning("[{Alias}] Не удалось получить блокировку в методе connect()!", Alias);
            }

            return res;
        }

        /// <summary>
        /// Actual implementation of the reading thread. Must be started in readThread ONLY!
        /// </summary>
        private void ReadThreadImpl()
        {
            //Log.Error("[{Alias}] Starting execution of the most-important method '{1}'...", Alias, nameof(ReadThreadImpl));

            int sehExCounter = 0;
            while (!gateStopped.WaitOne(0, false))
            {
                try
                {
                    byte[] buf = null;
                    try
                    {
                        // Timeout is configured in milliseconds
                        ZError error;
                        using (ZFrame frame = clrZmqSubscriber.ReceiveFrame(out error))
                        {
                            if (frame != null)
                            {
                                buf = frame.Read();
                            }
                            else if (error.Number == ZError.EAGAIN.Number)
                            {
                                // Скорее всего приложение на той стороне выключено и теперь мы об этом знаем!
                                //Log.Warning("[{Alias}] Паблишер на той стороне выключен? num:{errorNumber}; name:{errorName}; text: {errorText}",
                                //    Alias, error.Number, error.Name, error.Text);

                                // TODO: сообщить в телегу об этом несчастье???

                                continue;
                            }
                            else
                            {
                                //Log.Warning("[{Alias}] Ошибка при чтении сообщения ZMQ? num:{errorNumber}; name:{errorName}; text: {errorText}",
                                //    Alias, error.Number, error.Name, error.Text);
                            }
                        }
                    }
                    catch (ZException zex)
                    {
                        if (sehExCounter > 1)
                        {
                            // Log.Error(zex, "[{Alias}] Исключение при чтении сообщения ZMQ.", Alias);
                         }

                        buf = null;
                        sehExCounter++;
                    }

                    if ((buf == null) || (buf.Length <= 0))
                        continue;

                    //string results = Encoding.ASCII.GetString(buf);
                    string results = Encoding.UTF8.GetString(buf);
                    //Log.DebugFormat("[{0}] {1}", Alias, results);

                    // DERIBIT;BTC-PERPETUAL;T;58297.5;3960.0;sell;2021-04-10 01:10:42.383374
                    // OKEX;BTC-USDT-210924;T;30079.9;2;B;6848201;2021-06-22 14:24:56.243000
                    // OKEX;BTC-USDT-SWAP;T;29741.2;2;S;88850270;2021-06-22 14:24:56.296000
                    //string[] split = results.Split(SplitChars, StringSplitOptions.RemoveEmptyEntries);
                    //if (split.Length < 3)
                    //    continue;
                    
                }
                catch (ThreadAbortException) { }
                catch (ThreadInterruptedException) { }
                catch (System.Exception ex)
                {
                    //Log.Error(ex, "Какое-то исключение в основном рабочем потоке.");
                }
            } // End while (!gateStopped.WaitOne(0, false))

            //Log.Error(new System.Exception(String.Format("[{0}] Execution of the method '{1}' is finished...", Alias, nameof(ReadThreadImpl))),
            //    "Завершение основного рабочего потока. Важное событие, не обязательно ошибка.");
        }

        private void disconnect()
        {
            if (clrZmqSubscriber != null)
            {
                gateStopped.Set();

                Thread.Sleep(3000);

                //Log.Warning("[{Alias}] Unsibscribing from '{primaryUrl}'...", Alias, primaryUrl);
                //clrZmqSubscriber.Unsubscribe(new byte[] { })
                clrZmqSubscriber.Unsubscribe(subscriptionTopic);

                //Log.Warning("[{Alias}] Disposing '{clrZmqSubscriber}'...", Alias, nameof(clrZmqSubscriber));
                clrZmqSubscriber.Dispose();
            }

            ZSocket sub = PrepareSocketSub();

            lock (clrZmqSubscriberSyncObj)
            {
                clrZmqSubscriber = sub;
            }

            //onDataPortalDisconnected();

            Thread.Sleep(300);
        }
    }
}
