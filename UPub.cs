using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Collections.Generic;
using System.Diagnostics.Contracts;

namespace ZeroMQ
{
    public class UPub
    {
        protected readonly object clrZmqPublisherSyncObj = new object();
        protected ZSocket clrZmqPublisher;

        /// <summary>
        /// Message counter (it counts full message as one, even if it has many frames)
        /// </summary>
        protected long clrZmqPublishCounter = 0;

        public readonly string DataPubUrl;

        public UPub(string dataPubUrl)
        {
            if (String.IsNullOrWhiteSpace(dataPubUrl))
                throw new ArgumentNullException("dataPubUrl", "Please, provide reasonable publisher URL i.e. 'tcp://*:54321'");

            this.DataPubUrl = dataPubUrl;

            ZSocket pub = new ZSocket(ZContext.Current, ZSocketType.PUB);

            // Таймауты по 7 секунд позволят не зависать в блокирующих командах совсем уж навечно
            pub.SetOption(ZSocketOption.SNDTIMEO, 7000);
            pub.SetOption(ZSocketOption.RCVTIMEO, 7000);

            // Описание параметров TcpKeepAliveXXX:
            // http://api.zeromq.org/4-2:zmq-setsockopt
            pub.TcpKeepAlive = TcpKeepaliveBehaviour.Enable;
            pub.TcpKeepAliveIdle = 120; // seconds
            pub.TcpKeepAliveInterval = 30; // seconds
            pub.TcpKeepAliveCount = Int32.MaxValue;

            //Log.Information("[{Host}] pub.ReconnectInterval: {1}; pub.ReconnectIntervalMax: {2}",
            //    Host, pub.ReconnectInterval, pub.ReconnectIntervalMax);
            pub.ReconnectInterval = TimeSpan.FromSeconds(0.2);
            pub.ReconnectIntervalMax = TimeSpan.FromSeconds(60 /* Constants.ReconnectIntervalMaxSec */);
            //pub.LastEndpoint ???

            //Log.Warning("[{Host}] Ставлю таймаут на операцию ПОЛУЧЕНИЯ данных!", Host);
            //pub.ReceiveTimeout = TimeSpan.FromSeconds(17);

            //Log.Warning("[{Host}] Ставлю таймаут на операцию ОТПРАВКИ данных!", Host);
            pub.SendTimeout = TimeSpan.FromSeconds(7);
            pub.ReceiveTimeout = TimeSpan.FromSeconds(7);

            if (!pub.SetOption(ZSocketOption.SNDHWM, 1024))
            {
                //Log.Error("[{Host}] Не смог поменять SNDHWM для PUBLISHER???", Host);
            }
            int sendHighWatermark;
            if (pub.GetOption(ZSocketOption.SNDHWM, out sendHighWatermark))
            {
                //Log.Warning("[{Host}] SNDHWM option of PUBLISHER '{1}' is {2} now.", Host, nameof(clrZmqPublisher), sendHighWatermark);
            }

            // Допустим, <add key="DataPub" value="tcp://*:56888" />
            //Log.Information("[{Host}] И привязываю PUBLISHER к адресу '{1}'...", Host, dataPubUrl);
            pub.Bind(dataPubUrl);

            lock (clrZmqPublisherSyncObj)
            {
                clrZmqPublisher = pub;
            }
        }

        /// <summary>
        /// Message counter (it counts full message as one, even if it has many frames)
        /// </summary>
        public long ClrZmqPublishCounter { get { return clrZmqPublishCounter; } }

        #region Send single 'Frame'
        public bool SendBytes(byte[] frameBuf, int offset, int count, ref ZError error /* , out Exception zeroEx */)
        {
            //zeroEx = null;
            //Contract.Assert(clrZmqPublisher != null, "This field must be set in ctor.");
            if (clrZmqPublisher == null)
                return false;

            //try
            {
                lock (clrZmqPublisherSyncObj)
                {
                    // Дилеру не надо париться с идентификаторами. Удобно.
                    if (clrZmqPublisher.SendBytes(frameBuf, offset, count, ZSocketFlags.DontWait, out error))
                    {
                        clrZmqPublishCounter++;
                        return true;
                    }
                }
            }
            //catch (Exception evEx)
            //{
            //    zeroEx = evEx;
            //    //Log.Error(evEx, "[{Host}] Обработчик события OnBarClosed сгенерировал исключение? Нехорошо.", Host);
            //}

            return false;
        }

        public bool SendUnsafe(byte[] frameBuf, int count, ref ZError error /* , out Exception zeroEx */)
        {
            //zeroEx = null;
            //Contract.Assert(clrZmqPublisher != null, "This field must be set in ctor.");
            if (clrZmqPublisher == null)
                return false;

            //try
            {
                lock (clrZmqPublisherSyncObj)
                {
                    // Дилеру не надо париться с идентификаторами. Удобно.
                    if (clrZmqPublisher.SendBytesUnsafe(frameBuf, count, ZSocketFlags.DontWait, out error))
                    {
                        clrZmqPublishCounter++;
                        return true;
                    }
                }
            }
            //catch (Exception evEx)
            //{
            //    zeroEx = evEx;
            //    //Log.Error(evEx, "[{Host}] Обработчик события OnBarClosed сгенерировал исключение? Нехорошо.", Host);
            //}

            return false;
        }

        public bool SendUms(UnmanagedMemoryStream frameBuf, int count, ref ZError error /* , out Exception zeroEx */)
        {
            //zeroEx = null;
            //Contract.Assert(clrZmqPublisher != null, "This field must be set in ctor.");
            if (clrZmqPublisher == null)
                return false;

            //try
            {
                lock (clrZmqPublisherSyncObj)
                {
                    // Дилеру не надо париться с идентификаторами. Удобно.
                    if (clrZmqPublisher.SendUms(frameBuf, count, ZSocketFlags.DontWait, out error))
                    {
                        clrZmqPublishCounter++;
                        return true;
                    }
                }
            }
            //catch (Exception evEx)
            //{
            //    zeroEx = evEx;
            //    //Log.Error(evEx, "[{Host}] Обработчик события OnBarClosed сгенерировал исключение? Нехорошо.", Host);
            //}

            return false;
        }
        #endregion Send single 'Frame'

        #region Send message of many 'Frames'
        public bool SendBytes(List<byte[]> frames, List<int> counts, ref ZError error /*, out Exception zeroEx */)
        {
            //zeroEx = null;
            //Contract.Assert(clrZmqPublisher != null, "This field must be set in ctor.");
            if (clrZmqPublisher == null)
                throw new InvalidOperationException("How have you managed to call this method before end of .ctor or after dispose?");

            //try
            {
                lock (clrZmqPublisherSyncObj)
                {
                    for (int j = 0, len = frames.Count; j < len; j++)
                    {
                        byte[] frameBuf = frames[j];
                        ZSocketFlags flags = (j < len - 1) ? (ZSocketFlags.DontWait | ZSocketFlags.More) : ZSocketFlags.DontWait;
                        // Дилеру не надо париться с идентификаторами. Удобно.
                        if (clrZmqPublisher.SendBytes(frameBuf, 0, counts[j], flags, out error))
                        {
                            //clrZmqPublishCounter++;
                        }
                        else
                            return false;
                    }

                    // If all frames were sent, then report success.
                    clrZmqPublishCounter++;
                    return true;
                }
            }
            //catch (Exception evEx)
            //{
            //    zeroEx = evEx;
            //    //Log.Error(evEx, "[{Host}] Обработчик события OnBarClosed сгенерировал исключение? Нехорошо.", Host);
            //}

            //return false;
        }

        public bool SendUnsafe(List<byte[]> frames, List<int> counts, ref ZError error /*, out Exception zeroEx */)
        {
            //zeroEx = null;
            //Contract.Assert(clrZmqPublisher != null, "This field must be set in ctor.");
            if (clrZmqPublisher == null)
                throw new InvalidOperationException("How have you managed to call this method before end of .ctor or after dispose?");

            //try
            {
                lock (clrZmqPublisherSyncObj)
                {
                    for (int j = 0, len = frames.Count; j < len; j++)
                    {
                        byte[] frameBuf = frames[j];
                        ZSocketFlags flags = (j < len - 1) ? (ZSocketFlags.DontWait | ZSocketFlags.More) : ZSocketFlags.DontWait;
                        // Дилеру не надо париться с идентификаторами. Удобно.
                        if (clrZmqPublisher.SendBytesUnsafe(frameBuf, counts[j], flags, out error))
                        {
                            //clrZmqPublishCounter++;
                        }
                        else
                            return false;
                    }

                    // If all frames were sent, then report success.
                    clrZmqPublishCounter++;
                    return true;
                }
            }
            //catch (Exception evEx)
            //{
            //    zeroEx = evEx;
            //    //Log.Error(evEx, "[{Host}] Обработчик события OnBarClosed сгенерировал исключение? Нехорошо.", Host);
            //}

            //return false;
        }

        public bool SendUms(List<UnmanagedMemoryStream> frames, List<int> counts, ref ZError error /*, out Exception zeroEx */)
        {
            //zeroEx = null;
            //Contract.Assert(clrZmqPublisher != null, "This field must be set in ctor.");
            if (clrZmqPublisher == null)
                throw new InvalidOperationException("How have you managed to call this method before end of .ctor or after dispose?");

            //try
            {
                lock (clrZmqPublisherSyncObj)
                {
                    for (int j = 0, len = frames.Count; j < len; j++)
                    {
                        UnmanagedMemoryStream frameBuf = frames[j];
                        ZSocketFlags flags = (j < len - 1) ? (ZSocketFlags.DontWait | ZSocketFlags.More) : ZSocketFlags.DontWait;
                        // Дилеру не надо париться с идентификаторами. Удобно.
                        if (clrZmqPublisher.SendUms(frameBuf, counts[j], flags, out error))
                        {
                            //clrZmqPublishCounter++;
                        }
                        else
                            return false;
                    }

                    // If all frames were sent, then report success.
                    clrZmqPublishCounter++;
                    return true;
                }
            }
            //catch (Exception evEx)
            //{
            //    zeroEx = evEx;
            //    //Log.Error(evEx, "[{Host}] Обработчик события OnBarClosed сгенерировал исключение? Нехорошо.", Host);
            //}

            //return false;
        }
        #endregion Send message of many 'Frames'
    }
}
