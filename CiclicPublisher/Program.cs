using System;
using System.Linq;
using System.Text;
using System.Threading;
using System.Collections.Generic;

using ZeroMQ;
using System.Diagnostics;
using System.IO;

namespace CiclicPublisher
{
    internal class Program
    {
        public static string Host = "CycPub";

        protected static UPub clrZmqPublisher;
        protected static long clrZmqPublishCounter = 0;

        protected static string dataPubUrl;

        static void Main(string[] args)
        {
            const int keyCount = 8;
            const int messageCount = 16;
            const int repeatCount = 1900;
            const int repeatDelayMs = 10_000;

            List<Tuple<byte[], byte[]>> messages = new List<Tuple<byte[], byte[]>>(messageCount);
            for (int j = 0; j < messageCount; j++)
            {
                byte[] topic = new byte[] { (byte)(j % keyCount) };
                byte[] msg = Guid.NewGuid().ToByteArray();
                using (MemoryStream ms = new MemoryStream(msg, 0, msg.Length, true))
                using (BinaryWriter bw = new BinaryWriter(ms))
                {
                    bw.Write(j);

                    bw.Flush();
                    bw.Close();
                }

                Tuple<byte[], byte[]> tuple = new Tuple<byte[], byte[]>(topic, msg);
                messages.Add(tuple);
            }

            Console.WriteLine("Preparing publisher...");

            //dataPubUrl = ConfigurationManager.AppSettings["DataPub"];
            dataPubUrl = "tcp://*:7373";
            Console.WriteLine("[{0}] Native LibZMQ version: {1}", Host, ZeroMQ.lib.zmq.LibraryVersion);

            Console.WriteLine("[{0}] Создаю экземпляр класса ZSocket типа PUB...", Host);
            // Допустим, <add key="DataPub" value="tcp://*:56888" />
            Console.WriteLine("[{0}] И привязываю PUBLISHER к адресу '{1}'...", Host, dataPubUrl);

            clrZmqPublisher = new UPub(dataPubUrl);

            Console.WriteLine("I'm going to start publishing now...");

            TimeSpan[] totalTimes = new TimeSpan[] { TimeSpan.Zero, TimeSpan.Zero };
            for (int j = 0; j <= repeatCount; j++)
            {
                Thread.Sleep(repeatDelayMs);

                // Буду публиковать топик + сообщение
                List<byte[]> frames = new List<byte[]>();
                frames.Add(null);
                frames.Add(null);
                List<int> counts = new List<int>();
                counts.Add(0);
                counts.Add(0);

                int methId = j % 2;
                string method = (methId == 0) ? "UPub.SendBytes(List)" : "UPub.SendUnsafe(List)";

                ZError error = ZError.None;
                Stopwatch sw = Stopwatch.StartNew();
                foreach (Tuple<byte[], byte[]> tuple in messages)
                {
                    frames[0] = tuple.Item1;
                    frames[1] = tuple.Item2;
                    counts[0] = frames[0].Length;
                    counts[1] = frames[1].Length;


                    if (methId == 0)
                    {
                        // Дилеру не надо париться с идентификаторами. Удобно.
                        if (!clrZmqPublisher.SendBytes(frames, counts, ref error))
                            Console.WriteLine("[{0}] Проблема при отправке сообщения методом {1}. error: {2}", Host, method, error);
                        else
                            clrZmqPublishCounter++;
                    }
                    else
                    {
                        // Дилеру не надо париться с идентификаторами. Удобно.
                        if (!clrZmqPublisher.SendUnsafe(frames, counts, ref error))
                            Console.WriteLine("[{0}] Проблема при отправке сообщения методом {1}. error: {2}", Host, method, error);
                        else
                            clrZmqPublishCounter++;
                    }
                }
                sw.Stop();

                totalTimes[methId] = totalTimes[methId] + sw.Elapsed;

                Console.WriteLine("{0} сообщений опубликовано за   {1:### ##0} mcs методом {2}", messages.Count, sw.Elapsed.TotalMilliseconds * 1000, method);
            } // End for (int j = 0; j <= repeatCount; j++)


            TimeSpan avgTimeBytes = new TimeSpan(totalTimes[0].Ticks / ((repeatCount + 1) / 2));
            TimeSpan avgUnsafeBytes = new TimeSpan(totalTimes[1].Ticks / ((repeatCount + 1) / 2));
            Console.WriteLine("[{0}] Среднее время   {1:### ##0} mcs методом {2}", Host, avgTimeBytes.TotalMilliseconds * 1000, "UPub.SendBytes");
            Console.WriteLine("[{0}] Среднее время   {1:### ##0} mcs методом {2}", Host, avgUnsafeBytes.TotalMilliseconds * 1000, "UPub.SendUnsafe");
        }
    }
}
