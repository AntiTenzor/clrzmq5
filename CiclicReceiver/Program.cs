using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

using ZeroMQ;

namespace CiclicReceiver
{
    internal class Program
    {
        public static string Host = "CycSub";

        protected static USub clrZmqSubscriber;
        protected static long clrZmqReceiveCounter = 0;

        protected static string dataPubUrl;

        static void Main(string[] args)
        {
            //dataPubUrl = ConfigurationManager.AppSettings["DataPub"];
            dataPubUrl = "tcp://127.0.0.1:7373";
            Console.WriteLine("[{0}] Native LibZMQ version: {1}", Host, ZeroMQ.lib.zmq.LibraryVersion);

            Console.WriteLine("[{0}] Создаю экземпляр класса ZSocket типа SUB...", Host);
            // Допустим, <add key="DataPub" value="tcp://*:56888" />
            Console.WriteLine("[{0}] И подключаю SUBSCRIBER к адресу '{1}'...", Host, dataPubUrl);

            clrZmqSubscriber = new USub(new[] { dataPubUrl });

            clrZmqSubscriber.OnRawMessageReceived -= ClrZmqSubscriber_OnMessageReceived;
            clrZmqSubscriber.OnRawMessageReceived += ClrZmqSubscriber_OnMessageReceived;

            Console.WriteLine("[{0}] I'm going to start subscriber now...", Host);

            clrZmqSubscriber.Start();

            Console.WriteLine("[{0}] Subscriber is receiving messages...", Host);




            Console.WriteLine("[{0}] Press Enter to finish", Host);
            Console.ReadLine();


            clrZmqSubscriber.Stop();
            Console.WriteLine("[{0}] Finished.");
        }

        private static void ClrZmqSubscriber_OnMessageReceived(USub sender, Queue<ArraySegment<byte>> payloads)
        {
            if ((payloads == null) || (payloads.Count <= 0))
                return;

            int framesCount = payloads.Count;
            if (framesCount != 2)
                Console.WriteLine("Expected length:{0}; actual length: {1}", 2, framesCount);

            ArraySegment<byte> topic = payloads.Dequeue();
            if (topic.Count != 1)
                Console.WriteLine("   Expected topic length:{0}; actual TOPIC length: {1}", 1, topic.Count);
            byte top = topic.Array[0];
            sender.arrayPool.Return(topic.Array);

            ArraySegment<byte> info = payloads.Dequeue();
            if (info.Count != 16)
                Console.WriteLine("   Expected info length:{0}; actual INFO length: {1}", 16, info.Count);

            int counter = -1234;
            Guid infoGuid = default(Guid);
            using (MemoryStream ms = new MemoryStream(info.Array, 0, info.Count, false))
            using (BinaryReader br = new BinaryReader(ms))
            {
                counter = br.ReadInt32();

                ms.Position = 0;
                if (info.Count == 16)
                    infoGuid = new Guid(br.ReadBytes(info.Count));
            }
            sender.arrayPool.Return(info.Array);


            Console.WriteLine("   TOPIC:{0} |   Counter:{1}; frames.Count:{2}; info.Count:{3};   infoGuid: {4}",
                top, counter, framesCount, info.Count, infoGuid);
            Console.WriteLine();

            while (payloads.Count > 0)
            {
                ArraySegment<byte> trash = payloads.Dequeue();
                sender.arrayPool.Return(trash.Array);
            }
        }
    }
}
