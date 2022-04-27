using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;

using ZeroMQ;

namespace Examples
{
	static partial class Program
	{
		static int RTDealerMemLeak_Workers = 10;

		public static void RTDealerMemLeak(string[] args)
		{
            //
            // ROUTER-to-DEALER example
            //
            // While this example runs in a single process, that is only to make
            // it easier to start and stop the example. Each thread has its own
            // context and conceptually acts as a separate process.
            //
            // Author: metadings, Anton Kytmanov
            //

            const string routerUrl = "tcp://*:5671";

            using (var context = new ZContext())
			using (var broker = new ZSocket(context, ZSocketType.ROUTER))
			{
				// Will return error if worker is not available anymore
				broker.RouterMandatory = RouterMandatory.Report;

				broker.Bind(routerUrl /* "tcp://*:5671" */);

				for (int i = 0; i < RTDealerMemLeak_Workers; ++i)
				{
					int j = i;
					new Thread(() => RTDealerMemLeak_Worker(j)).Start();
				}

				var stopwatch = new Stopwatch();
				stopwatch.Start();

				// Run for five seconds and then tell workers to end
				int workers_fired = 0;
				while (true)
				{
					// Next message gives us least recently used worker
					using (ZMessage zmsg = broker.ReceiveMessage())
					{
						int frameCount = zmsg.Count;
						string id = zmsg.PopString();
						Console.WriteLine("[BROK] Message from {0} with {1} frames...", id, frameCount);

                        using (ZFrame empty = zmsg.Pop())
                        {
                            if (empty.Length > 0)
                            {
                                if (empty.Length < 100)
                                {
                                    Console.WriteLine("[BROK] Welcome message?  {0}",
                                        PrintZFrame("[ZFRAME]", empty));
                                }
                                else
                                {
                                    byte[] payload = empty.Read();
                                    Console.WriteLine("[BROK] Payload {0} bytes?", payload.Length);
                                }
                            }
                            empty.Close();
                        }

                        using (ZFrame zf = new ZFrame(id))
                        {
                            // Send address identity
                            bool success = broker.SendFrameMore(zf, out ZError error);
                            zf.Close();
                        }

                        //broker.SendMore(new ZFrame(id));
                        // Why should I send empty frame???
                        //broker.SendMore(new ZFrame());

                        // Encourage workers until it's time to fire them
                        if (stopwatch.Elapsed < TimeSpan.FromSeconds(137))
						{
                            // Send payload to process
							broker.Send(new ZFrame($"Work harder! {DateTime.Now:HH:mm:ss.ffffff}"));
						}
						else
						{
                            // Send finalization command
							broker.Send(new ZFrame("Fired!"));

							if (++workers_fired >= RTDealer_Workers)
							{
                                Console.WriteLine();
                                Console.WriteLine("[BROK] All workers fired. Test complete.");
                                break;
							}
						}

						Console.WriteLine();
					} // End using (ZMessage zmsg = broker.ReceiveMessage())
                } // End while (true)

                //Thread.Sleep(7000);
                //Console.WriteLine("[BROK] Disconnecting...");
                //broker.Disconnect(routerUrl);

                Thread.Sleep(7000);
                Console.WriteLine("[BROK] Disposing...");
                //broker.Dispose();
            }

            Console.WriteLine("[BROK] Done!");
        }

		static void RTDealerMemLeak_Worker(int i)
		{
			using (var context = new ZContext())
			using (var worker = new ZSocket(context, ZSocketType.DEALER))
			{
				worker.IdentityString = "PEER" + i;	// Set a printable identity
				worker.Connect("tcp://127.0.0.1:5671");

				int total = 0;
				while (true)
				{
					// Tell the broker we're ready for work
					
					// DEALER socket must NOT send its identity!!!
					//worker.SendMore(new ZFrame(worker.Identity));
					
					// Why should I send empty frame???
					//worker.SendMore(new ZFrame());
					
					worker.Send(new ZFrame("Hi Boss! " + worker.IdentityString + " is ready."));

					// Get workload from broker, until finished
					string msg = null;
					using (ZMessage zmsg = worker.ReceiveMessage())
					{
						msg = zmsg.PopString();
						zmsg.Dismiss();
					}

					bool finished = "Fired!".Equals(msg, StringComparison.InvariantCultureIgnoreCase);

					if (finished)
					{
						break;
					}
					else
					{
						total++;

						// Do some random work
						Thread.Sleep(3);

						byte[] payload = new byte[1 * 1024*1024];
                        //if (!worker.SendBytes(payload, 0, payload.Length, ZSocketFlags.DontWait, out ZError error))
                        if (!worker.SendBytesUnsafe(payload, ZSocketFlags.DontWait, out ZError error))
                        {
                            Console.WriteLine("Что-то пошло не так? {0}", error);
                            throw new InvalidOperationException("Что-то пошло не так? " + error.ToString());
						}
					}
				}

				Console.WriteLine("\r\n  [WORK] {0} has completed {1} tasks", worker.IdentityString, total);
			}
		}
	}
}