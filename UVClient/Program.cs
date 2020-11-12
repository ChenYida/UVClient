using NATS.Client;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace UVClient
{
    internal class Replier
    {
        private Dictionary<string, string> parsedArgs = new Dictionary<string, string>();

        private int count = 200;
        private string url = Defaults.Url;
        private string subject = "foo";
        private bool sync = false;
        private int received = 0;
        private bool verbose = false;
        private Msg replyMsg = new Msg();
        private string creds = null;

        public void Run(string[] args)
        {
            parseArgs(args);
            banner();

            Options opts = ConnectionFactory.GetDefaultOptions();
            opts.Url = url;
            if (creds != null)
            {
                opts.SetUserCredentials(creds);
            }

            replyMsg.Data = Encoding.UTF8.GetBytes("reply");

            using (IConnection c = new ConnectionFactory().CreateConnection(opts))
            {
                EventHandler<MsgHandlerEventArgs> h = async (sender, args) =>
                {
                    for (int i = 0; i < 30; i++)
                    {
                        c.Publish("RealtimeData", Encoding.UTF8.GetBytes((i * 10).ToString()));
                        Console.WriteLine($"Send point x={i} y={i * 10}.");
                        await Task.Delay(200);
                    }
                    c.Publish("RealtimeData", Encoding.UTF8.GetBytes("Complete"));
                };
                c.SubscribeAsync("RunMethod", h);

                TimeSpan elapsed;

                if (sync)
                {
                    elapsed = receiveSyncSubscriber(c);
                }
                else
                {
                    elapsed = receiveAsyncSubscriber(c);
                }

                Console.Write("Replied to {0} msgs in {1} seconds ", received, elapsed.TotalSeconds);
                Console.WriteLine("({0} replies/second).",
                    (int)(received / elapsed.TotalSeconds));
                printStats(c);
            }
        }

        private void printStats(IConnection c)
        {
            IStatistics s = c.Stats;
            Console.WriteLine("Statistics:  ");
            Console.WriteLine("   Incoming Payload Bytes: {0}", s.InBytes);
            Console.WriteLine("   Incoming Messages: {0}", s.InMsgs);
            Console.WriteLine("   Outgoing Payload Bytes: {0}", s.OutBytes);
            Console.WriteLine("   Outgoing Messages: {0}", s.OutMsgs);
        }

        private TimeSpan receiveAsyncSubscriber(IConnection c)
        {
            Stopwatch sw = null;
            AutoResetEvent subDone = new AutoResetEvent(false);

            EventHandler<MsgHandlerEventArgs> msgHandler = (sender, args) =>
            {
                if (received == 0)
                {
                    sw = new Stopwatch();
                    sw.Start();
                }

                received++;

                if (verbose)
                    Console.WriteLine("Received: " + args.Message);

                replyMsg.Subject = args.Message.Reply;
                var random = new Random();
                int num = random.Next();
                replyMsg.Data = Encoding.UTF8.GetBytes("UV received your request <" + Encoding.Default.GetString(args.Message.Data) + "> and returns " + num);

                Console.WriteLine("  Received message <{0}> on subject {1}", Encoding.Default.GetString(args.Message.Data), subject);

                c.Publish(replyMsg);
                c.Flush();

                if (received == count)
                {
                    sw.Stop();
                    subDone.Set();
                }
            };

            using (IAsyncSubscription s = c.SubscribeAsync(subject, msgHandler))
            {
                // just wait to complete
                subDone.WaitOne();
            }

            return sw.Elapsed;
        }

        private TimeSpan receiveSyncSubscriber(IConnection c)
        {
            using (ISyncSubscription s = c.SubscribeSync(subject))
            {
                Stopwatch sw = new Stopwatch();

                while (received < count)
                {
                    if (received == 0)
                        sw.Start();

                    Msg m = s.NextMessage();
                    received++;

                    if (verbose)
                        Console.WriteLine("Received: " + m);

                    replyMsg.Subject = m.Reply;
                    c.Publish(replyMsg);
                }

                sw.Stop();

                return sw.Elapsed;
            }
        }

        private void usage()
        {
            Console.Error.WriteLine(
                "Usage:  Replier [-url url] [-subject subject] " +
                "-count [count] -creds [file] [-sync] [-verbose]");

            Environment.Exit(-1);
        }

        private void parseArgs(string[] args)
        {
            if (args == null)
                return;

            for (int i = 0; i < args.Length; i++)
            {
                if (args[i].Equals("-sync") ||
                    args[i].Equals("-verbose"))
                {
                    parsedArgs.Add(args[i], "true");
                }
                else
                {
                    if (i + 1 == args.Length)
                        usage();

                    parsedArgs.Add(args[i], args[i + 1]);
                    i++;
                }
            }

            if (parsedArgs.ContainsKey("-count"))
                count = Convert.ToInt32(parsedArgs["-count"]);

            if (parsedArgs.ContainsKey("-url"))
                url = parsedArgs["-url"];

            if (parsedArgs.ContainsKey("-subject"))
                subject = parsedArgs["-subject"];

            if (parsedArgs.ContainsKey("-sync"))
                sync = true;

            if (parsedArgs.ContainsKey("-verbose"))
                verbose = true;

            if (parsedArgs.ContainsKey("-creds"))
                creds = parsedArgs["-creds"];
        }

        private void banner()
        {
            Console.WriteLine("Receiving messages on subject {1}",
                count, subject);
            Console.WriteLine("  Url: {0}", url);
            Console.WriteLine("  Receiving: {0}",
                sync ? "Synchronously" : "Asynchronously");
        }

        public static void Main(string[] args)
        {
            try
            {
                new Replier().Run(args);
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine("Exception: " + ex.Message);
                Console.Error.WriteLine(ex);
            }
        }
    }
}