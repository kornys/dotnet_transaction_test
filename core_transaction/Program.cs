using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;
using Amqp;
using Amqp.Framing;

namespace core_transactions
{
    class Test
    {
        public static void Main(string[] args)
        {
            String url = (args.Length > 0) ? args[0] : "amqp://127.0.0.1:5672";
            String destination = (args.Length > 1) ? args[1] : "myqueue";

            Trace.TraceLevel = TraceLevel.Frame;
            Trace.TraceListener = (l, f, a) => Console.WriteLine(
                    DateTime.Now.ToString("[hh:mm:ss.fff]") + " " + string.Format(f, a));

            Address address = new Address(url);
            string testName = "TransactedPosting";
            int nMsgs = 5;

            Connection connection = new Connection(address);
            Session session = new Session(connection);
            SenderLink sender = new SenderLink(session, "sender-" + testName, destination);

            // commit
            using (var ts = new TransactionScope())
            {
                for (int i = 0; i < nMsgs; i++)
                {
                    Message message = new Message("test");
                    message.Properties = new Properties() { MessageId = "commit" + i, GroupId = testName };
                    sender.Send(message);
                }

                ts.Complete();
            }
            Console.WriteLine("Commit");

            // rollback
            using (var ts = new TransactionScope())
            {
                for (int i = nMsgs; i < nMsgs * 2; i++)
                {
                    Message message = new Message("test");
                    message.Properties = new Properties() { MessageId = "rollback" + i, GroupId = testName };
                    sender.Send(message); //error connection is closed
                }
            }
            Console.WriteLine("Rollback");

            // commit
            using (var ts = new TransactionScope())
            {
                for (int i = 0; i < nMsgs; i++)
                {
                    Message message = new Message("test");
                    message.Properties = new Properties() { MessageId = "commit" + i, GroupId = testName };
                    sender.Send(message);
                }

                ts.Complete();
            }
            Console.WriteLine("Commit");

            //receive
            ReceiverLink receiver = new ReceiverLink(session, "receiver-" + testName, destination);
            for (int i = 0; i < nMsgs * 2; i++)
            {
                Message message = receiver.Receive();
                Trace.WriteLine(TraceLevel.Information, "receive: {0}", message.Properties.MessageId);
                receiver.Accept(message);
            }

            Console.WriteLine("Received messages");

            Message m = receiver.Receive(TimeSpan.FromSeconds(5));

            if (m != null)
            {
                throw new Exception("Broker contains uncommited messages");
            }

            connection.Close();
        }
    }
}
