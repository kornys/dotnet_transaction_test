using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amqp;
using Amqp.Framing;
using System.Transactions;

namespace net_transaction
{
    class Test
    {
        public static void Main(string[] args)
        {
            Address address = new Address("amqp://admin:admin@10.37.144.50:5672");
            string queue = "myqueue";
            string testName = "TransactedPosting";
            int nMsgs = 5;

            Connection connection = new Connection(address);
            Session session = new Session(connection);
            SenderLink sender = new SenderLink(session, "sender-" + testName, queue);

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
            ReceiverLink receiver = new ReceiverLink(session, "receiver-" + testName, queue);
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