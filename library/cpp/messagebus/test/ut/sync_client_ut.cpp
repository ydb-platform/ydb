#include <library/cpp/messagebus/test/helper/example.h>
#include <library/cpp/messagebus/test/helper/object_count_check.h>

namespace NBus {
    namespace NTest {
        using namespace std;

        ////////////////////////////////////////////////////////////////////
        /// \brief Client for sending synchronous message to local server
        struct TSyncClient {
            TNetAddr ServerAddr;

            TExampleProtocol Proto;
            TBusMessageQueuePtr Bus;
            TBusSyncClientSessionPtr Session;

            int NumReplies;
            int NumMessages;

            /// constructor creates instances of queue, protocol and session
            TSyncClient(const TNetAddr& serverAddr)
                : ServerAddr(serverAddr)
            {
                /// create or get instance of message queue, need one per application
                Bus = CreateMessageQueue();

                NumReplies = 0;
                NumMessages = 10;

                /// register source/client session
                TBusClientSessionConfig sessionConfig;
                Session = Bus->CreateSyncSource(&Proto, sessionConfig);
                Session->RegisterService("localhost");
            }

            ~TSyncClient() {
                Session->Shutdown();
            }

            /// dispatch of requests is done here
            void Work() {
                for (int i = 0; i < NumMessages; i++) {
                    THolder<TExampleRequest> mess(new TExampleRequest(&Proto.RequestCount));
                    EMessageStatus status;
                    THolder<TBusMessage> reply(Session->SendSyncMessage(mess.Get(), status, &ServerAddr));
                    if (!!reply) {
                        NumReplies++;
                    }
                }
            }
        };

        Y_UNIT_TEST_SUITE(SyncClientTest) {
            Y_UNIT_TEST(TestSync) {
                TObjectCountCheck objectCountCheck;

                TExampleServer server;
                TSyncClient client(server.GetActualListenAddr());
                client.Work();
                // assert correct number of replies
                UNIT_ASSERT_EQUAL(client.NumReplies, client.NumMessages);
                // assert that there is no message left in flight
                UNIT_ASSERT_EQUAL(server.Session->GetInFlight(), 0);
                UNIT_ASSERT_EQUAL(client.Session->GetInFlight(), 0);
            }
        }

    }
}
