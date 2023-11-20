#pragma once

///////////////////////////////////////////////////////////////////
/// \file
/// \brief Example of using local session for communication.

#include <library/cpp/messagebus/test/helper/alloc_counter.h>
#include <library/cpp/messagebus/test/helper/example.h>
#include <library/cpp/messagebus/test/helper/message_handler_error.h>

#include <library/cpp/messagebus/ybus.h>
#include <library/cpp/messagebus/oldmodule/module.h>

namespace NBus {
    namespace NTest {
        using namespace std;

#define TYPE_HOSTINFOREQUEST 100
#define TYPE_HOSTINFORESPONSE 101

        ////////////////////////////////////////////////////////////////////
        /// \brief DupDetect protocol that common between client and server
        ////////////////////////////////////////////////////////////////////
        /// \brief HostInfo request class
        class THostInfoMessage: public TBusMessage {
        public:
            THostInfoMessage()
                : TBusMessage(TYPE_HOSTINFOREQUEST)
            {
            }
            THostInfoMessage(ECreateUninitialized)
                : TBusMessage(MESSAGE_CREATE_UNINITIALIZED)
            {
            }

            ~THostInfoMessage() override {
            }
        };

        ////////////////////////////////////////////////////////////////////
        /// \brief HostInfo reply class
        class THostInfoReply: public TBusMessage {
        public:
            THostInfoReply()
                : TBusMessage(TYPE_HOSTINFORESPONSE)
            {
            }
            THostInfoReply(ECreateUninitialized)
                : TBusMessage(MESSAGE_CREATE_UNINITIALIZED)
            {
            }

            ~THostInfoReply() override {
            }
        };

        ////////////////////////////////////////////////////////////////////
        /// \brief HostInfo protocol that common between client and server
        class THostInfoProtocol: public TBusProtocol {
        public:
            THostInfoProtocol()
                : TBusProtocol("HOSTINFO", 0)
            {
            }
            /// serialized protocol specific data into TBusData
            void Serialize(const TBusMessage* mess, TBuffer& data) override {
                Y_UNUSED(data);
                Y_UNUSED(mess);
            }

            /// deserialized TBusData into new instance of the message
            TAutoPtr<TBusMessage> Deserialize(ui16 messageType, TArrayRef<const char> payload) override {
                Y_UNUSED(payload);

                if (messageType == TYPE_HOSTINFOREQUEST) {
                    return new THostInfoMessage(MESSAGE_CREATE_UNINITIALIZED);
                } else if (messageType == TYPE_HOSTINFORESPONSE) {
                    return new THostInfoReply(MESSAGE_CREATE_UNINITIALIZED);
                } else {
                    Y_ABORT("unknown");
                }
            }
        };

        //////////////////////////////////////////////////////////////
        /// \brief HostInfo handler (should convert it to module too)
        struct THostInfoHandler: public TBusServerHandlerError {
            TBusServerSessionPtr Session;
            TBusServerSessionConfig HostInfoConfig;
            THostInfoProtocol HostInfoProto;

            THostInfoHandler(TBusMessageQueue* queue) {
                Session = TBusServerSession::Create(&HostInfoProto, this, HostInfoConfig, queue);
            }

            void OnMessage(TOnMessageContext& mess) override {
                usleep(10 * 1000); /// pretend we are doing something

                TAutoPtr<THostInfoReply> reply(new THostInfoReply());

                mess.SendReplyMove(reply);
            }

            TNetAddr GetActualListenAddr() {
                return TNetAddr("localhost", Session->GetActualListenPort());
            }
        };

        //////////////////////////////////////////////////////////////
        /// \brief DupDetect handler (should convert it to module too)
        struct TDupDetectHandler: public TBusClientHandlerError {
            TNetAddr ServerAddr;

            TBusClientSessionPtr DupDetect;
            TBusClientSessionConfig DupDetectConfig;
            TExampleProtocol DupDetectProto;

            int NumMessages;
            int NumReplies;

            TDupDetectHandler(const TNetAddr& serverAddr, TBusMessageQueuePtr queue)
                : ServerAddr(serverAddr)
            {
                DupDetect = TBusClientSession::Create(&DupDetectProto, this, DupDetectConfig, queue);
                DupDetect->RegisterService("localhost");
            }

            void Work() {
                NumMessages = 10;
                NumReplies = 0;

                for (int i = 0; i < NumMessages; i++) {
                    TExampleRequest* mess = new TExampleRequest(&DupDetectProto.RequestCount);
                    DupDetect->SendMessage(mess, &ServerAddr);
                }
            }

            void OnReply(TAutoPtr<TBusMessage> mess, TAutoPtr<TBusMessage> reply) override {
                Y_UNUSED(mess);
                Y_UNUSED(reply);
                NumReplies++;
            }
        };

        /////////////////////////////////////////////////////////////////
        /// \brief DupDetect module

        struct TDupDetectModule: public TBusModule {
            TNetAddr HostInfoAddr;

            TBusClientSessionPtr HostInfoClientSession;
            TBusClientSessionConfig HostInfoConfig;
            THostInfoProtocol HostInfoProto;

            TExampleProtocol DupDetectProto;
            TBusServerSessionConfig DupDetectConfig;

            TNetAddr ListenAddr;

            TDupDetectModule(const TNetAddr& hostInfoAddr)
                : TBusModule("DUPDETECTMODULE")
                , HostInfoAddr(hostInfoAddr)
            {
            }

            bool Init(TBusMessageQueue* queue) {
                HostInfoClientSession = CreateDefaultSource(*queue, &HostInfoProto, HostInfoConfig);
                HostInfoClientSession->RegisterService("localhost");

                return TBusModule::CreatePrivateSessions(queue);
            }

            TBusServerSessionPtr CreateExtSession(TBusMessageQueue& queue) override {
                TBusServerSessionPtr session = CreateDefaultDestination(queue, &DupDetectProto, DupDetectConfig);

                ListenAddr = TNetAddr("localhost", session->GetActualListenPort());

                return session;
            }

            /// entry point into module, first function to call
            TJobHandler Start(TBusJob* job, TBusMessage* mess) override {
                TExampleRequest* dmess = dynamic_cast<TExampleRequest*>(mess);
                Y_UNUSED(dmess);

                THostInfoMessage* hmess = new THostInfoMessage();

                /// send message to imaginary hostinfo server
                job->Send(hmess, HostInfoClientSession, TReplyHandler(), 0, HostInfoAddr);

                return TJobHandler(&TDupDetectModule::ProcessHostInfo);
            }

            /// next handler is executed when all outstanding requests from previous handler is completed
            TJobHandler ProcessHostInfo(TBusJob* job, TBusMessage* mess) {
                TExampleRequest* dmess = dynamic_cast<TExampleRequest*>(mess);
                Y_UNUSED(dmess);

                THostInfoMessage* hmess = job->Get<THostInfoMessage>();
                THostInfoReply* hreply = job->Get<THostInfoReply>();
                EMessageStatus hstatus = job->GetStatus<THostInfoMessage>();
                Y_ASSERT(hmess != nullptr);
                Y_ASSERT(hreply != nullptr);
                Y_ASSERT(hstatus == MESSAGE_OK);

                return TJobHandler(&TDupDetectModule::Finish);
            }

            /// last handler sends reply and returns NULL
            TJobHandler Finish(TBusJob* job, TBusMessage* mess) {
                Y_UNUSED(mess);

                TExampleResponse* reply = new TExampleResponse(&DupDetectProto.ResponseCount);
                job->SendReply(reply);

                return nullptr;
            }
        };

    }
}
