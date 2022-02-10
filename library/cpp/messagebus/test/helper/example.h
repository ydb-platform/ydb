#pragma once

#include <library/cpp/testing/unittest/registar.h>

#include "alloc_counter.h"
#include "message_handler_error.h"

#include <library/cpp/messagebus/ybus.h>
#include <library/cpp/messagebus/misc/test_sync.h>

#include <util/system/event.h>

namespace NBus {
    namespace NTest {
        class TExampleRequest: public TBusMessage {
            friend class TExampleProtocol;

        private:
            TAllocCounter AllocCounter;

        public:
            TString Data;

        public:
            TExampleRequest(TAtomic* counterPtr, size_t payloadSize = 320);
            TExampleRequest(ECreateUninitialized, TAtomic* counterPtr);
        };

        class TExampleResponse: public TBusMessage {
            friend class TExampleProtocol;

        private:
            TAllocCounter AllocCounter;

        public:
            TString Data;
            TExampleResponse(TAtomic* counterPtr, size_t payloadSize = 320);
            TExampleResponse(ECreateUninitialized, TAtomic* counterPtr);
        };

        class TExampleProtocol: public TBusProtocol {
        public:
            TAtomic RequestCount;
            TAtomic ResponseCount;
            TAtomic RequestCountDeserialized;
            TAtomic ResponseCountDeserialized;
            TAtomic StartCount;

            TExampleProtocol(int port = 0);

            ~TExampleProtocol() override;

            void Serialize(const TBusMessage* message, TBuffer& buffer) override;

            TAutoPtr<TBusMessage> Deserialize(ui16 messageType, TArrayRef<const char> payload) override;
        };

        class TExampleClient: private TBusClientHandlerError {
        public:
            TExampleProtocol Proto;
            bool UseCompression;
            bool CrashOnError;
            size_t DataSize;

            ssize_t MessageCount;
            TAtomic RepliesCount;
            TAtomic Errors;
            EMessageStatus LastError;

            TSystemEvent WorkDone;

            TBusMessageQueuePtr Bus;
            TBusClientSessionPtr Session;

        public:
            TExampleClient(const TBusClientSessionConfig sessionConfig = TBusClientSessionConfig(), int port = 0);
            ~TExampleClient() override;

            EMessageStatus SendMessage(const TNetAddr* addr = nullptr);

            void SendMessages(size_t count, const TNetAddr* addr = nullptr);
            void SendMessages(size_t count, const TNetAddr& addr);

            void ResetCounters();
            void WaitReplies();
            EMessageStatus WaitForError();
            void WaitForError(EMessageStatus status);

            void SendMessagesWaitReplies(size_t count, const TNetAddr* addr = nullptr);
            void SendMessagesWaitReplies(size_t count, const TNetAddr& addr);

            void OnReply(TAutoPtr<TBusMessage> mess, TAutoPtr<TBusMessage> reply) override;

            void OnError(TAutoPtr<TBusMessage> mess, EMessageStatus) override;
        };

        class TExampleServer: private TBusServerHandlerError {
        public:
            TExampleProtocol Proto;
            bool UseCompression;
            bool AckMessageBeforeSendReply;
            TMaybe<size_t> DataSize; // Nothing means use request size
            bool ForgetRequest;

            TTestSync TestSync;

            TBusMessageQueuePtr Bus;
            TBusServerSessionPtr Session;

        public:
            TExampleServer(
                const char* name = "TExampleServer",
                const TBusServerSessionConfig& sessionConfig = TBusServerSessionConfig());

            TExampleServer(unsigned port, const char* name = "TExampleServer");

            ~TExampleServer() override;

        public:
            size_t GetInFlight() const;
            unsigned GetActualListenPort() const;
            // any of
            TNetAddr GetActualListenAddr() const;

            void WaitForOnMessageCount(unsigned n);

        protected:
            void OnMessage(TOnMessageContext& mess) override;
        };

    }
}
