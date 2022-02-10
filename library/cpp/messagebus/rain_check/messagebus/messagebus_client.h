#pragma once

#include <library/cpp/messagebus/rain_check/core/task.h>

#include <library/cpp/messagebus/ybus.h>

namespace NRainCheck {
    class TBusFuture: public TSubtaskCompletion {
        friend class TBusClientService;

    private:
        THolder<NBus::TBusMessage> Request;
        THolder<NBus::TBusMessage> Response;
        NBus::EMessageStatus Status;

    private:
        TTaskRunnerBase* Task;

        void SetDoneAndSchedule(NBus::EMessageStatus, TAutoPtr<NBus::TBusMessage>);

    public:
        // TODO: add MESSAGE_UNDEFINED
        TBusFuture()
            : Status(NBus::MESSAGE_DONT_ASK)
            , Task(nullptr)
        {
        }

        NBus::TBusMessage* GetRequest() const {
            return Request.Get();
        }

        NBus::TBusMessage* GetResponse() const {
            Y_ASSERT(IsDone());
            return Response.Get();
        }

        NBus::EMessageStatus GetStatus() const {
            Y_ASSERT(IsDone());
            return Status;
        }
    };

    class TBusClientService: private NBus::IBusClientHandler {
    private:
        NBus::TBusClientSessionPtr Session;

    public:
        TBusClientService(const NBus::TBusSessionConfig&, NBus::TBusProtocol*, NBus::TBusMessageQueue*);
        ~TBusClientService() override;

        void Send(NBus::TBusMessageAutoPtr, const NBus::TNetAddr&, TBusFuture* future);
        void SendOneWay(NBus::TBusMessageAutoPtr, const NBus::TNetAddr&, TBusFuture* future);

        // Use it only for monitoring
        NBus::TBusClientSessionPtr GetSessionForMonitoring() const;

    private:
        void SendCommon(NBus::TBusMessage*, const NBus::TNetAddr&, TBusFuture* future);
        void ProcessResultCommon(NBus::TBusMessageAutoPtr, const NBus::TNetAddr&, TBusFuture* future, NBus::EMessageStatus);

        void OnReply(TAutoPtr<NBus::TBusMessage> pMessage, TAutoPtr<NBus::TBusMessage> pReply) override;
        void OnError(TAutoPtr<NBus::TBusMessage> pMessage, NBus::EMessageStatus status) override;
        void OnMessageSentOneWay(TAutoPtr<NBus::TBusMessage>) override;
    };

}
