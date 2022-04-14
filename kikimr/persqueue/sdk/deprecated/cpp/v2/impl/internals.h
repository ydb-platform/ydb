#pragma once
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/types.h>

#include <library/cpp/grpc/common/time_point.h>
#include <library/cpp/threading/future/future.h>

#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/string/cast.h>

#include <grpc++/channel.h>
#include <grpc++/create_channel.h>

#include <contrib/libs/grpc/include/grpcpp/impl/codegen/client_context.h>

#include <chrono>

#define WRITE_LOG(msg, srcId, sessionId, level, logger)                 \
    if (logger && logger->IsEnabled(level)) {                           \
        logger->Log(TStringBuilder() << msg, srcId, sessionId, level);  \
    }

#define DEBUG_LOG(msg, srcId, sessionId) WRITE_LOG(msg, srcId, sessionId, TLOG_DEBUG, Logger)
#define INFO_LOG(msg, srcId, sessionId) WRITE_LOG(msg, srcId, sessionId, TLOG_INFO, Logger)
#define WARN_LOG(msg, srcId, sessionId) WRITE_LOG(msg, srcId, sessionId, TLOG_WARNING, Logger)
#define ERR_LOG(msg, srcId, sessionId) WRITE_LOG(msg, srcId, sessionId, TLOG_ERR, Logger)

namespace NPersQueue {

TString GetToken(ICredentialsProvider* credentials);

void FillMetaHeaders(grpc::ClientContext& context, const TString& database, ICredentialsProvider* credentials);

bool UseCDS(const TServerSetting& server);

struct TWriteData {
    TProducerSeqNo SeqNo;
    TData Data;

    TWriteData(ui64 seqNo, TData&& data)
        : SeqNo(seqNo)
        , Data(std::move(data))
    {}
};

class IQueueEvent {
public:
    virtual ~IQueueEvent() = default;

    //! Execute an action defined by implementation.
    virtual bool Execute(bool ok) = 0;

    //! Finish and destroy request.
    virtual void DestroyRequest() = 0;
};

class IHandler : public TAtomicRefCount<IHandler> {
public:
    IHandler()
    {}

    virtual ~IHandler()
    {}

    virtual void Destroy(const TError&) = 0;
    virtual void Done() = 0;
    virtual TString ToString() = 0;
};

using IHandlerPtr = TIntrusivePtr<IHandler>;

class TQueueEvent : public IQueueEvent {
public:
    TQueueEvent(IHandlerPtr handler)
        : Handler(std::move(handler))
    {
        Y_ASSERT(Handler);
    }

private:
    bool Execute(bool ok) override {
        if (ok) {
            Handler->Done();
        } else {
            TError error;
            error.SetDescription("event " + Handler->ToString() + " failed");
            error.SetCode(NErrorCode::ERROR);
            Handler->Destroy(error);
        }
        return false;
    }

    virtual ~TQueueEvent() {
        Handler.Reset();
    }

    void DestroyRequest() override
    {
        delete this;
    }

    IHandlerPtr Handler;
};

} // namespace NPersQueue
