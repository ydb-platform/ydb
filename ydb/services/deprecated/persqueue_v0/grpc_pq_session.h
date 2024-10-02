#pragma once

#include "ydb/core/client/server/grpc_base.h"
#include <ydb/library/grpc/server/grpc_server.h>
#include <library/cpp/string_utils/quote/quote.h>
#include <util/generic/queue.h>

using grpc::Status;


namespace NKikimr {
namespace NGRpcProxy {

///////////////////////////////////////////////////////////////////////////////

using namespace NKikimrClient;

template<class TResponse>
class ISessionHandler : public TAtomicRefCount<ISessionHandler<TResponse>> {
public:
    virtual ~ISessionHandler()
    { }

    /// Finish session.
    virtual void Finish() = 0;

    /// Send reply to client.
    virtual void Reply(TResponse&& resp) = 0;

    virtual void ReadyForNextRead() = 0;

    virtual bool IsShuttingDown() const = 0;
};

template<class TResponse>
using ISessionHandlerRef = TIntrusivePtr<ISessionHandler<TResponse>>;


template <class TRequest, class TResponse>
class ISession : public ISessionHandler<TResponse>
{

    using ISessionRef = TIntrusivePtr<ISession<TRequest, TResponse>>;

protected:
    class TRequestCreated : public NYdbGrpc::IQueueEvent {
    public:
        TRequestCreated(ISessionRef session)
            : Session(session)
        { }

        bool Execute(bool ok) override {
            if (!ok) {
                Session->DestroyStream("waiting stream creating failed");
                return false;
            }

            Session->OnCreated();
            return false;
        }

        void DestroyRequest() override {
            if (!Session->Context.c_call() && Session->ClientDone) {
                // AsyncNotifyWhenDone will not appear on the queue.
                delete Session->ClientDone;
                Session->ClientDone = nullptr;
            }
            delete this;
        }

        ISessionRef Session;
    };

    class TReadDone : public NYdbGrpc::IQueueEvent {
    public:
        TReadDone(ISessionRef session)
            : Session(session)
        { }

        bool Execute(bool ok) override {
            if (ok) {
                Session->OnRead(Request);
            } else {
                if (Session->IsCancelled()) {
                    Session->DestroyStream("reading from stream failed");
                } else {
                    Session->OnDone();
                }
            }
            return false;
        }

        void DestroyRequest() override {
            delete this;
        }

        TRequest Request;
        ISessionRef Session;
    };

    class TWriteDone : public NYdbGrpc::IQueueEvent {
    public:
        TWriteDone(ISessionRef session, ui64 size)
            : Session(session)
            , Size(size)
        { }

        bool Execute(bool ok) override {
            Session->OnWriteDone(Size);
            if (!ok) {
                Session->DestroyStream("writing to stream failed");
                return false;
            }

            TGuard<TSpinLock> lock(Session->Lock);
            if (Session->Responses.empty()) {
                Session->HaveWriteInflight = false;
                if (Session->NeedFinish) {
                    lock.Release();
                    Session->Stream.Finish(Status::OK, new TFinishDone(Session));
                }
            } else {
                auto resp = std::move(Session->Responses.front());
                Session->Responses.pop();
                lock.Release();
                ui64 sz = resp.ByteSize();
                Session->Stream.Write(resp, new TWriteDone(Session, sz));
            }

            return false;
        }

        void DestroyRequest() override {
            delete this;
        }

        ISessionRef Session;
        ui64 Size;
    };

    class TFinishDone : public NYdbGrpc::IQueueEvent {
    public:
        TFinishDone(ISessionRef session)
            : Session(session)
        { }

        bool Execute(bool) override {
            Session->DestroyStream("some stream finished");
            return false;
        }

        void DestroyRequest() override {
            delete this;
        }

        ISessionRef Session;
    };

    class TClientDone : public NYdbGrpc::IQueueEvent {
    public:
        TClientDone(ISessionRef session)
            : Session(session)
        {
            Session->ClientDone = this;
        }

        bool Execute(bool) override {
            Session->ClientIsDone = true;
            Session->DestroyStream("sesison closed");
            return false;
        }

        void DestroyRequest() override {
            Y_ABORT_UNLESS(Session->ClientDone);
            Session->ClientDone = nullptr;
            delete this;
        }

        ISessionRef Session;
    };

public:
    ISession(grpc::ServerCompletionQueue* cq)
        : CQ(cq)
        , Stream(&Context)
        , HaveWriteInflight(false)
        , NeedFinish(false)
        , ClientIsDone(false)
    {
        Context.AsyncNotifyWhenDone(new TClientDone(this));
    }

    TString GetDatabase() const {
        TString key = "x-ydb-database";
        const auto& clientMetadata = Context.client_metadata();
        const auto range = clientMetadata.equal_range(grpc::string_ref{key.data(), key.size()});
        if (range.first == range.second) {
            return "";
        }

        TVector<TStringBuf> values;
        values.reserve(std::distance(range.first, range.second));

        for (auto it = range.first; it != range.second; ++it) {
            return TString(it->second.data(), it->second.size());
        }
        return "";
    }

    TString GetPeerName() const {
        auto res = Context.peer();
        // Remove percent-encoding
        CGIUnescape(res);

        if (res.StartsWith("ipv4:[") || res.StartsWith("ipv6:[")) {
            size_t pos = res.find(']');
            Y_ABORT_UNLESS(pos != TString::npos);
            res = res.substr(6, pos - 6);
        } else if (res.StartsWith("ipv4:")) {
            size_t pos = res.rfind(':');
            if (pos == TString::npos) {//no port
                res = res.substr(5);
            } else {
                res = res.substr(5, pos - 5);
            }
        } else {
            size_t pos = res.rfind(":"); //port
            if (pos != TString::npos) {
                res = res.substr(0, pos);
            }
        }
        return res;
    }

protected:

    virtual void OnCreated() = 0;
    virtual void OnRead(const TRequest& request) = 0;
    virtual void OnDone() = 0;
    virtual void OnWriteDone(ui64 size) = 0;

    virtual void DestroyStream(const TString& reason, NPersQueue::NErrorCode::EErrorCode code = NPersQueue::NErrorCode::BAD_REQUEST) = 0;

    /// Start accepting session's requests.
    virtual void Start() = 0;

    bool IsCancelled() const {
        return ClientIsDone && Context.IsCancelled();
    }

    void ReplyWithError(const TString& description, NPersQueue::NErrorCode::EErrorCode code)
    {
        TResponse response;
        response.MutableError()->SetDescription(description);
        response.MutableError()->SetCode(code);
        Reply(std::move(response));
        Finish();
    }

    /// Finish session.
    void Finish() override {
        {
            TGuard<TSpinLock> lock(Lock);
            if (NeedFinish)
                return;
            if (HaveWriteInflight || !Responses.empty()) {
                NeedFinish = true;
                return;
            }
            HaveWriteInflight = true;
        }

        Stream.Finish(Status::OK, new TFinishDone(this));
    }

    /// Send reply to client.
    void Reply(TResponse&& resp) override {
        {
            TGuard<TSpinLock> lock(Lock);
            if (NeedFinish) //ignore responses after finish
                return;
            if (HaveWriteInflight || !Responses.empty()) {
                Responses.push(std::move(resp));
                return;
            } else {
                HaveWriteInflight = true;
            }
        }

        ui64 size = resp.ByteSize();
        Stream.Write(resp, new TWriteDone(this, size));
    }

    void ReadyForNextRead() override {
        {
            TGuard<TSpinLock> lock(Lock);
            if (NeedFinish) {
                return;
            }
        }

        auto read = new TReadDone(this);
        Stream.Read(&read->Request, read);
    }

protected:
    grpc::ServerCompletionQueue* const CQ;
    grpc::ServerContext Context;
    grpc::ServerAsyncReaderWriter<TResponse, TRequest> Stream;

    TSpinLock Lock;
private:
    bool HaveWriteInflight;
    bool NeedFinish;
    std::atomic<bool> ClientIsDone;
    TClientDone* ClientDone;
    TQueue<TResponse> Responses; //TODO: if Responses total size is too big - fail this session;
};

}
}
