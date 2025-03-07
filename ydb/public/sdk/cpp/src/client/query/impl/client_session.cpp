#include "client_session.h"

#define INCLUDE_YDB_INTERNAL_H
#include <src/client/impl/ydb_internal/plain_status/status.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <src/library/issue/yql_issue_message.h>
#include <thread>

namespace NYdb::inline Dev::NQuery {

// Custom lock primitive to protect session from destroying
// during async read execution.
// The problem is TSession::TImpl holds grpc stream processor by IntrusivePtr
// and this processor alredy refcounted by internal code.
// That mean during TSession::TImpl dtor no gurantee to grpc procerrot will be destroyed.
// StreamProcessor_->Cancel() doesn't help it just start async cancelation but we have no way
// to wait cancelation has done.
// So we need some way to protect access to row session impl pointer
// from async reader (processor callback). We can't use shared/weak ptr here because TSessionImpl
// stores as uniq ptr inside session pool and as shared ptr in the TSession
// when user got session (see GetSmartDeleter related code).

// Why just not std::mutex? - Requirement do not destroy a mutex while it is locked
// makes it difficult to use here. Moreover we need to allow recursive lock.

// Why recursive lock? - In happy path we destroy session from CloseFromServer call,
// so the session dtor called from thread which already got the lock.

// TODO: Proably we can add sync version of Cancel method in to grpc stream procesor to make sure
// no more callback will be called.

class TSafeTSessionImplHolder {
    TSession::TImpl* Ptr;
    std::atomic_uint32_t Semaphore;
    std::atomic<std::thread::id> OwnerThread;
public:
    TSafeTSessionImplHolder(TSession::TImpl* p)
        : Ptr(p)
        , Semaphore(0)
    {}

    TSession::TImpl* TrySharedOwning() noexcept {
        auto old = Semaphore.fetch_add(1); 
        if (old == 0) {
            OwnerThread.store(std::this_thread::get_id());
            return Ptr;
        } else {
            return nullptr;
        }
    }

    void Release() noexcept {
        OwnerThread.store(std::thread::id());
        Semaphore.store(0);
    }

    void WaitAndLock() noexcept {
        if (OwnerThread.load() == std::this_thread::get_id()) {
            return;
        }

        uint32_t cur = 0;
        uint32_t newVal = 1;
        while (!Semaphore.compare_exchange_weak(cur, newVal,
            std::memory_order_release, std::memory_order_relaxed)) {
                std::this_thread::yield();
                cur = 0;
        }
    }
};

void TSession::TImpl::StartAsyncRead(TStreamProcessorPtr ptr, std::weak_ptr<ISessionClient> client,
    std::shared_ptr<TSafeTSessionImplHolder> holder)
{
    auto resp = std::make_shared<Ydb::Query::SessionState>();
    ptr->Read(resp.get(), [resp, ptr, client, holder](NYdbGrpc::TGrpcStatus grpcStatus) mutable {
        switch (grpcStatus.GRpcStatusCode) {
            case grpc::StatusCode::OK:
                StartAsyncRead(ptr, client, holder);
                break;
            default: {
                auto impl = holder->TrySharedOwning();
                if (impl) {
                    impl->CloseFromServer(client);
                    holder->Release();
                }
            }
        }
    });
}

TSession::TImpl::TImpl(TStreamProcessorPtr ptr, const std::string& sessionId, const std::string& endpoint, std::weak_ptr<ISessionClient> client)
    : TKqpSessionCommon(sessionId, endpoint, true)
    , StreamProcessor_(ptr)
    , SessionHolder(std::make_shared<TSafeTSessionImplHolder>(this))
{
    if (ptr) {
        MarkActive();
        SetNeedUpdateActiveCounter(true);
        StartAsyncRead(StreamProcessor_, client, SessionHolder);
    } else {
        MarkBroken();
        SetNeedUpdateActiveCounter(true);
    }
}

TSession::TImpl::~TImpl()
{
    if (StreamProcessor_) {
        StreamProcessor_->Cancel();
    }
    SessionHolder->WaitAndLock();
}

void TSession::TImpl::MakeImplAsync(TStreamProcessorPtr ptr,
    std::shared_ptr<TAttachSessionArgs> args)
{
    auto resp = std::make_shared<Ydb::Query::SessionState>();
    ptr->Read(resp.get(), [args, resp, ptr](NYdbGrpc::TGrpcStatus grpcStatus) mutable {
        if (grpcStatus.GRpcStatusCode != grpc::StatusCode::OK) {
            TStatus st(TPlainStatus(grpcStatus, args->Endpoint));
            args->Promise.SetValue(TCreateSessionResult(std::move(st), TSession(args->Client)));

        } else {
            if (resp->status() == Ydb::StatusIds::SUCCESS) {
                NYdb::TStatus st(TPlainStatus(grpcStatus, args->Endpoint));
                TSession::TImpl::NewSmartShared(ptr, std::move(args), st);

            } else {
                NYdb::NIssue::TIssues opIssues;
                NYdb::NIssue::IssuesFromMessage(resp->issues(), opIssues);
                TStatus st(static_cast<EStatus>(resp->status()), std::move(opIssues));
                args->Promise.SetValue(TCreateSessionResult(std::move(st), TSession(args->Client)));
            }
        }
    });
}

void TSession::TImpl::NewSmartShared(TStreamProcessorPtr ptr,
    std::shared_ptr<TAttachSessionArgs> args, NYdb::TStatus st)
{
    args->Promise.SetValue(
        TCreateSessionResult(
            std::move(st),
            TSession(
                args->Client,
                new TSession::TImpl(ptr, args->SessionId, args->Endpoint, args->SessionClient)
            )
        )
    );
}

}
