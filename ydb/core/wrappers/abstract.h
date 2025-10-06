#pragma once

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/core/util/backoff.h>
#include <ydb/core/wrappers/retry_policy.h>
#include <util/system/mutex.h>

#include <ydb/core/protos/s3_settings.pb.h>
#include <ydb/core/wrappers/events/abstract.h>
#include <ydb/core/wrappers/events/common.h>
#include <ydb/core/wrappers/events/get_object.h>
#include <ydb/core/wrappers/events/object_exists.h>

#include <memory>

namespace NKikimr::NWrappers {

struct TEvExternalStorage {
    using TEvListObjectsRequest = NExternalStorage::TEvListObjectsRequest;
    using TEvListObjectsResponse = NExternalStorage::TEvListObjectsResponse;
    using TEvGetObjectRequest = NExternalStorage::TEvGetObjectRequest;
    using TEvGetObjectResponse = NExternalStorage::TEvGetObjectResponse;
    using TEvHeadObjectRequest = NExternalStorage::TEvHeadObjectRequest;
    using TEvHeadObjectResponse = NExternalStorage::TEvHeadObjectResponse;
    using TEvPutObjectRequest = NExternalStorage::TEvPutObjectRequest;
    using TEvPutObjectResponse = NExternalStorage::TEvPutObjectResponse;
    using TEvDeleteObjectRequest = NExternalStorage::TEvDeleteObjectRequest;
    using TEvDeleteObjectResponse = NExternalStorage::TEvDeleteObjectResponse;
    using TEvDeleteObjectsRequest = NExternalStorage::TEvDeleteObjectsRequest;
    using TEvDeleteObjectsResponse = NExternalStorage::TEvDeleteObjectsResponse;
    using TEvCreateMultipartUploadRequest = NExternalStorage::TEvCreateMultipartUploadRequest;
    using TEvCreateMultipartUploadResponse = NExternalStorage::TEvCreateMultipartUploadResponse;
    using TEvUploadPartRequest = NExternalStorage::TEvUploadPartRequest;
    using TEvUploadPartResponse = NExternalStorage::TEvUploadPartResponse;
    using TEvCompleteMultipartUploadRequest = NExternalStorage::TEvCompleteMultipartUploadRequest;
    using TEvCompleteMultipartUploadResponse = NExternalStorage::TEvCompleteMultipartUploadResponse;
    using TEvAbortMultipartUploadRequest = NExternalStorage::TEvAbortMultipartUploadRequest;
    using TEvAbortMultipartUploadResponse = NExternalStorage::TEvAbortMultipartUploadResponse;
    using TEvCheckObjectExistsRequest = NExternalStorage::TEvCheckObjectExistsRequest;
    using TEvCheckObjectExistsResponse = NExternalStorage::TEvCheckObjectExistsResponse;
    using TEvUploadPartCopyRequest = NExternalStorage::TEvUploadPartCopyRequest;
    using TEvUploadPartCopyResponse = NExternalStorage::TEvUploadPartCopyResponse;
};

namespace NExternalStorage {

class TThreadSafeBackoff {
private:
    mutable TMutex Mutex;
    NKikimr::TBackoff Policy;
public:
    TThreadSafeBackoff(size_t maxRetries = 100, TDuration initial = TDuration::Seconds(3), TDuration max = TDuration::Seconds(10))
        : Policy(maxRetries, initial, max) {}
    void Reset() {
        with_lock(Mutex) {
            Policy.Reset();
        }
    }
    TDuration Next() {
        with_lock(Mutex) {
            return Policy.Next();
        }
    }
};

class IReplyAdapter {
private:
    std::optional<NActors::TActorId> CustomRecipient;
public:
    using TPtr = std::shared_ptr<IReplyAdapter>;
    virtual ~IReplyAdapter() = default;

    NActors::TActorId GetRecipient(const NActors::TActorId& defaultValue) {
        return CustomRecipient.value_or(defaultValue);
    }

    virtual std::unique_ptr<IEventBase> RebuildReplyEvent(std::unique_ptr<TEvListObjectsResponse>&& ev) const = 0;
    virtual std::unique_ptr<IEventBase> RebuildReplyEvent(std::unique_ptr<TEvGetObjectResponse>&& ev) const = 0;
    virtual std::unique_ptr<IEventBase> RebuildReplyEvent(std::unique_ptr<TEvHeadObjectResponse>&& ev) const = 0;
    virtual std::unique_ptr<IEventBase> RebuildReplyEvent(std::unique_ptr<TEvPutObjectResponse>&& ev) const = 0;
    virtual std::unique_ptr<IEventBase> RebuildReplyEvent(std::unique_ptr<TEvDeleteObjectResponse>&& ev) const = 0;
    virtual std::unique_ptr<IEventBase> RebuildReplyEvent(std::unique_ptr<TEvDeleteObjectsResponse>&& ev) const = 0;
    virtual std::unique_ptr<IEventBase> RebuildReplyEvent(std::unique_ptr<TEvCreateMultipartUploadResponse>&& ev) const = 0;
    virtual std::unique_ptr<IEventBase> RebuildReplyEvent(std::unique_ptr<TEvUploadPartResponse>&& ev) const = 0;
    virtual std::unique_ptr<IEventBase> RebuildReplyEvent(std::unique_ptr<TEvCompleteMultipartUploadResponse>&& ev) const = 0;
    virtual std::unique_ptr<IEventBase> RebuildReplyEvent(std::unique_ptr<TEvAbortMultipartUploadResponse>&& ev) const = 0;
    virtual std::unique_ptr<IEventBase> RebuildReplyEvent(std::unique_ptr<TEvCheckObjectExistsResponse>&& ev) const = 0;
    virtual std::unique_ptr<IEventBase> RebuildReplyEvent(std::unique_ptr<TEvUploadPartCopyResponse>&& ev) const = 0;
};

class TReplyAdapterContainer {
private:
    IReplyAdapter::TPtr Adapter;
    std::shared_ptr<TThreadSafeBackoff> Backoff;
public:
    TReplyAdapterContainer() = default;
    TReplyAdapterContainer(IReplyAdapter::TPtr adapter, std::shared_ptr<TThreadSafeBackoff> backoff = {})
        : Adapter(adapter)
        , Backoff(std::move(backoff)) {

    }

    const IReplyAdapter::TPtr& GetObject() const {
        return Adapter;
    }

    bool operator!() const {
        return !Adapter;
    }

    template <class TBaseEventObject>
    std::unique_ptr<IEventBase> RebuildReplyEvent(std::unique_ptr<TBaseEventObject>&& ev) const {
        return Adapter ? Adapter->RebuildReplyEvent(std::move(ev)) : std::move(ev);
    }

    NActors::TActorId GetRecipient(const NActors::TActorId& defaultValue) const {
        return Adapter ? Adapter->GetRecipient(defaultValue) : defaultValue;
    }

    template <class TBaseEventObject>
    void Reply(const NActors::TActorId& recipientId, std::unique_ptr<TBaseEventObject>&& ev) const {
        bool doBackoff = false;
        TDuration delay = TDuration::Zero();

        if (ev->IsSuccess()) {
            
            Backoff->Reset();
        } else if (NWrappers::ShouldBackoff(ev->GetError())) {
            AFL_VERIFY(Backoff);
            delay = Backoff->Next();
            doBackoff = delay > TDuration::Zero();
        }

        std::unique_ptr<NActors::IEventBase> finalEvent;
        if (Adapter) {
            finalEvent = Adapter->RebuildReplyEvent(std::move(ev));
        } else {
            finalEvent.reset(ev.release());
        }

        const NActors::TActorId recipient = Adapter ? Adapter->GetRecipient(recipientId) : recipientId;
        if (doBackoff) {
            auto* handle = new NActors::IEventHandle(recipient, NActors::TActorId(), finalEvent.release());
            TlsActivationContext->ActorSystem()->Schedule(delay, handle);
        } else {
            TlsActivationContext->ActorSystem()->Send(recipient, finalEvent.release());
        }

    }

};

class IExternalStorageOperator {
protected:
    TReplyAdapterContainer ReplyAdapter;
    std::shared_ptr<TThreadSafeBackoff> BackoffPolicy = std::make_shared<TThreadSafeBackoff>();
    virtual TString DoDebugString() const {
        return "";
    }
public:
    using TPtr = std::shared_ptr<IExternalStorageOperator>;
    void InitReplyAdapter(IReplyAdapter::TPtr adapter) {
        Y_ABORT_UNLESS(!ReplyAdapter);
        ReplyAdapter = TReplyAdapterContainer(adapter, BackoffPolicy);
    }


    virtual ~IExternalStorageOperator() = default;

    TString DebugString() const {
        return DoDebugString();
    }
    virtual void Execute(TEvListObjectsRequest::TPtr & ev) const = 0;
    virtual void Execute(TEvGetObjectRequest::TPtr& ev) const = 0;
    virtual void Execute(TEvHeadObjectRequest::TPtr& ev) const = 0;
    virtual void Execute(TEvPutObjectRequest::TPtr& ev) const = 0;
    virtual void Execute(TEvDeleteObjectRequest::TPtr& ev) const = 0;
    virtual void Execute(TEvDeleteObjectsRequest::TPtr& ev) const = 0;
    virtual void Execute(TEvCreateMultipartUploadRequest::TPtr& ev) const = 0;
    virtual void Execute(TEvUploadPartRequest::TPtr& ev) const = 0;
    virtual void Execute(TEvCompleteMultipartUploadRequest::TPtr& ev) const = 0;
    virtual void Execute(TEvAbortMultipartUploadRequest::TPtr& ev) const = 0;
    virtual void Execute(TEvCheckObjectExistsRequest::TPtr& ev) const = 0;
    virtual void Execute(TEvUploadPartCopyRequest::TPtr& ev) const = 0;
};

class IExternalStorageConfig {
private:
    class TStoragesCollection;

protected:
    virtual IExternalStorageOperator::TPtr DoConstructStorageOperator(bool verbose) const = 0;
    virtual TString DoGetStorageId() const = 0;

    TString GetStorageId() const {
        return DoGetStorageId();
    }
public:
    using TPtr = std::shared_ptr<IExternalStorageConfig>;
    virtual ~IExternalStorageConfig() = default;
    IExternalStorageOperator::TPtr ConstructStorageOperator(bool verbose = true) const;
    static IExternalStorageConfig::TPtr Construct(const NKikimrSchemeOp::TS3Settings& settings);
};
} // NExternalStorage

using IExternalStorageConfig = NExternalStorage::IExternalStorageConfig;

} // NKikimr::NWrappers
