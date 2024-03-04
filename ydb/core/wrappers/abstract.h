#pragma once
#include <ydb/core/base/events.h>
#include <ydb/core/wrappers/events/abstract.h>
#include <ydb/core/wrappers/events/common.h>
#include <ydb/core/wrappers/events/delete_objects.h>
#include <ydb/core/wrappers/events/list_objects.h>
#include <ydb/core/wrappers/events/object_exists.h>
#include <ydb/core/wrappers/events/get_object.h>
#include <util/generic/ptr.h>

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
public:
    TReplyAdapterContainer() = default;
    TReplyAdapterContainer(IReplyAdapter::TPtr adapter)
        : Adapter(adapter) {

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
        if (Adapter) {
            TlsActivationContext->ActorSystem()->Send(Adapter->GetRecipient(recipientId), Adapter->RebuildReplyEvent(std::move(ev)).release());
        } else {
            TlsActivationContext->ActorSystem()->Send(recipientId, ev.release());
        }

    }

};

class IExternalStorageOperator {
protected:
    TReplyAdapterContainer ReplyAdapter;
    virtual TString DoDebugString() const {
        return "";
    }
public:
    using TPtr = std::shared_ptr<IExternalStorageOperator>;
    void InitReplyAdapter(IReplyAdapter::TPtr adapter) {
        Y_ABORT_UNLESS(!ReplyAdapter);
        ReplyAdapter = TReplyAdapterContainer(adapter);
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
