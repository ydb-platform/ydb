#pragma once

#ifndef KIKIMR_DISABLE_S3_OPS

#include "abstract.h"

#include <ydb/core/base/events.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <ydb/library/actors/core/log.h>

#include <util/string/builder.h>
#include <util/string/printf.h>

namespace NKikimr::NWrappers::NExternalStorage {
class TFakeBucketStorage {
private:
    mutable TMutex Mutex;
    TMap<TString, TString> Data;
    static inline TAtomicCounter WritesCount;
    static inline TAtomicCounter DeletesCount;
public:
    static i64 GetWritesCount() {
        return WritesCount.Val();
    }

    static i64 GetDeletesCount() {
        return DeletesCount.Val();
    }

    static void ResetWriteCounters() {
        WritesCount = 0;
        DeletesCount = 0;
    }

    TMap<TString, TString>::const_iterator begin() const {
        return Data.begin();
    }
    TMap<TString, TString>::const_iterator end() const {
        return Data.end();
    }
    ui32 GetSize() const {
        return Data.size();
    }
    void PutObject(const TString& objectId, const TString& data) {
        WritesCount.Inc();
        TGuard<TMutex> g(Mutex);
        Data[objectId] = data;
    }
    std::optional<TString> GetObject(const TString& objectId) const {
        TGuard<TMutex> g(Mutex);
        auto it = Data.find(objectId);
        if (it == Data.end()) {
            return {};
        }
        return it->second;
    }
    void Remove(const TString& objectId) {
        DeletesCount.Inc();
        TGuard<TMutex> g(Mutex);
        Data.erase(objectId);
    }
};

class TFakeExternalStorage {
private:
    YDB_ACCESSOR_DEF(TString, SecretKey);
    mutable TMutex Mutex;
    mutable TMap<TString, TFakeBucketStorage> BucketStorages;
    TEvListObjectsResponse::TResult BuildListObjectsResult(const TEvListObjectsRequest::TRequest& request) const;

    TString AwsToString(const Aws::String& awsString) const {
        TString result(awsString.data(), awsString.size());
        return result;
    }
public:
    TFakeExternalStorage() = default;

    static i64 GetWritesCount() {
        return TFakeBucketStorage::GetWritesCount();
    }

    static i64 GetDeletesCount() {
        return TFakeBucketStorage::GetDeletesCount();
    }

    static void ResetWriteCounters() {
        return TFakeBucketStorage::ResetWriteCounters();
    }

    const TFakeBucketStorage& GetBucket(const TString& bucketId) const {
        TGuard<TMutex> g(Mutex);
        auto it = BucketStorages.find(bucketId);
        if (it == BucketStorages.end()) {
            it = BucketStorages.emplace(bucketId, TFakeBucketStorage()).first;
        }
        return it->second;
    }

    TFakeBucketStorage& MutableBucket(const TString& bucketId) const {
        TGuard<TMutex> g(Mutex);
        auto it = BucketStorages.find(bucketId);
        if (it == BucketStorages.end()) {
            it = BucketStorages.emplace(bucketId, TFakeBucketStorage()).first;
        }
        return it->second;
    }

    ui32 GetSize() const {
        ui32 result = 0;
        TGuard<TMutex> g(Mutex);
        for (auto&& i : BucketStorages) {
            result += i.second.GetSize();
        }
        return result;
    }

    ui32 GetBucketsCount() const {
        TGuard<TMutex> g(Mutex);
        return BucketStorages.size();
    }
    void Execute(TEvListObjectsRequest::TPtr& ev, const TReplyAdapterContainer& adapter) const;
    void Execute(TEvGetObjectRequest::TPtr& ev, const TReplyAdapterContainer& adapter) const;
    void Execute(TEvHeadObjectRequest::TPtr& ev, const TReplyAdapterContainer& adapter) const;
    void Execute(TEvPutObjectRequest::TPtr& ev, const TReplyAdapterContainer& adapter) const;
    void Execute(TEvDeleteObjectRequest::TPtr& ev, const TReplyAdapterContainer& adapter) const;
    void Execute(TEvDeleteObjectsRequest::TPtr& ev, const TReplyAdapterContainer& adapter) const;
    void Execute(TEvCreateMultipartUploadRequest::TPtr& ev, const TReplyAdapterContainer& adapter) const;
    void Execute(TEvUploadPartRequest::TPtr& ev, const TReplyAdapterContainer& adapter) const;
    void Execute(TEvCompleteMultipartUploadRequest::TPtr& ev, const TReplyAdapterContainer& adapter) const;
    void Execute(TEvAbortMultipartUploadRequest::TPtr& ev, const TReplyAdapterContainer& adapter) const;
    void Execute(TEvCheckObjectExistsRequest::TPtr& ev, const TReplyAdapterContainer& adapter) const;
    void Execute(TEvUploadPartCopyRequest::TPtr& ev, const TReplyAdapterContainer& adapter) const;
};

class TFakeExternalStorageOperator: public IExternalStorageOperator {
private:
    const TString Bucket;
    const TString SecretKey;
    std::shared_ptr<TFakeExternalStorage> OwnedStorage;

    template <class TEvent>
    void ExecuteImpl(TEvent& ev) const {
        ev->Get()->MutableRequest().WithBucket(Bucket);
        Y_ABORT_UNLESS(SecretKey == Singleton<TFakeExternalStorage>()->GetSecretKey());
        if (OwnedStorage) {
            OwnedStorage->Execute(ev, ReplyAdapter);
        } else {
            Singleton<TFakeExternalStorage>()->Execute(ev, ReplyAdapter);
        }
    }

    virtual TString DoDebugString() const override {
        return "type:FAKE;";
    }

public:
    TFakeExternalStorageOperator(const TString& bucket, const TString& secretKey, const std::shared_ptr<TFakeExternalStorage> storage = {})
        : Bucket(bucket)
        , SecretKey(secretKey)
        , OwnedStorage(storage)
    {
    }
    virtual void Execute(TEvCheckObjectExistsRequest::TPtr& ev) const override {
        ExecuteImpl(ev);
    }
    virtual void Execute(TEvListObjectsRequest::TPtr& ev) const override {
        ExecuteImpl(ev);
    }
    virtual void Execute(TEvGetObjectRequest::TPtr& ev) const override {
        ExecuteImpl(ev);
    }
    virtual void Execute(TEvHeadObjectRequest::TPtr& ev) const override {
        ExecuteImpl(ev);
    }
    virtual void Execute(TEvPutObjectRequest::TPtr& ev) const override {
        ExecuteImpl(ev);
    }
    virtual void Execute(TEvDeleteObjectRequest::TPtr& ev) const override {
        ExecuteImpl(ev);
    }
    virtual void Execute(TEvDeleteObjectsRequest::TPtr& ev) const override {
        ExecuteImpl(ev);
    }
    virtual void Execute(TEvCreateMultipartUploadRequest::TPtr& ev) const override {
        ExecuteImpl(ev);
    }
    virtual void Execute(TEvUploadPartRequest::TPtr& ev) const override {
        ExecuteImpl(ev);
    }
    virtual void Execute(TEvCompleteMultipartUploadRequest::TPtr& ev) const override {
        ExecuteImpl(ev);
    }
    virtual void Execute(TEvAbortMultipartUploadRequest::TPtr& ev) const override {
        ExecuteImpl(ev);
    }
    virtual void Execute(TEvUploadPartCopyRequest::TPtr& ev) const override {
        ExecuteImpl(ev);
    }
};
} // NKikimr::NWrappers::NExternalStorage

#endif // KIKIMR_DISABLE_S3_OPS
