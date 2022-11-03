#pragma once

#ifndef KIKIMR_DISABLE_S3_OPS

#include "abstract.h"

#include <ydb/core/base/events.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <library/cpp/actors/core/log.h>

#include <util/string/builder.h>
#include <util/string/printf.h>

namespace NKikimr::NWrappers::NExternalStorage {
class TFakeExternalStorage {
private:
    mutable TMutex Mutex;
    mutable TMap<TString, TString> Data;
public:
    TFakeExternalStorage() = default;
    TMap<TString, TString> GetData() const {
        TGuard<TMutex> g(Mutex);
        return Data;
    }
    void Execute(TEvListObjectsRequest::TPtr& ev) const;
    void Execute(TEvGetObjectRequest::TPtr& ev) const;
    void Execute(TEvHeadObjectRequest::TPtr& ev) const;
    void Execute(TEvPutObjectRequest::TPtr& ev) const;
    void Execute(TEvDeleteObjectRequest::TPtr& ev) const;
    void Execute(TEvDeleteObjectsRequest::TPtr& ev) const;
    void Execute(TEvCreateMultipartUploadRequest::TPtr& ev) const;
    void Execute(TEvUploadPartRequest::TPtr& ev) const;
    void Execute(TEvCompleteMultipartUploadRequest::TPtr& ev) const;
    void Execute(TEvAbortMultipartUploadRequest::TPtr& ev) const;
    void Execute(TEvCheckObjectExistsRequest::TPtr& ev) const;
};

class TFakeExternalStorageOperator: public IExternalStorageOperator {
public:
    TFakeExternalStorageOperator() = default;
    virtual void Execute(TEvCheckObjectExistsRequest::TPtr& ev) const override {
        Singleton<TFakeExternalStorage>()->Execute(ev);
    }
    virtual void Execute(TEvListObjectsRequest::TPtr& ev) const override {
        Singleton<TFakeExternalStorage>()->Execute(ev);
    }
    virtual void Execute(TEvGetObjectRequest::TPtr& ev) const override {
        Singleton<TFakeExternalStorage>()->Execute(ev);
    }
    virtual void Execute(TEvHeadObjectRequest::TPtr& ev) const override {
        Singleton<TFakeExternalStorage>()->Execute(ev);
    }
    virtual void Execute(TEvPutObjectRequest::TPtr& ev) const override {
        Singleton<TFakeExternalStorage>()->Execute(ev);
    }
    virtual void Execute(TEvDeleteObjectRequest::TPtr& ev) const override {
        Singleton<TFakeExternalStorage>()->Execute(ev);
    }
    virtual void Execute(TEvDeleteObjectsRequest::TPtr& ev) const override {
        Singleton<TFakeExternalStorage>()->Execute(ev);
    }
    virtual void Execute(TEvCreateMultipartUploadRequest::TPtr& ev) const override {
        Singleton<TFakeExternalStorage>()->Execute(ev);
    }
    virtual void Execute(TEvUploadPartRequest::TPtr& ev) const override {
        Singleton<TFakeExternalStorage>()->Execute(ev);
    }
    virtual void Execute(TEvCompleteMultipartUploadRequest::TPtr& ev) const override {
        Singleton<TFakeExternalStorage>()->Execute(ev);
    }
    virtual void Execute(TEvAbortMultipartUploadRequest::TPtr& ev) const override {
        Singleton<TFakeExternalStorage>()->Execute(ev);
    }
};
} // NKikimr::NWrappers::NExternalStorage

#endif // KIKIMR_DISABLE_S3_OPS
