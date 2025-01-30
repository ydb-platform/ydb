#pragma once

#ifndef KIKIMR_DISABLE_S3_OPS

#include <ydb/library/actors/core/log.h>
#include <ydb/core/wrappers/abstract.h>

namespace NKikimr::NWrappers::NExternalStorage {

class TUnavailableExternalStorageOperator: public IExternalStorageOperator {
private:
    const TString Exception;
    const TString Reason;

    template <class TResponse, class TRequestPtr>
    void ExecuteImpl(TRequestPtr& ev) const {
        const Aws::S3::S3Error error = Aws::S3::S3Error(
            Aws::Client::AWSError<Aws::Client::CoreErrors>(Aws::Client::CoreErrors::SERVICE_UNAVAILABLE, Exception, Reason, false));
        std::unique_ptr<TResponse> response;
        constexpr bool hasKey = requires(const TRequestPtr& r) { r->Get()->GetRequest().GetKey(); };
        constexpr bool hasRange = std::is_same_v<TResponse, TEvGetObjectResponse>;
        if constexpr (hasRange) {
            std::pair<ui64, ui64> range;
            AFL_VERIFY(TResponse::TryParseRange(TString(ev->Get()->GetRequest().GetRange()), range))(
                "original", ev->Get()->GetRequest().GetRange());
            response = std::make_unique<TResponse>(TString(ev->Get()->GetRequest().GetKey()), range, error);
        } else if constexpr (hasKey) {
            response = std::make_unique<TResponse>(TString(ev->Get()->GetRequest().GetKey()), error);
        } else {
            response = std::make_unique<TResponse>(error);
        }
        ReplyAdapter.Reply(ev->Sender, std::move(response));
    }

    virtual TString DoDebugString() const override {
        return "type:UNAVAILABLE;";
    }

public:
    TUnavailableExternalStorageOperator(const TString& exceptionName, const TString& unavailabilityReason)
        : Exception(exceptionName)
        , Reason(unavailabilityReason) {
    }
    virtual void Execute(TEvCheckObjectExistsRequest::TPtr& ev) const override {
        ExecuteImpl<TEvCheckObjectExistsResponse>(ev);
    }
    virtual void Execute(TEvListObjectsRequest::TPtr& ev) const override {
        ExecuteImpl<TEvListObjectsResponse>(ev);
    }
    virtual void Execute(TEvGetObjectRequest::TPtr& ev) const override {
        ExecuteImpl<TEvGetObjectResponse>(ev);
    }
    virtual void Execute(TEvHeadObjectRequest::TPtr& ev) const override {
        ExecuteImpl<TEvHeadObjectResponse>(ev);
    }
    virtual void Execute(TEvPutObjectRequest::TPtr& ev) const override {
        ExecuteImpl<TEvPutObjectResponse>(ev);
    }
    virtual void Execute(TEvDeleteObjectRequest::TPtr& ev) const override {
        ExecuteImpl<TEvDeleteObjectResponse>(ev);
    }
    virtual void Execute(TEvDeleteObjectsRequest::TPtr& ev) const override {
        ExecuteImpl<TEvDeleteObjectsResponse>(ev);
    }
    virtual void Execute(TEvCreateMultipartUploadRequest::TPtr& ev) const override {
        ExecuteImpl<TEvCreateMultipartUploadResponse>(ev);
    }
    virtual void Execute(TEvUploadPartRequest::TPtr& ev) const override {
        ExecuteImpl<TEvUploadPartResponse>(ev);
    }
    virtual void Execute(TEvCompleteMultipartUploadRequest::TPtr& ev) const override {
        ExecuteImpl<TEvCompleteMultipartUploadResponse>(ev);
    }
    virtual void Execute(TEvAbortMultipartUploadRequest::TPtr& ev) const override {
        ExecuteImpl<TEvAbortMultipartUploadResponse>(ev);
    }
    virtual void Execute(TEvUploadPartCopyRequest::TPtr& ev) const override {
        ExecuteImpl<TEvUploadPartCopyResponse>(ev);
    }
};

#endif   // KIKIMR_DISABLE_S3_OPS
}
