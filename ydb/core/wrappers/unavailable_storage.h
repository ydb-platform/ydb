#pragma once

#include <ydb/library/actors/core/log.h>
#include <ydb/core/wrappers/abstract.h>

namespace NKikimr::NWrappers::NExternalStorage {

class TUnavailableExternalStorageOperator: public IExternalStorageOperator {
private:
    const TString Exception;
    const TString Reason;
    mutable std::atomic<ui64> RequestCount{0};
    mutable std::atomic<TInstant> LastResetTime{TInstant::Now()};

    template <class TResponse, class TRequestPtr>
    void ExecuteImpl(TRequestPtr& ev) const {
        TInstant now = TInstant::Now();
        TInstant lastReset = LastResetTime.load();
        if (now - lastReset > TDuration::Seconds(30)) {
            RequestCount.store(0);
            LastResetTime.store(now);
        }

        ui64 currentRequest = RequestCount.fetch_add(1);
        if (currentRequest > 0) {
            ui64 delayMs = std::min(1000ULL << std::min(currentRequest - 1ULL, 3ULL), 10000ULL);
            Sleep(TDuration::MilliSeconds(delayMs));
        }

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

    void ResetRequestCount() const {
        RequestCount.store(0);
        LastResetTime.store(TInstant::Now());
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

}   // namespace NKikimr::NWrappers::NExternalStorage
