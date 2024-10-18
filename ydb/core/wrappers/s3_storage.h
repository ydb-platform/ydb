#pragma once

#ifndef KIKIMR_DISABLE_S3_OPS

#include "abstract.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/wrappers/events/common.h>

#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/client/ClientConfiguration.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/model/StorageClass.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/auth/AWSCredentials.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/client/AWSClient.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/model/AbortMultipartUploadRequest.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/model/CreateMultipartUploadRequest.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/model/CompleteMultipartUploadRequest.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/model/GetObjectRequest.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/model/HeadObjectRequest.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/model/PutObjectRequest.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/model/DeleteObjectRequest.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/model/UploadPartRequest.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/model/UploadPartCopyRequest.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/S3Client.h>
#include <ydb/library/actors/core/log.h>
#include <util/generic/scope.h>
#include <util/string/builder.h>
#include <util/string/printf.h>

#include <condition_variable>
#include <mutex>

namespace NKikimr::NWrappers::NExternalStorage {

class TS3ExternalStorage: public IExternalStorageOperator {
private:
    THolder<Aws::S3::S3Client> Client;
    const Aws::Client::ClientConfiguration Config;
    const Aws::Auth::AWSCredentials Credentials;
    const TString Bucket;
    const Aws::S3::Model::StorageClass StorageClass = Aws::S3::Model::StorageClass::STANDARD;
    bool Verbose = true;

    mutable std::mutex RunningQueriesMutex;
    mutable std::condition_variable RunningQueriesNotifier;
    mutable int RunningQueriesCount = 0;

    template <typename TRequest, typename TOutcome>
    using THandler = std::function<void(const Aws::S3::S3Client*, const TRequest&, const TOutcome&, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&)>;

    template <typename TRequest, typename TOutcome>
    using TFunc = std::function<void(const Aws::S3::S3Client*, const TRequest&, THandler<TRequest, TOutcome>, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&)>;

    template <typename TEvRequest, typename TEvResponse, template <typename...> typename TContext>
    void Call(typename TEvRequest::TPtr& ev, TFunc<typename TEvRequest::TRequest, typename TEvResponse::TOutcome> func) const {
        using TCtx = TContext<TEvRequest, TEvResponse>;
        ev->Get()->MutableRequest().WithBucket(Bucket);

        auto ctx = std::make_shared<TCtx>(TlsActivationContext->ActorSystem(), ev->Sender, ev->Get()->GetRequestContext(), StorageClass, ReplyAdapter);
        auto callback = [this](
            const Aws::S3::S3Client*,
            const typename TEvRequest::TRequest& request,
            const typename TEvResponse::TOutcome& outcome,
            const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context) {
                const auto* ctx = static_cast<const TCtx*>(context.get());

                Y_DEFER {
                    std::unique_lock guard(RunningQueriesMutex);
                    --RunningQueriesCount;
                    if (RunningQueriesCount == 0) {
                        RunningQueriesNotifier.notify_all();
                    }
                };

                if (Verbose) {
                    LOG_NOTICE_S(*ctx->GetActorSystem(), NKikimrServices::S3_WRAPPER, "Response"
                        << ": uuid# " << ctx->GetUUID()
                        << ", response# " << outcome);
                } else {
                    LOG_INFO_S(*ctx->GetActorSystem(), NKikimrServices::S3_WRAPPER, "Response"
                        << ": uuid# " << ctx->GetUUID()
                        << ", response# " << outcome);
                }
                ctx->Reply(request, outcome);
        };

        if (Verbose) {
            LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::S3_WRAPPER, "Request"
                << ": uuid# " << ctx->GetUUID()
                << ", request# " << ev->Get()->GetRequest());
        } else {
            LOG_INFO_S(*TlsActivationContext, NKikimrServices::S3_WRAPPER, "Request"
                << ": uuid# " << ctx->GetUUID()
                << ", request# " << ev->Get()->GetRequest());
        }
        func(Client.Get(), ctx->PrepareRequest(ev), callback, ctx);

        std::unique_lock guard(RunningQueriesMutex);
        ++RunningQueriesCount;
    }

public:
    TS3ExternalStorage(const Aws::Client::ClientConfiguration& config,
        const Aws::Auth::AWSCredentials& credentials,
        const TString& bucket, const Aws::S3::Model::StorageClass storageClass,
        bool verbose = true,
        bool useVirtualAdressing = true)
        : Client(new Aws::S3::S3Client(
            credentials,
            config,
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
            useVirtualAdressing))
        , Config(config)
        , Credentials(credentials)
        , Bucket(bucket)
        , StorageClass(storageClass)
        , Verbose(verbose)
    {
    }

    ~TS3ExternalStorage();

    virtual void Execute(TEvCheckObjectExistsRequest::TPtr& ev) const override;
    virtual void Execute(TEvListObjectsRequest::TPtr& ev) const override;
    virtual void Execute(TEvGetObjectRequest::TPtr& ev) const override;
    virtual void Execute(TEvHeadObjectRequest::TPtr& ev) const override;
    virtual void Execute(TEvPutObjectRequest::TPtr& ev) const override;
    virtual void Execute(TEvDeleteObjectRequest::TPtr& ev) const override;
    virtual void Execute(TEvDeleteObjectsRequest::TPtr& ev) const override;
    virtual void Execute(TEvCreateMultipartUploadRequest::TPtr& ev) const override;
    virtual void Execute(TEvUploadPartRequest::TPtr& ev) const override;
    virtual void Execute(TEvCompleteMultipartUploadRequest::TPtr& ev) const override;
    virtual void Execute(TEvAbortMultipartUploadRequest::TPtr& ev) const override;
    virtual void Execute(TEvUploadPartCopyRequest::TPtr& ev) const override;
};
} // NKikimr::NWrappers::NExternalStorage

#endif // KIKIMR_DISABLE_S3_OPS
