#pragma once

#ifndef KIKIMR_DISABLE_S3_OPS

#include "abstract.h"
#include "s3_storage_config.h"

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
#include <library/cpp/actors/core/log.h>
#include <util/string/builder.h>
#include <util/string/printf.h>

namespace NKikimr::NWrappers::NExternalStorage {

class TS3ExternalStorage: public IExternalStorageOperator, TS3User {
private:
    THolder<Aws::S3::S3Client> Client;
    const Aws::Client::ClientConfiguration Config;
    const Aws::Auth::AWSCredentials Credentials;
    const TString Bucket;
    const Aws::S3::Model::StorageClass StorageClass = Aws::S3::Model::StorageClass::STANDARD;

    template <typename TRequest, typename TOutcome>
    using THandler = std::function<void(const Aws::S3::S3Client*, const TRequest&, const TOutcome&, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&)>;

    template <typename TRequest, typename TOutcome>
    using TFunc = std::function<void(const Aws::S3::S3Client*, const TRequest&, THandler<TRequest, TOutcome>, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&)>;

    template <typename TEvRequest, typename TEvResponse, template <typename...> typename TContext>
    void Call(typename TEvRequest::TPtr& ev, TFunc<typename TEvRequest::TRequest, typename TEvResponse::TOutcome> func) const {
        using TCtx = TContext<TEvRequest, TEvResponse>;
        ev->Get()->MutableRequest().WithBucket(Bucket);

        auto ctx = std::make_shared<TCtx>(TlsActivationContext->ActorSystem(), ev->Sender, ev->Get()->GetRequestContext(), StorageClass);
        auto callback = [](
            const Aws::S3::S3Client*,
            const typename TEvRequest::TRequest& request,
            const typename TEvResponse::TOutcome& outcome,
            const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context) {
                const auto* ctx = static_cast<const TCtx*>(context.get());

                LOG_INFO_S(*ctx->GetActorSystem(), NKikimrServices::S3_WRAPPER, "Response"
                    << ": uuid# " << ctx->GetUUID()
                    << ", response# " << outcome);
                ctx->Reply(request, outcome);
        };

        LOG_INFO_S(*TlsActivationContext, NKikimrServices::S3_WRAPPER, "Request"
            << ": uuid# " << ctx->GetUUID()
            << ", request# " << ev->Get()->GetRequest());
        func(Client.Get(), ctx->PrepareRequest(ev), callback, ctx);
    }

public:
    TS3ExternalStorage(const Aws::Client::ClientConfiguration& config,
        const Aws::Auth::AWSCredentials& credentials,
        const TString& bucket, const Aws::S3::Model::StorageClass storageClass)
        : Client(new Aws::S3::S3Client(credentials, config))
        , Config(config)
        , Credentials(credentials)
        , Bucket(bucket)
        , StorageClass(storageClass)
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
