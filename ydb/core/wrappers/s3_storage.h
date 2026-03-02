#pragma once

#ifndef KIKIMR_DISABLE_S3_OPS

#include "abstract.h"

#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/auth/AWSCredentials.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/client/AWSClient.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/client/ClientConfiguration.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/S3Client.h>

#include <ydb/library/actors/core/log.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/generic/typetraits.h>

#include <condition_variable>
#include <mutex>

namespace NKikimr::NWrappers::NExternalStorage {

Y_HAS_MEMBER(GetBucket, Bucket); // Generates a helper class THasBucket

class TS3ExternalStorage: public IExternalStorageOperator {
public:
    struct TS3CountersRoot;

    struct TS3RequestCounters: public TAtomicRefCount<TS3RequestCounters> {
        TS3RequestCounters(const TS3CountersRoot* parent, NMonitoring::TDynamicCounterPtr requestGroup)
            : Parent(parent)
            , RequestGroup(std::move(requestGroup))
        {
        }

        NMonitoring::THistogramCounter* GetLatency() const;
        NMonitoring::TCounterForPtr* GetBytesWritten() const;
        NMonitoring::TCounterForPtr* GetBytesRead() const;
        NMonitoring::TCounterForPtr* GetCodeCounter(const TString& code) const;

    private:
        const TS3CountersRoot* Parent = nullptr;
        NMonitoring::TDynamicCounterPtr RequestGroup;
        mutable NMonitoring::THistogramPtr LatencyHist;
        mutable NMonitoring::TDynamicCounters::TCounterPtr BytesWritten;
        mutable NMonitoring::TDynamicCounters::TCounterPtr BytesRead;
        mutable NMonitoring::TDynamicCounters::TCounterPtr CodeOk;
    };

    struct TS3CountersRoot {
        TS3CountersRoot() = default;

        explicit TS3CountersRoot(NMonitoring::TDynamicCounterPtr root)
            : Root(std::move(root))
        {
        }

        template <typename TEvRequest>
        TIntrusivePtr<TS3RequestCounters> GetRequestCounters(const TString& bucket) const {
            if (!Root) {
                return nullptr;
            }

            TString requestName(TEvRequest::RequestName);
            std::lock_guard<std::mutex> g(Mutex);
            if (auto it = Cache.find(requestName); it != Cache.end()) {
                return it->second;
            }

            NMonitoring::TDynamicCounterPtr reqRoot;
            if (Root) {
                reqRoot = Root->GetSubgroup("request", requestName);
                constexpr bool requestHasBucket = THasBucket<typename TEvRequest::TRequest>::value;
                if constexpr (requestHasBucket) {
                    reqRoot = reqRoot->GetSubgroup("bucket", bucket);
                }
            }

            TIntrusivePtr<TS3RequestCounters> counters = new TS3RequestCounters(this, std::move(reqRoot));
            Cache.emplace(std::move(requestName), counters);
            return counters;
        }

        NMonitoring::TDynamicCounterPtr Root;
        mutable std::mutex Mutex;
        mutable THashMap<TString, TIntrusivePtr<TS3RequestCounters>> Cache;
    };

private:
    THolder<Aws::S3::S3Client> Client;
    const Aws::Client::ClientConfiguration Config;
    const Aws::Auth::AWSCredentials Credentials;
    const TString Bucket;
    const Aws::S3::Model::StorageClass StorageClass = Aws::S3::Model::StorageClass::STANDARD;
    bool Verbose = true;
    TS3CountersRoot Counters;

    mutable std::mutex RunningQueriesMutex;
    mutable std::condition_variable RunningQueriesNotifier;
    mutable int RunningQueriesCount = 0;

    template <typename TRequest, typename TOutcome>
    using THandler = std::function<void(const Aws::S3::S3Client*, const TRequest&, const TOutcome&, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&)>;

    template <typename TRequest, typename TOutcome>
    using TFunc = std::function<void(const Aws::S3::S3Client*, const TRequest&, THandler<TRequest, TOutcome>, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&)>;

    template <typename TEvRequest>
    TIntrusivePtr<TS3RequestCounters> GetRequestCounters() const {
        return Counters.GetRequestCounters<TEvRequest>(Bucket);
    }

    template <typename TEvRequest, typename TEvResponse, template <typename...> typename TContext>
    void Call(typename TEvRequest::TPtr& ev, TFunc<typename TEvRequest::TRequest, typename TEvResponse::TOutcome> func) const {
        using TCtx = TContext<TEvRequest, TEvResponse>;
        ev->Get()->MutableRequest().WithBucket(Bucket);

        auto ctx = std::make_shared<TCtx>(TlsActivationContext->ActorSystem(), ev->Sender, ev->Get()->GetRequestContext(), StorageClass, ReplyAdapter, GetRequestCounters<TEvRequest>());
        auto callback = [this](
            const Aws::S3::S3Client*,
            const typename TEvRequest::TRequest& request,
            const typename TEvResponse::TOutcome& outcome,
            const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context)
        {
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
    TS3ExternalStorage(
            const Aws::Client::ClientConfiguration& config,
            const Aws::Auth::AWSCredentials& credentials,
            const TString& bucket,
            NMonitoring::TDynamicCounterPtr counters,
            const Aws::S3::Model::StorageClass storageClass,
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
        , Counters(std::move(counters))
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
