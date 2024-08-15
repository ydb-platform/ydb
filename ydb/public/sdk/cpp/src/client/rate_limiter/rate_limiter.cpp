#include <ydb-cpp-sdk/client/rate_limiter/rate_limiter.h>

#define INCLUDE_YDB_INTERNAL_H
#include <src/client/impl/ydb_internal/make_request/make.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/api/grpc/ydb_rate_limiter_v1.grpc.pb.h>
#include <src/client/common_client/impl/client.h>

namespace NYdb::NRateLimiter {

TListResourcesResult::TListResourcesResult(TStatus status, std::vector<std::string> paths)
    : TStatus(std::move(status))
    , ResourcePaths_(std::move(paths))
{
}

TDescribeResourceResult::TDescribeResourceResult(TStatus status, const Ydb::RateLimiter::DescribeResourceResult& result)
    : TStatus(std::move(status))
    , ResourcePath_(result.resource().resource_path())
    , HierarchicalDrrProps_(result.resource().hierarchical_drr())
{
}

TDescribeResourceResult::THierarchicalDrrProps::THierarchicalDrrProps(const Ydb::RateLimiter::HierarchicalDrrSettings& settings) {
    if (settings.max_units_per_second()) {
        MaxUnitsPerSecond_ = settings.max_units_per_second();
    }

    if (settings.max_burst_size_coefficient()) {
        MaxBurstSizeCoefficient_ = settings.max_burst_size_coefficient();
    }

    if (settings.prefetch_coefficient()) {
        PrefetchCoefficient_ = settings.prefetch_coefficient();
    }

    if (settings.prefetch_watermark()) {
        PrefetchWatermark_ = settings.prefetch_watermark();
    }
}

class TRateLimiterClient::TImpl : public TClientImplCommon<TRateLimiterClient::TImpl> {
public:
    TImpl(std::shared_ptr<TGRpcConnectionsImpl> connections, const TCommonClientSettings& settings)
        : TClientImplCommon(std::move(connections), settings)
    {
    }

    template <class TRequest, class TSettings>
    static TRequest MakePropsCreateOrAlterRequest(const std::string& coordinationNodePath, const std::string& resourcePath, const TSettings& settings) {
        TRequest request = MakeOperationRequest<TRequest>(settings);
        request.set_coordination_node_path(TStringType{coordinationNodePath});

        Ydb::RateLimiter::Resource& resource = *request.mutable_resource();
        resource.set_resource_path(TStringType{resourcePath});

        Ydb::RateLimiter::HierarchicalDrrSettings& hdrr = *resource.mutable_hierarchical_drr();
        if (settings.MaxUnitsPerSecond_) {
            hdrr.set_max_units_per_second(*settings.MaxUnitsPerSecond_);
        }
        if (settings.MaxBurstSizeCoefficient_) {
            hdrr.set_max_burst_size_coefficient(*settings.MaxBurstSizeCoefficient_);
        }
        if (settings.PrefetchCoefficient_) {
            hdrr.set_prefetch_coefficient(*settings.PrefetchCoefficient_);
        }
        if (settings.PrefetchWatermark_) {
            hdrr.set_prefetch_watermark(*settings.PrefetchWatermark_);
        }

        return request;
    }

    TAsyncStatus CreateResource(const std::string& coordinationNodePath, const std::string& resourcePath, const TCreateResourceSettings& settings) {
        auto request = MakePropsCreateOrAlterRequest<Ydb::RateLimiter::CreateResourceRequest>(coordinationNodePath, resourcePath, settings);

        return RunSimple<Ydb::RateLimiter::V1::RateLimiterService, Ydb::RateLimiter::CreateResourceRequest, Ydb::RateLimiter::CreateResourceResponse>(
            std::move(request),
            &Ydb::RateLimiter::V1::RateLimiterService::Stub::AsyncCreateResource,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncStatus AlterResource(const std::string& coordinationNodePath, const std::string& resourcePath, const TAlterResourceSettings& settings) {
        auto request = MakePropsCreateOrAlterRequest<Ydb::RateLimiter::AlterResourceRequest>(coordinationNodePath, resourcePath, settings);

        return RunSimple<Ydb::RateLimiter::V1::RateLimiterService, Ydb::RateLimiter::AlterResourceRequest, Ydb::RateLimiter::AlterResourceResponse>(
            std::move(request),
            &Ydb::RateLimiter::V1::RateLimiterService::Stub::AsyncAlterResource,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncStatus DropResource(const std::string& coordinationNodePath, const std::string& resourcePath, const TDropResourceSettings& settings) {
        auto request = MakeOperationRequest<Ydb::RateLimiter::DropResourceRequest>(settings);
        request.set_coordination_node_path(TStringType{coordinationNodePath});
        request.set_resource_path(TStringType{resourcePath});

        return RunSimple<Ydb::RateLimiter::V1::RateLimiterService, Ydb::RateLimiter::DropResourceRequest, Ydb::RateLimiter::DropResourceResponse>(
            std::move(request),
            &Ydb::RateLimiter::V1::RateLimiterService::Stub::AsyncDropResource,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncListResourcesResult ListResources(const std::string& coordinationNodePath, const std::string& resourcePath, const TListResourcesSettings& settings) {
        auto request = MakeOperationRequest<Ydb::RateLimiter::ListResourcesRequest>(settings);
        request.set_coordination_node_path(TStringType{coordinationNodePath});
        request.set_resource_path(TStringType{resourcePath});
        request.set_recursive(settings.Recursive_);

        auto promise = NThreading::NewPromise<TListResourcesResult>();

        auto extractor = [promise]
            (google::protobuf::Any* any, TPlainStatus status) mutable {
                std::vector<std::string> list;
                if (any) {
                    Ydb::RateLimiter::ListResourcesResult result;
                    any->UnpackTo(&result);
                    list.reserve(result.resource_paths_size());
                    for (const std::string& path : result.resource_paths()) {
                        list.push_back(path);
                    }
                }

                TListResourcesResult val(TStatus(std::move(status)), std::move(list));
                promise.SetValue(std::move(val));
            };

        Connections_->RunDeferred<Ydb::RateLimiter::V1::RateLimiterService, Ydb::RateLimiter::ListResourcesRequest, Ydb::RateLimiter::ListResourcesResponse>(
            std::move(request),
            extractor,
            &Ydb::RateLimiter::V1::RateLimiterService::Stub::AsyncListResources,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings));

        return promise.GetFuture();
    }

    TAsyncDescribeResourceResult DescribeResource(const std::string& coordinationNodePath, const std::string& resourcePath, const TDescribeResourceSettings& settings) {
        auto request = MakeOperationRequest<Ydb::RateLimiter::DescribeResourceRequest>(settings);
        request.set_coordination_node_path(TStringType{coordinationNodePath});
        request.set_resource_path(TStringType{resourcePath});

        auto promise = NThreading::NewPromise<TDescribeResourceResult>();

        auto extractor = [promise]
            (google::protobuf::Any* any, TPlainStatus status) mutable {
                Ydb::RateLimiter::DescribeResourceResult result;
                if (any) {
                    any->UnpackTo(&result);
                }

                TDescribeResourceResult val(TStatus(std::move(status)), result);
                promise.SetValue(std::move(val));
            };

        Connections_->RunDeferred<Ydb::RateLimiter::V1::RateLimiterService, Ydb::RateLimiter::DescribeResourceRequest, Ydb::RateLimiter::DescribeResourceResponse>(
            std::move(request),
            extractor,
            &Ydb::RateLimiter::V1::RateLimiterService::Stub::AsyncDescribeResource,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings));

        return promise.GetFuture();
    }

    TAsyncStatus AcquireResource(const std::string& coordinationNodePath, const std::string& resourcePath, const TAcquireResourceSettings& settings) {
        auto request = MakeOperationRequest<Ydb::RateLimiter::AcquireResourceRequest>(settings);
        request.set_coordination_node_path(TStringType{coordinationNodePath});
        request.set_resource_path(TStringType{resourcePath});

        if (settings.IsUsedAmount_) {
            request.set_used(settings.Amount_.value());
        } else {
            request.set_required(settings.Amount_.value());
        }

        return RunSimple<Ydb::RateLimiter::V1::RateLimiterService, Ydb::RateLimiter::AcquireResourceRequest, Ydb::RateLimiter::AcquireResourceResponse>(
            std::move(request),
            &Ydb::RateLimiter::V1::RateLimiterService::Stub::AsyncAcquireResource,
            TRpcRequestSettings::Make(settings));
    }
};

TRateLimiterClient::TRateLimiterClient(const TDriver& driver, const TCommonClientSettings& settings)
    : Impl_(std::make_shared<TImpl>(CreateInternalInterface(driver), settings))
{
}

TAsyncStatus TRateLimiterClient::CreateResource(const std::string& coordinationNodePath, const std::string& resourcePath, const TCreateResourceSettings& settings) {
    return Impl_->CreateResource(coordinationNodePath, resourcePath, settings);
}

TAsyncStatus TRateLimiterClient::AlterResource(const std::string& coordinationNodePath, const std::string& resourcePath, const TAlterResourceSettings& settings) {
    return Impl_->AlterResource(coordinationNodePath, resourcePath, settings);
}

TAsyncStatus TRateLimiterClient::DropResource(const std::string& coordinationNodePath, const std::string& resourcePath, const TDropResourceSettings& settings) {
    return Impl_->DropResource(coordinationNodePath, resourcePath, settings);
}

TAsyncListResourcesResult TRateLimiterClient::ListResources(const std::string& coordinationNodePath, const std::string& resourcePath, const TListResourcesSettings& settings) {
    return Impl_->ListResources(coordinationNodePath, resourcePath, settings);
}

TAsyncDescribeResourceResult TRateLimiterClient::DescribeResource(const std::string& coordinationNodePath, const std::string& resourcePath, const TDescribeResourceSettings& settings) {
    return Impl_->DescribeResource(coordinationNodePath, resourcePath, settings);
}

TAsyncStatus TRateLimiterClient::AcquireResource(const std::string& coordinationNodePath, const std::string& resourcePath, const TAcquireResourceSettings& settings) {
    return Impl_->AcquireResource(coordinationNodePath, resourcePath, settings);
}

} // namespace NYdb::NRateLimiter
