#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_distributed_storage.h>

#include <ydb/public/api/grpc/draft/ydb_distributed_storage_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/src/client/common_client/impl/client.h>
#include <ydb/public/sdk/cpp/src/client/impl/internal/make_request/make.h>

namespace NYdb::inline Dev::NDistributedStorage {

using Ydb::DistributedStorage::ChangeDataCenterStateRequest;
using Ydb::DistributedStorage::ChangeDataCenterStateResponse;
using Ydb::DistributedStorage::V1::DistributedStorageService;

class TDistributedStorageClient::TImpl : public TClientImplCommon<TImpl> {
public:
    using TClientImplCommon::TClientImplCommon;

    TAsyncStatus SetPoolRedundancy(const std::string& name, EPoolRedundancy target, std::optional<uint32_t> generation,
            const TSetPoolRedundancySettings& settings) {
        using TRequest = Ydb::DistributedStorage::SetPoolRedundancyRequest;
        auto request = MakeOperationRequest<TRequest>(settings);
        request.set_storage_pool_name(name);
        request.set_asynchronous_redundancy(target == EPoolRedundancy::OnePlusOne ? TRequest::REDUNDANCY_1_PLUS_1 : TRequest::REDUNDANCY_2_PLUS_2);
        if (generation) {
            request.set_storage_pool_generation(*generation);
        } else {
            request.set_allow_any_storage_pool_generation(true);
        }
        return RunSimple<DistributedStorageService, TRequest, Ydb::DistributedStorage::SetPoolRedundancyResponse>(
            std::move(request), &DistributedStorageService::Stub::AsyncSetPoolRedundancy, TRpcRequestSettings::Make(settings));
    }

    TAsyncStatus ChangeDataCenterState(const std::string& name, ChangeDataCenterStateRequest::Action action,
            bool dryRun, const TChangeDataCenterStateSettings& settings) {
        auto request = MakeOperationRequest<ChangeDataCenterStateRequest>(settings);
        request.set_datacenter_name(name);
        request.set_action(action);
        request.set_dry_run(dryRun);
        return RunSimple<DistributedStorageService, ChangeDataCenterStateRequest, ChangeDataCenterStateResponse>(
            std::move(request), &DistributedStorageService::Stub::AsyncChangeDataCenterState, TRpcRequestSettings::Make(settings));
    }
};

TDistributedStorageClient::TDistributedStorageClient(const TDriver& driver, const TCommonClientSettings& settings)
    : Impl_(std::make_shared<TImpl>(CreateInternalInterface(driver), settings))
{}

TAsyncStatus TDistributedStorageClient::SetPoolRedundancy(const std::string& name, EPoolRedundancy target,
        std::optional<uint32_t> generation, const TSetPoolRedundancySettings& settings) {
    return Impl_->SetPoolRedundancy(name, target, generation, settings);
}

TAsyncStatus TDistributedStorageClient::TakeDownDataCenter(const std::string& name, bool dryRun,
        const TChangeDataCenterStateSettings& settings) {
    return Impl_->ChangeDataCenterState(name, ChangeDataCenterStateRequest::TAKEDOWN, dryRun, settings);
}

TAsyncStatus TDistributedStorageClient::RejoinDataCenter(const std::string& name, const TChangeDataCenterStateSettings& settings) {
    return Impl_->ChangeDataCenterState(name, ChangeDataCenterStateRequest::REJOIN, false, settings);
}

} // namespace NYdb::inline Dev::NDistributedStorage
