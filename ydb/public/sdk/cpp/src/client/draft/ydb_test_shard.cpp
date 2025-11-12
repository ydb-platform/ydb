#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_test_shard.h>

#include <ydb/public/sdk/cpp/src/client/common_client/impl/client.h>
#include <ydb/public/sdk/cpp/src/client/impl/internal/make_request/make.h>

#include <ydb/public/api/grpc/draft/ydb_test_shard_v1.grpc.pb.h>
#include <ydb/public/api/protos/draft/ydb_test_shard.pb.h>

namespace NYdb::inline Dev::NTestShard {

class TTestShardClient::TImpl : public TClientImplCommon<TTestShardClient::TImpl> {
public:
    TImpl(std::shared_ptr<TGRpcConnectionsImpl> connections, const TCommonClientSettings& settings)
        : TClientImplCommon(std::move(connections), settings)
    {}

    TAsyncCreateTestShardResult CreateTestShard(uint64_t ownerIdx,
            const std::vector<std::string>& channels, uint32_t count,
            const std::string& config, const std::string& database,
            const TCreateTestShardSettings& settings) {
        auto request = MakeOperationRequest<Ydb::TestShard::CreateTestShardRequest>(settings);
        request.set_owner_idx(ownerIdx);
        for (const auto& channel : channels) {
            request.add_channels(channel);
        }
        if (count > 1) {
            request.set_count(count);
        }
        if (!config.empty()) {
            request.set_config(config);
        }
        if (!database.empty()) {
            request.set_database(database);
        }

        auto promise = NThreading::NewPromise<TCreateTestShardResult>();

        auto extractor = [promise] (google::protobuf::Any* any, TPlainStatus status) mutable {
            std::vector<uint64_t> tabletIds;
            if (any) {
                Ydb::TestShard::CreateTestShardResult result;
                if (any->UnpackTo(&result)) {
                    tabletIds.reserve(result.tablet_ids_size());
                    for (int i = 0; i < result.tablet_ids_size(); ++i) {
                        tabletIds.push_back(result.tablet_ids(i));
                    }
                }
            }
            promise.SetValue(TCreateTestShardResult(TStatus(std::move(status)), std::move(tabletIds)));
        };

        Connections_->RunDeferred<Ydb::TestShard::V1::TestShardService, Ydb::TestShard::CreateTestShardRequest, Ydb::TestShard::CreateTestShardResponse>(
            std::move(request),
            extractor,
            &Ydb::TestShard::V1::TestShardService::Stub::AsyncCreateTestShard,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings));

        return promise.GetFuture();
    }

    TAsyncStatus DeleteTestShard(uint64_t ownerIdx, uint32_t count,
            const std::string& database,
            const TDeleteTestShardSettings& settings) {
        auto request = MakeOperationRequest<Ydb::TestShard::DeleteTestShardRequest>(settings);
        request.set_owner_idx(ownerIdx);
        if (count > 1) {
            request.set_count(count);
        }
        if (!database.empty()) {
            request.set_database(database);
        }

        return RunSimple<Ydb::TestShard::V1::TestShardService, Ydb::TestShard::DeleteTestShardRequest, Ydb::TestShard::DeleteTestShardResponse>(
            std::move(request),
            &Ydb::TestShard::V1::TestShardService::Stub::AsyncDeleteTestShard,
            TRpcRequestSettings::Make(settings));
    }
};

TTestShardClient::TTestShardClient(const TDriver& driver, const TCommonClientSettings& settings)
    : Impl_(new TImpl(CreateInternalInterface(driver), settings))
{}

TTestShardClient::~TTestShardClient() = default;

TAsyncCreateTestShardResult TTestShardClient::CreateTestShard(uint64_t ownerIdx,
        const std::vector<std::string>& channels, uint32_t count,
        const std::string& config, const std::string& database,
        const TCreateTestShardSettings& settings) {
    return Impl_->CreateTestShard(ownerIdx, channels, count, config, database, settings);
}

TAsyncStatus TTestShardClient::DeleteTestShard(uint64_t ownerIdx,
        uint32_t count, const std::string& database,
        const TDeleteTestShardSettings& settings) {
    return Impl_->DeleteTestShard(ownerIdx, count, database, settings);
}

} // namespace NYdb::inline Dev::NTestShard
