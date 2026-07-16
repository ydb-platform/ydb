#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/test_shard/test_shard.h>

#include <ydb/public/sdk/cpp/src/client/common_client/impl/client.h>
#include <ydb/public/sdk/cpp/src/client/impl/internal/make_request/make.h>

#include <ydb/public/api/grpc/ydb_test_shard_v1.grpc.pb.h>
#include <ydb/public/api/protos/ydb_test_shard.pb.h>

namespace NYdb::NTestShardSet {

class TTestShardSetClient::TImpl : public TClientImplCommon<TTestShardSetClient::TImpl> {
public:
    TImpl(std::shared_ptr<TGRpcConnectionsImpl> connections, const TCommonClientSettings& settings)
        : TClientImplCommon(std::move(connections), settings)
    {}

    TAsyncCreateTestShardSetResult CreateTestShardSet(const std::string& path,
            const std::vector<std::string>& channels, uint32_t count,
            const std::string& config,
            const TCreateTestShardSetSettings& settings) {
        auto request = MakeOperationRequest<Ydb::TestShardSet::CreateTestShardSetRequest>(settings);
        request.set_path(path);
        for (const auto& channel : channels) {
            request.add_channels(channel);
        }
        request.set_count(count);
        if (!config.empty()) {
            request.set_config(config);
        }

        auto promise = NThreading::NewPromise<TCreateTestShardSetResult>();

        auto extractor = [promise] (google::protobuf::Any* any, TPlainStatus status) mutable {
            std::vector<uint64_t> tabletIds;
            if (any) {
                Ydb::TestShardSet::CreateTestShardSetResult result;
                if (any->UnpackTo(&result)) {
                    tabletIds.reserve(result.tablet_ids_size());
                    for (int i = 0; i < result.tablet_ids_size(); ++i) {
                        tabletIds.push_back(result.tablet_ids(i));
                    }
                }
            }
            promise.SetValue(TCreateTestShardSetResult(TStatus(std::move(status)), std::move(tabletIds)));
        };

        Connections_->RunDeferred<Ydb::TestShardSet::V1::TestShardSetService, Ydb::TestShardSet::CreateTestShardSetRequest, Ydb::TestShardSet::CreateTestShardSetResponse>(
            std::move(request),
            extractor,
            &Ydb::TestShardSet::V1::TestShardSetService::Stub::AsyncCreateTestShardSet,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings));

        return promise.GetFuture();
    }

    TAsyncStatus DeleteTestShardSet(const std::string& path,
            const TDeleteTestShardSetSettings& settings) {
        auto request = MakeOperationRequest<Ydb::TestShardSet::DeleteTestShardSetRequest>(settings);
        request.set_path(path);

        return RunSimple<Ydb::TestShardSet::V1::TestShardSetService, Ydb::TestShardSet::DeleteTestShardSetRequest, Ydb::TestShardSet::DeleteTestShardSetResponse>(
            std::move(request),
            &Ydb::TestShardSet::V1::TestShardSetService::Stub::AsyncDeleteTestShardSet,
            TRpcRequestSettings::Make(settings));
    }
};

TTestShardSetClient::TTestShardSetClient(const TDriver& driver, const TCommonClientSettings& settings)
    : Impl_(new TImpl(CreateInternalInterface(driver), settings))
{}

TTestShardSetClient::~TTestShardSetClient() = default;

TAsyncCreateTestShardSetResult TTestShardSetClient::CreateTestShardSet(const std::string& path,
        const std::vector<std::string>& channels, uint32_t count,
        const std::string& config,
        const TCreateTestShardSetSettings& settings) {
    return Impl_->CreateTestShardSet(path, channels, count, config, settings);
}

TAsyncStatus TTestShardSetClient::DeleteTestShardSet(const std::string& path,
        const TDeleteTestShardSetSettings& settings) {
    return Impl_->DeleteTestShardSet(path, settings);
}

} // namespace NYdb::NTestShardSet
