#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_keyvalue.h>

#include <ydb/public/api/grpc/ydb_keyvalue_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/src/client/common_client/impl/client.h>
#include <ydb/public/sdk/cpp/src/client/impl/internal/make_request/make.h>

namespace NYdb::inline Dev::NKeyValue {

    class TKeyValueClient::TImpl: public TClientImplCommon<TKeyValueClient::TImpl> {
    public:
        TImpl(std::shared_ptr<TGRpcConnectionsImpl> connections, const TCommonClientSettings& settings)
            : TClientImplCommon(std::move(connections), settings)
        {
        }

        TAsyncStatus DropVolume(const std::string& path, const TDropVolumeSettings& settings) {
            auto request = MakeOperationRequest<Ydb::KeyValue::DropVolumeRequest>(settings);
            request.set_path(TStringType{path});

            return RunSimple<Ydb::KeyValue::V1::KeyValueService, Ydb::KeyValue::DropVolumeRequest, Ydb::KeyValue::DropVolumeResponse>(
                std::move(request),
                &Ydb::KeyValue::V1::KeyValueService::Stub::AsyncDropVolume,
                TRpcRequestSettings::Make(settings));
        }
    };

    TKeyValueClient::TKeyValueClient(const TDriver& driver, const TCommonClientSettings& settings)
        : Impl_(new TImpl(CreateInternalInterface(driver), settings))
    {
    }

    TAsyncStatus TKeyValueClient::DropVolume(const std::string& path, const TDropVolumeSettings& settings) {
        return Impl_->DropVolume(path, settings);
    }

} // namespace NYdb::inline Dev::NKeyValue
