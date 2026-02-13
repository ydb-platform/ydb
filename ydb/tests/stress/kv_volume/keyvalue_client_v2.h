#pragma once

#include "keyvalue_client.h"

#include <ydb/public/api/grpc/ydb_keyvalue_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_keyvalue_v2.grpc.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <grpcpp/client_context.h>

#include <functional>
#include <memory>

namespace NKvVolumeStress {

class TKeyValueClientV2 final : public IKeyValueClient {
public:
    explicit TKeyValueClientV2(const TString& hostPort);

    bool CreateVolume(const TString& path, ui32 partitionCount, const TVector<TString>& channels, TString* error) override;
    bool DropVolume(const TString& path, TString* error) override;
    bool Write(const TString& path, ui32 partitionId, const TVector<std::pair<TString, TString>>& kvPairs, ui32 channel, TString* error) override;
    bool DeleteKey(const TString& path, ui32 partitionId, const TString& key, TString* error) override;
    bool Read(const TString& path, ui32 partitionId, const TString& key, ui32 offset, ui32 size, TString* value, TString* error) override;

private:
    bool CallWithRetry(
        const std::function<grpc::Status(grpc::ClientContext*)>& call,
        const std::function<Ydb::StatusIds::StatusCode()>& getOperationStatus,
        TString* error);

private:
    std::shared_ptr<grpc::Channel> Channel_;
    std::unique_ptr<Ydb::KeyValue::V1::KeyValueService::Stub> StubV1_;
    std::unique_ptr<Ydb::KeyValue::V2::KeyValueService::Stub> StubV2_;
};

} // namespace NKvVolumeStress
