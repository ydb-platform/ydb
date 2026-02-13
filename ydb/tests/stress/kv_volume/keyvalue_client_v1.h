#pragma once

#include <ydb/public/api/grpc/ydb_keyvalue_v1.grpc.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <grpcpp/client_context.h>

#include <functional>
#include <memory>

namespace NKvVolumeStress {

class TKeyValueClientV1 {
public:
    explicit TKeyValueClientV1(const TString& hostPort);

    bool CreateVolume(const TString& path, ui32 partitionCount, const TVector<TString>& channels, TString* error);
    bool DropVolume(const TString& path, TString* error);
    bool Write(const TString& path, ui32 partitionId, const TVector<std::pair<TString, TString>>& kvPairs, ui32 channel, TString* error);
    bool DeleteKey(const TString& path, ui32 partitionId, const TString& key, TString* error);
    bool Read(const TString& path, ui32 partitionId, const TString& key, ui32 offset, ui32 size, TString* value, TString* error);

private:
    bool CallWithRetry(
        const std::function<grpc::Status(grpc::ClientContext*)>& call,
        const std::function<Ydb::StatusIds::StatusCode()>& getOperationStatus,
        TString* error);

private:
    std::shared_ptr<grpc::Channel> Channel_;
    std::unique_ptr<Ydb::KeyValue::V1::KeyValueService::Stub> Stub_;
};

} // namespace NKvVolumeStress
