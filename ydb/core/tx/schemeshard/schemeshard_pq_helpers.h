#pragma once

#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/schemeshard/schemeshard_path.h>

namespace NKikimr::NSchemeShard {

class TSchemeShard;
class TPath;

TPath DatabasePathFromModifySchemeOperation(
    TSchemeShard* ss,
    const NKikimrSchemeOp::TModifyScheme& operation);

std::tuple<TString, TString, TString> GetDatabaseCloudIds(const TPath& databasePath);

class PQGroupReserve {
public:
    PQGroupReserve(const ::NKikimrPQ::TPQTabletConfig& tabletConfig, ui64 partitions, ui64 currentStorageUsage = 0) {
        Storage = NKikimrPQ::TPQTabletConfig::METERING_MODE_REQUEST_UNITS == tabletConfig.GetMeteringMode()
            ? currentStorageUsage : partitions * NPQ::TopicPartitionReserveSize(tabletConfig);
        Throughput = partitions * NPQ::TopicPartitionReserveThroughput(tabletConfig);
    }

    ui64 Storage;
    ui64 Throughput;
};

void SendTopicCloudEvent(
    const NKikimrSchemeOp::TModifyScheme& operation,
    NKikimrScheme::EStatus status,
    const TString& reason,
    TSchemeShard* ss,
    const TString& peerName,
    const TString& userSID,
    const TString& maskedToken,
    ui64 txId);

} // NKikimr::NSchemeShard
