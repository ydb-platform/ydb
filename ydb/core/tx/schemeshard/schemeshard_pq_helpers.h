#pragma once

#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/schemeshard/schemeshard_path.h>
#include <ydb/core/tx/schemeshard/schemeshard__operation_part.h>
#include <ydb/core/tx/schemeshard/schemeshard__operation_common.h>

namespace NKikimrScheme {
class TEvModifySchemeTransaction;
class TEvModifySchemeTransactionResult;
}

namespace NKikimr::NSchemeShard {

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
    TSchemeShard* ss,
    const TActorContext& ctx,
    NKikimrScheme::EStatus status,
    const TString& reason,
    const TString& userSID,
    const TString& peerName);

void SendTopicCloudEventIfNeeded(
    const NKikimrScheme::TEvModifySchemeTransaction& record,
    const NKikimrScheme::TEvModifySchemeTransactionResult& response,
    TSchemeShard* ss,
    const TString& peerName,
    const TString& userSID);

} // NKikimr::NSchemeShard
