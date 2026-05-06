#pragma once

#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/schemeshard/schemeshard_path.h>
#include <ydb/core/tx/schemeshard/schemeshard__operation_part.h>
#include <ydb/core/tx/schemeshard/schemeshard__operation_common.h>

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

void ScheduleSendTopicCloudEvent(
    const NKikimrSchemeOp::TModifyScheme& operation,
    TOperationContext& context,
    NKikimrScheme::EStatus status,
    const TString& reason,
    const TString& userSID = TString(),
    const TString& peerName = TString());

void SendTopicCloudEvent(
    const NKikimrSchemeOp::TModifyScheme& operation,
    TSchemeShard* ss,
    const TActorContext& ctx,
    NKikimrScheme::EStatus status,
    const TString& reason,
    const TString& userSID = TString(),
    const TString& peerName = TString());

void FinishWithError(
    TProposeResponse* result,
    const NKikimrSchemeOp::TModifyScheme& operation,
    NKikimrScheme::EStatus status,
    const TString& errStr,
    TOperationContext& context);

} // NKikimr::NSchemeShard
