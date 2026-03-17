#pragma once

#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/schemeshard/schemeshard_path.h>
#include <ydb/core/tx/schemeshard/schemeshard__operation_part.h>

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

void ScheduleSendTopicCloudEvent(
    const NKikimrSchemeOp::TModifyScheme& operation,
    TOperationContext& context,
    NKikimrScheme::EStatus status,
    const TString& reason);

void FinishWithError(
    TProposeResponse* result,
    const NKikimrSchemeOp::TModifyScheme& operation,
    NKikimrScheme::EStatus status,
    const TString& errStr,
    TOperationContext& context);

} // NKikimr::NSchemeShard
