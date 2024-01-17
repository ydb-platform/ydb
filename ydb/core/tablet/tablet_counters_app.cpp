#include "tablet_counters_app.h"

#include "tablet_counters_protobuf.h"

#include <ydb/core/protos/counters_schemeshard.pb.h>
#include <ydb/core/protos/counters_datashard.pb.h>
#include <ydb/core/protos/counters_hive.pb.h>
#include <ydb/core/protos/counters_kesus.pb.h>
#include <ydb/core/graph/shard/protos/counters_shard.pb.h>

namespace NKikimr {

THolder<TTabletCountersBase> CreateAppCountersByTabletType(TTabletTypes::EType type) {
    switch (type) {
    case TTabletTypes::SchemeShard:
        return MakeHolder<TAppProtobufTabletCounters<
            NSchemeShard::ESimpleCounters_descriptor,
            NSchemeShard::ECumulativeCounters_descriptor,
            NSchemeShard::EPercentileCounters_descriptor
        >>();
    case TTabletTypes::DataShard:
        return MakeHolder<TAppProtobufTabletCounters<
            NDataShard::ESimpleCounters_descriptor,
            NDataShard::ECumulativeCounters_descriptor,
            NDataShard::EPercentileCounters_descriptor
        >>();
    case TTabletTypes::Hive:
        return MakeHolder<TAppProtobufTabletCounters<
            NHive::ESimpleCounters_descriptor,
            NHive::ECumulativeCounters_descriptor,
            NHive::EPercentileCounters_descriptor
        >>();
    case TTabletTypes::Kesus:
        return MakeHolder<TAppProtobufTabletCounters<
            NKesus::ESimpleCounters_descriptor,
            NKesus::ECumulativeCounters_descriptor,
            NKesus::EPercentileCounters_descriptor
        >>();
    case TTabletTypes::GraphShard:
        return MakeHolder<TAppProtobufTabletCounters<
            NGraphShard::ESimpleCounters_descriptor,
            NGraphShard::ECumulativeCounters_descriptor,
            NGraphShard::EPercentileCounters_descriptor
        >>();
    default:
        return {};
    }
}

}
