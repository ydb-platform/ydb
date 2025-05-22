#pragma once

#include <ydb/core/protos/pqconfig.pb.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

namespace NKikimr::NPQ::NHelpers {

struct TCreateConsumerParams {
    TString Consumer;
    ui64 Offset = 0;
    ui32 Generation = 0;
    ui32 Step = 0;
    TString Session;
    ui64 OffsetRewindSum = 0;
    ui64 ReadRuleGeneration = 0;
};

struct TPartitionParams {
    ui32 Id = Max<ui32>();
    ui64 TabletId = Max<ui64>();
    TVector<ui32> Children;
    TVector<ui32> Parents;
};

struct TMakeConfigParams {
    ui64 Version = 0;
    TVector<TCreateConsumerParams> Consumers;
    TVector<TPartitionParams> Partitions;
    TVector<TPartitionParams> AllPartitions;
    ui32 PartitionsCount = 1;
    NKikimrPQ::TPQTabletConfig::EMeteringMode MeteringMode = NKikimrPQ::TPQTabletConfig::METERING_MODE_REQUEST_UNITS;
    bool HugeConfig = false;
};

NKikimrPQ::TPQTabletConfig MakeConfig(const TMakeConfigParams& params);

NKikimrPQ::TPQTabletConfig MakeConfig(ui64 version,
                                      const TVector<TCreateConsumerParams>& consumers,
                                      ui32 partitionsCount = 1,
                                      NKikimrPQ::TPQTabletConfig::EMeteringMode meteringMode = NKikimrPQ::TPQTabletConfig::METERING_MODE_REQUEST_UNITS);

NKikimrPQ::TBootstrapConfig MakeBootstrapConfig();

}
