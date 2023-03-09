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

NKikimrPQ::TPQTabletConfig MakeConfig(ui64 version,
                                      const TVector<TCreateConsumerParams>& consumers,
                                      ui32 partitionsCount = 1);

NKikimrPQ::TBootstrapConfig MakeBootstrapConfig();

}
