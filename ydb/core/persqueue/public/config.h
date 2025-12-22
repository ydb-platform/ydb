#pragma once

#include <util/generic/fwd.h>

namespace NKikimrPQ {

class TPQTabletConfig;
class TPQTabletConfig_TConsumer;
class TMirrorPartitionConfig;
class TMLPStorageSnapshot;
class TMLPStorageWAL;
class TStatusResponse;
class TStatusResponse_TPartResult;

};

namespace NKikimr {

bool CheckPersQueueConfig(const NKikimrPQ::TPQTabletConfig& config, const bool shouldHavePartitionsList = true, TString *error = nullptr);

} // NKikimr
