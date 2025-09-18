#pragma once

#include <util/generic/fwd.h>

namespace NKikimrPQ {

class TPQTabletConfig;
class TMirrorPartitionConfig;

};

namespace NKikimr {

bool CheckPersQueueConfig(const NKikimrPQ::TPQTabletConfig& config, const bool shouldHavePartitionsList = true, TString *error = nullptr);

} // NKikimr
