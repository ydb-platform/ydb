#pragma once
#include <ydb/core/protos/msgbus_pq.pb.h>

namespace NKikimr {

bool CheckPersQueueConfig(const NKikimrPQ::TPQTabletConfig& config, const bool shouldHavePartitionsList = true, TString *error = nullptr);

} // NKikimr
