#pragma once

#include <optional>

#include <util/generic/fwd.h>
#include <util/generic/hash_set.h>
#include <util/system/compiler.h>

#include <ydb/library/actors/core/actor.h>

namespace NKikimrPQ {

class TPQConfig;
class TPQTabletConfig;
class TPQTabletConfig_TConsumer;
class TPQTabletConfig_TPartition;
class TMirrorPartitionConfig;
class TMLPStorageSnapshot;
class TMLPStorageWAL;
class TStatusResponse;
class TStatusResponse_TPartResult;

} // namespace NKikimrPQ

namespace NKikimr {

bool CheckPersQueueConfig(const NKikimrPQ::TPQTabletConfig& config, const bool shouldHavePartitionsList = true, TString *error = nullptr);

namespace NPQ {

bool IsQuotingEnabled(const NKikimrPQ::TPQConfig& config, bool isLocalDC);
bool IsTopicMessagesBatchingEnabled(const NActors::TActorContext& ctx);
bool DetailedMetricsAreEnabled(const NKikimrPQ::TPQTabletConfig& config);
const NKikimrPQ::TPQTabletConfig_TPartition* GetPartitionConfigFromAllPartitions(const NKikimrPQ::TPQTabletConfig& config Y_LIFETIME_BOUND, const ui32 partitionId) noexcept;

TString GetDLQTopicPath(const NKikimrPQ::TPQTabletConfig_TConsumer& consumer);

THashSet<TString> CollectDLQTopicPaths(
    const NKikimrPQ::TPQTabletConfig& config,
    const TString& database,
    std::optional<ui64> modificationVersion = std::nullopt
);

} // namespace NPQ

} // namespace NKikimr
