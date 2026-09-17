#pragma once

#include "pdisk_mock.h"
#include <ydb/core/blobstorage/pdisk/subsystem/subsystem.h>

#include <functional>
#include <map>

namespace NKikimr {

using TPDiskMockStates = std::map<std::pair<ui32, ui32>, TIntrusivePtr<TPDiskMockState>>;

// The state store belongs to the test and must outlive the node actor systems.
class TMockPDiskSubsystem final : public IPDiskSubsystem {
public:
    using TCreateState = std::function<TIntrusivePtr<TPDiskMockState>(
        ui32, ui32, const TPDiskConfig&)>;
    using TActorCreated = std::function<void(NActors::TActorId)>;

    TMockPDiskSubsystem(TPDiskMockStates* states, TCreateState createState,
        TActorCreated actorCreated = {});

    void Start(const NActors::TActorContext& ctx, ui32 pdiskId,
        const TIntrusivePtr<TPDiskConfig>& config, const NPDisk::TMainKey& mainKey,
        ui32 poolId, ui32 nodeId) override;

private:
    TPDiskMockStates& States;
    TCreateState CreateState;
    TActorCreated ActorCreated;
};

} // namespace NKikimr
