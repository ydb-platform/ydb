#pragma once

#include "public.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/session/public.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

#include <ydb/core/nbs/nbs1_compat_api/cloud/blockstore/libs/service/service.h>

#include <library/cpp/logger/log.h>

namespace NYdb::NBS::NBlockStore {

// Adapts classic RPCs to host-local partitions. Session ownership stays with
// each partition; Start/Stop only control request admission.
struct INbsBlockStoreFacade: public NNbs1CompatApi::NBlockStore::IBlockStore
{
    // Opens admission without changing partition sessions.
    void Start() override = 0;

    // Closes admission; does not drain admitted I/O or remove sessions.
    void Stop() override = 0;

    // Publishes a matched session and control target, replacing the same
    // DiskId. Returns the new incarnation's token. The previous owner stops its
    // backend; old control targets must remain callable by requests retaining
    // them.
    virtual TResultOrError<TString> RegisterVolume(
        NStorage::NPartitionDirect::TPartitionSessionPtr session,
        NStorage::NPartitionDirect::IPartitionSessionControlPtr control) = 0;

    // Removes only the specified incarnation; stale tokens are harmless.
    virtual void UnregisterVolume(
        const TString& diskId,
        const TString& registrationId) = 0;
};

// Creates one facade shared by TNbsService and transport services.
INbsBlockStoreFacadePtr CreateNbsBlockStoreFacade(TLog log);

}   // namespace NYdb::NBS::NBlockStore
