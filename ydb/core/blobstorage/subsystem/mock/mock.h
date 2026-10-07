#pragma once

#include <ydb/core/blobstorage/subsystem/subsystem.h>
#include <ydb/core/blobstorage/dsproxy/mock/model.h>
#include <util/generic/vector.h>

namespace NKikimr {

// Keep the models outside the actor system to preserve data across restarts.
std::unique_ptr<IBlobStorageSubsystem> CreateMockBlobStorageSubsystem(
    TVector<TIntrusivePtr<NFake::TProxyDS>> groups, ui32 poolId = 0);

} // namespace NKikimr
