#pragma once

#include <ydb/core/blobstorage/subsystem/interface/subsystem.h>
#include <ydb/library/actors/core/mailbox.h>

#include <util/generic/ptr.h>
#include <util/system/types.h>

namespace NKikimr {

struct TNodeWardenConfig;

std::unique_ptr<IBlobStorageSubsystem> CreateBlobStorageSubsystem(
    TIntrusivePtr<TNodeWardenConfig> config, ui32 poolId = 0,
    NActors::TMailboxType::EType mailboxType = NActors::TMailboxType::ReadAsFilled);

} // namespace NKikimr
