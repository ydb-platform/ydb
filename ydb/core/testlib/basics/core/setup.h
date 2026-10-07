#pragma once

#include "helpers.h"
#include "runtime.h"
#include <ydb/core/blobstorage/subsystem/subsystem.h>

namespace NKikimr {

using TBlobStorageSubsystemFactory = std::function<std::unique_ptr<IBlobStorageSubsystem>(ui32 nodeIndex)>;

// Call before Initialize. Each node receives its own subsystem instance.
void ConfigureBlobStorage(NActors::TTestActorRuntime& runtime, TBlobStorageSubsystemFactory factory);

// Requires explicit BlobStorage configuration; never falls back to production.
void SetupTabletServicesWithBlobStorage(NActors::TTestActorRuntime& runtime, TAppPrepare* app = nullptr);

}
