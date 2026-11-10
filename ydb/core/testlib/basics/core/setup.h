#pragma once

#include "helpers.h"
#include "runtime.h"

#include <ydb/core/blobstorage/subsystem/interface/subsystem.h>

#include <functional>

namespace NKikimr {

using TBlobStorageSubsystemFactory = std::function<std::unique_ptr<IBlobStorageSubsystem>(ui32 nodeIndex)>;

// Configure before Initialize. Each node gets a separate subsystem instance;
// the caller chooses the implementation and owns any external storage models.
void ConfigureBlobStorage(NActors::TTestActorRuntime& runtime, TBlobStorageSubsystemFactory factory);

// Requires an explicitly configured storage implementation.
void SetupTabletServicesWithBlobStorage(NActors::TTestActorRuntime& runtime, TAppPrepare* app = nullptr);

}
