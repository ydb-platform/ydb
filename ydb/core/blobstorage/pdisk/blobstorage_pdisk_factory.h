#pragma once
#include "defs.h"
#include <ydb/core/blobstorage/pdisk/subsystem/subsystem.h>
#include "blobstorage_pdisk_config.h"
#include "blobstorage_pdisk_defs.h"
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NActors {
    struct TActorSetupCmd;
}

namespace NKikimr {

std::unique_ptr<IPDiskSubsystem> CreatePDiskSubsystem();

} // NKikimr
