#pragma once

#include "mon_model.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/public.h>

#include <util/generic/string.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

TString RenderMonPage(
    const TMonPageData& data,
    const TVChunkConfigs& vChunkConfigs,
    const ITouchedProvider& touchedProvider);

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
