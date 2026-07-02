#pragma once

#include "mon_model.h"

#include <util/generic/string.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

TString RenderMonPage(const TMonPageData& data);

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
