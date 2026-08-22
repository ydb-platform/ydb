#pragma once

#include "abstract.h"

namespace NKikimr::NWrappers {

IActor* CreateStorageWrapper(NExternalStorage::IExternalStorageOperator::TPtr storage);

} // NKikimr::NWrappers
