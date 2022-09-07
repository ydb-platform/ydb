#pragma once

#include "abstract.h"

namespace NKikimr::NWrappers {

IActor* CreateS3Wrapper(NExternalStorage::IExternalStorageOperator::TPtr storage);
} // NKikimr::NWrappers
