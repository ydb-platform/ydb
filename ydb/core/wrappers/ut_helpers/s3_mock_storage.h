#pragma once

#include "s3_mock.h"

#include <ydb/core/wrappers/abstract.h>

namespace NKikimr::NWrappers::NTestHelpers {

// A storage config whose operator answers from the mock storage inside the actor system of the requester, as the
// S3 client would from the HTTP mock: the same objects, the same checks, errors of the same S3 types
NExternalStorage::IExternalStorageConfig::TPtr MakeS3MockStorageConfig(
    std::shared_ptr<TS3Mock::TStorage> storage, const TString& bucket, bool useVirtualAddressing);

} // NKikimr::NWrappers::NTestHelpers
