#pragma once

#include <ydb/core/blobstorage/dsproxy/defs.h>
#include <util/stream/null.h>

namespace NKikimr {

constexpr bool IsVerbose = false;
#define CTEST (IsVerbose ? Cerr : Cnull)

} // NKikimr
