#pragma once

#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclogformat.h>

#include <vector>

namespace NKikimr {

namespace NSyncLog {

using TPhantomFlags = std::vector<TLogoBlobRec>;

} // namespace NSyncLog

} // namespace NKikimr
