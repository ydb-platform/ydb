#include "flat_executor_backup_common.h"

#include <util/string/hex.h>
#include <util/string/cast.h>

namespace NKikimr::NTabletFlatExecutor::NBackup {

TString FormatChecksumDigest(const NOpenSsl::NSha256::TDigest& digest) {
    return to_lower(HexEncode(digest.data(), digest.size()));
}

TString ComputeInitialChecksum(ui32 generation, ui32 step) {
    TString seed = ToString(generation) + ":" + ToString(step);
    return ComputeChecksum(seed);
}

} // namespace NKikimr::NTabletFlatExecutor::NBackup
