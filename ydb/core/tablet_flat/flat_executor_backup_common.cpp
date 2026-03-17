#include "flat_executor_backup_common.h"

#include <util/string/hex.h>

namespace NKikimr::NTabletFlatExecutor::NBackup {

TString FormatChecksumDigest(const NOpenSsl::NSha256::TDigest& digest) {
    return to_lower(HexEncode(digest.data(), digest.size()));
}

TString ComputeChecksum(TStringBuf data) {
    return FormatChecksumDigest(NOpenSsl::NSha256::Calc(data));
}

} // namespace NKikimr::NTabletFlatExecutor::NBackup
