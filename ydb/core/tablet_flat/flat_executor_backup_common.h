#pragma once

#include <library/cpp/openssl/crypto/sha.h>

#include <util/generic/string.h>
#include <util/generic/strbuf.h>

namespace NKikimr::NTabletFlatExecutor::NBackup {

TString FormatChecksumDigest(const NOpenSsl::NSha256::TDigest& digest);

template <typename... Ts>
TString ComputeChecksum(const Ts&... parts) {
    NOpenSsl::NSha256::TCalcer calcer;
    (calcer.Update(TStringBuf(parts)), ...);
    return FormatChecksumDigest(calcer.Final());
}

TString ComputeInitialChecksum(ui32 generation, ui32 step);

} // namespace NKikimr::NTabletFlatExecutor::NBackup
