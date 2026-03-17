#pragma once

#include <library/cpp/openssl/crypto/sha.h>

#include <util/generic/string.h>
#include <util/generic/strbuf.h>

namespace NKikimr::NTabletFlatExecutor::NBackup {

TString FormatChecksumDigest(const NOpenSsl::NSha256::TDigest& digest);
TString ComputeChecksum(TStringBuf data);

} // namespace NKikimr::NTabletFlatExecutor::NBackup
