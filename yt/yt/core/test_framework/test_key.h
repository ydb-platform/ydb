#pragma once

#include <yt/yt/core/crypto/public.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TString GetTestKeyContent(TStringBuf name);
NCrypto::TPemBlobConfigPtr CreateTestKeyBlob(TStringBuf name);
NCrypto::TPemBlobConfigPtr CreateTestKeyFile(TStringBuf name);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
