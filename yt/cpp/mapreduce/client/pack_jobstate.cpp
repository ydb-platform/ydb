#include "pack_jobstate.h"

#include <library/cpp/string_utils/base64/base64.h>

#include <util/stream/str.h>
#include <util/stream/zlib.h>


namespace NYT::NDetail {

////////////////////////////////////////////////////////////////////////////////

TString PackJobState(TStringBuf jobstate)
{
    TStringStream packed;
    TZLibCompress packer(&packed);
    packer << jobstate;
    packer.Finish();
    return Base64Encode(packed.Str());
}

TString UnpackJobState(TStringBuf packedJobState)
{
    TStringStream packed;
    packed << Base64Decode(packedJobState);
    TBufferedZLibDecompress unpacker(&packed);
    return unpacker.ReadAll();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
