#include "profile.h"

#include <util/stream/str.h>
#include <util/stream/zlib.h>

namespace NYT::NYTProf {

////////////////////////////////////////////////////////////////////////////////

void WriteProfile(IOutputStream* out, const NProto::Profile& profile)
{
    TZLibCompress compress(out, ZLib::StreamType::GZip);
    profile.SerializeToArcadiaStream(&compress);
    compress.Finish();
}

TString SerializeProfile(const NProto::Profile& profile)
{
    TStringStream stream;
    WriteProfile(&stream, profile);
    return stream.Str();
}

void ReadProfile(IInputStream* in, NProto::Profile* profile)
{
    profile->Clear();

    TZLibDecompress decompress(in);
    profile->ParseFromArcadiaStream(&decompress);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf
