#pragma once

#include <library/cpp/codecs/codecs.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/stream/output.h>

namespace NCodecs {
    class TStaticCodecInfo;

    // load

    TCodecConstPtr RestoreCodecFromCodecInfo(const TStaticCodecInfo&);

    TStaticCodecInfo LoadCodecInfoFromString(TStringBuf data);

    TString LoadStringFromArchive(const ui8* begin, size_t size);

    TCodecConstPtr RestoreCodecFromArchive(const ui8* begin, size_t size);

    // save

    TString SaveCodecInfoToString(const TStaticCodecInfo&);

    void SaveCodecInfoToStream(IOutputStream& out, const TStaticCodecInfo&);

    // misc

    TStaticCodecInfo LoadCodecInfoFromStream(IInputStream& in);

    TString FormatCodecInfo(const TStaticCodecInfo&);

}
