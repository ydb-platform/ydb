#include "escaping.h"

#include <google/protobuf/stubs/strutil.h>

// TProtoStringType is an Arcadia-only patch on protobuf's stubs/port.h. The
// yt-cpp-sdk CMake export builds this file against vanilla Conan protobuf,
// where the alias does not exist, so spell the string type as TString: it is
// TProtoStringType in Arcadia and std::string under TSTRING_IS_STD_STRING.

namespace NProtoBufRuntime {
    TString CEscape(TStringBuf src) {
        return TString{google::protobuf::CEscape(TString{src.data(), src.size()})};
    }

    TString Utf8SafeCEscape(TStringBuf src) {
        return TString{google::protobuf::strings::Utf8SafeCEscape(TString{src.data(), src.size()})};
    }

    TString UnescapeCEscapeString(TStringBuf src) {
        return TString{google::protobuf::UnescapeCEscapeString(TString{src.data(), src.size()})};
    }
} // namespace NProtoBufRuntime
