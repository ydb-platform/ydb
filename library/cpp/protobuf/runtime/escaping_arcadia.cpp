#include "escaping.h"

#include <google/protobuf/stubs/strutil.h>

namespace NProtoBufRuntime {
    TString CEscape(TStringBuf src) {
        return TString{google::protobuf::CEscape(TProtoStringType{src})};
    }

    TString Utf8SafeCEscape(TStringBuf src) {
        return TString{google::protobuf::strings::Utf8SafeCEscape(TProtoStringType{src})};
    }

    TString UnescapeCEscapeString(TStringBuf src) {
        return TString{google::protobuf::UnescapeCEscapeString(TProtoStringType{src})};
    }
} // namespace NProtoBufRuntime
