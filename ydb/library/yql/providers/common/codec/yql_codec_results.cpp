#include "yql_codec_results.h"

#include <library/cpp/string_utils/base64/base64.h>

#include <ydb/library/yql/utils/utf8.h>

namespace NYql {
namespace NCommon {

constexpr TStringBuf TYsonResultWriter::VoidString;

void TYsonResultWriter::OnStringScalar(TStringBuf value) {
    if (!IsUtf8(value)) {
        TString encoded = Base64Encode(value);
        Writer.OnBeginList();
        Writer.OnListItem();
        Writer.OnStringScalar(TStringBuf(encoded));
        Writer.OnEndList();
    } else {
        Writer.OnStringScalar(value);
    }
}

}
}
