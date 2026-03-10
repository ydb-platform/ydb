#include "yql_codec_results.h"

#include <library/cpp/string_utils/base64/base64.h>

#include <yql/essentials/utils/utf8.h>

namespace NYql::NResult {

constexpr TStringBuf TYsonResultWriter::VoidString;

void TYsonResultWriter::OnStringScalar(TStringBuf value) {
    if (!IsUtf8(value)) {
        TString encoded = Base64Encode(value);
        Writer_.OnBeginList();
        Writer_.OnListItem();
        Writer_.OnStringScalar(TStringBuf(encoded));
        Writer_.OnEndList();
    } else {
        Writer_.OnStringScalar(value);
    }
}

} // namespace NYql::NResult
