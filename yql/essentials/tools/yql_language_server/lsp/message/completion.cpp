#include "completion.h"

#include <yql/essentials/utils/json/reflection.h>

namespace NYql::NJson {

JSON_DEFINE_FROM(NLsp::TCompletionParams, json) {
    NLsp::TCompletionParams x;
    JSON_MOVE_FROM(json, "textDocument", x.TextDocument);
    JSON_MOVE_FROM(json, "position", x.Position);
    return x;
}

JSON_DEFINE_TO(NLsp::ECompletionItemKind, value) {
    return TJsonValue(static_cast<int>(value) + 1);
}

JSON_DEFINE_TO(NLsp::EMarkupKind, value) {
    return ToString(value);
}

YQL_DERIVE_JSON_TO(NLsp::TMarkupContent);

JSON_DEFINE_TO(NLsp::EInsertTextFormat, value) {
    return TJsonValue(static_cast<int>(value) + 1);
}

YQL_DERIVE_JSON_TO(NLsp::TCompletionItem);

YQL_DERIVE_JSON_TO(NLsp::TCompletionList);

} // namespace NYql::NJson
