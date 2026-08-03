#include "formatting.h"

namespace NYql::NJson {

JSON_DEFINE_FROM(NLsp::TFormattingOptions, json) {
    NLsp::TFormattingOptions x;
    JSON_MOVE_FROM(json, "tabSize", x.TabSize);
    JSON_MOVE_FROM(json, "insertSpaces", x.InsertSpaces);
    return x;
}

JSON_DEFINE_FROM(NLsp::TDocumentFormattingParams, json) {
    NLsp::TDocumentFormattingParams x;
    JSON_MOVE_FROM(json, "textDocument", x.TextDocument);
    JSON_MOVE_FROM(json, "options", x.Options);
    return x;
}

} // namespace NYql::NJson
