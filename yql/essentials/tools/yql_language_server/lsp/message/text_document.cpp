#include "text_document.h"

namespace NYql::NJson {

JSON_DEFINE_FROM(NLsp::TPosition, json) {
    NLsp::TPosition x;
    JSON_MOVE_FROM(json, "line", x.Line);
    JSON_MOVE_FROM(json, "character", x.Character);
    return x;
}

JSON_DEFINE_TO(NLsp::TPosition, value) {
    TJsonValue json(JSON_MAP);
    SaveTo(json, "line", value.Line);
    SaveTo(json, "character", value.Character);
    return json;
}

JSON_DEFINE_FROM(NLsp::TRange, json) {
    NLsp::TRange x;
    JSON_MOVE_FROM(json, "start", x.Start);
    JSON_MOVE_FROM(json, "end", x.End);
    return x;
}

JSON_DEFINE_FROM(NLsp::TTextDocumentIdentifier, json) {
    NLsp::TTextDocumentIdentifier x;
    JSON_MOVE_FROM(json, "uri", x.Uri);
    return x;
}

JSON_DEFINE_FROM(NLsp::TTextDocumentItem, json) {
    NLsp::TTextDocumentItem x;
    JSON_MOVE_FROM(json, "uri", x.Uri);
    JSON_MOVE_FROM(json, "languageId", x.LanguageId);
    JSON_MOVE_FROM(json, "version", x.Version);
    JSON_MOVE_FROM(json, "text", x.Text);
    return x;
}

JSON_DEFINE_FROM(NLsp::TTextDocumentPositionParams, json) {
    NLsp::TTextDocumentPositionParams x;
    JSON_MOVE_FROM(json, "textDocument", x.TextDocument);
    JSON_MOVE_FROM(json, "position", x.Position);
    return x;
}

} // namespace NYql::NJson
