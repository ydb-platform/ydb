#include "completion.h"

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
    switch (value) {
        case NLsp::EMarkupKind::PlainText:
            return TJsonValue("plaintext");
        case NLsp::EMarkupKind::Markdown:
            return TJsonValue("markdown");
    }
}

JSON_DEFINE_TO(NLsp::TMarkupContent, value) {
    TJsonValue json(JSON_MAP);
    SaveTo(json, "kind", value.Kind);
    SaveTo(json, "value", std::move(value.Value));
    return json;
}

JSON_DEFINE_TO(NLsp::EInsertTextFormat, value) {
    return TJsonValue(static_cast<int>(value) + 1);
}

JSON_DEFINE_TO(NLsp::TCompletionItem, value) {
    TJsonValue json(JSON_MAP);
    SaveTo(json, "label", std::move(value.Label));
    SaveTo(json, "kind", value.Kind);
    SaveTo(json, "detail", std::move(value.Detail));
    SaveTo(json, "documentation", std::move(value.Documentation));
    SaveTo(json, "filterText", std::move(value.FilterText));
    SaveTo(json, "insertText", std::move(value.InsertText));
    SaveTo(json, "insertTextFormat", value.InsertTextFormat);
    return json;
}

JSON_DEFINE_TO(NLsp::TCompletionList, value) {
    TJsonValue json(JSON_MAP);
    SaveTo(json, "isIncomplete", value.IsIncomplete);
    SaveTo(json, "items", std::move(value.Items));
    return json;
}

} // namespace NYql::NJson
