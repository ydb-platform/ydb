#include "json_envelope.h"

#include <util/charset/utf8.h>

namespace NKikimr {

const TStringBuf PLACEHOLDER = "%message%";

void TJsonEnvelope::Parse() {
    ReadJsonTree(TemplateString, &Value, true);
    Parse(&Value);
}

void TJsonEnvelope::Parse(NJson::TJsonValue* value) {
    switch (value->GetType()) {
    case NJson::JSON_STRING: {
            TReplace replace(value);
            if (replace.Parse(value->GetStringSafe())) {
                Replaces.emplace_back(std::move(replace));
            }
            break;
        }
    case NJson::JSON_ARRAY: {
            for (NJson::TJsonValue& el : value->GetArraySafe()) {
                Parse(&el);
            }
            break;
        }
    case NJson::JSON_MAP: {
            for (auto& [key, el] : value->GetMapSafe()) {
                Parse(&el);
            }
            break;
        }
    default:
        break;
    }
}

TString TJsonEnvelope::ApplyJsonEnvelope(const TStringBuf& message) {
    if (!IsUtf(message)) {
        throw std::runtime_error("Attempt to write non utf-8 string");
    }

    for (TReplace& replace : Replaces) {
        replace.Apply(message);
    }

    TStringStream ss;
    NJson::WriteJson(&ss, &Value, NJson::TJsonWriterConfig().SetValidateUtf8(true));
    ss << Endl;
    return ss.Str();
}

bool TJsonEnvelope::TReplace::Parse(const TString& replace) {
    size_t pos = replace.find(PLACEHOLDER);
    if (pos == TString::npos) {
        return false;
    }

    if (pos != 0) {
        ReplaceSequence.emplace_back(replace.substr(0, pos));
    }
    while (pos != TString::npos) {
        ReplaceSequence.emplace_back(); // placeholder
        if (pos + PLACEHOLDER.size() == replace.size()) {
            break;
        }
        size_t next = replace.find(PLACEHOLDER, pos + PLACEHOLDER.size());
        ReplaceSequence.emplace_back(replace.substr(pos + PLACEHOLDER.size(), next == TString::npos ? next : next - pos - PLACEHOLDER.size()));
        pos = next;
    }
    return true;
}

void TJsonEnvelope::TReplace::Apply(const TStringBuf& message) {
    TString result;
    for (const TString& replace : ReplaceSequence) {
        if (replace.empty()) {
            result += message;
        } else {
            result += replace;
        }
    }
    *Value = result;
}

} // namespace NKikimr
