#include "json_envelope.h"

#include <util/charset/utf8.h>

namespace NKikimr {

const TStringBuf PLACEHOLDER = "%message%";

void TJsonEnvelope::Parse() {
    ReadJsonTree(TemplateString, &Value, true);
    Parse(&Value);
}

void TJsonEnvelope::Parse(NJson::TJsonValue* value) {
    if (Replace) {
        return;
    }

    switch (value->GetType()) {
    case NJson::JSON_STRING: {
            TReplace replace(value);
            if (replace.Parse(value->GetStringSafe())) {
                Replace = std::move(replace);
            }
            break;
        }
    case NJson::JSON_ARRAY: {
            for (NJson::TJsonValue& el : value->GetArraySafe()) {
                Parse(&el);
                if (Replace) {
                    break;
                }
            }
            break;
        }
    case NJson::JSON_MAP: {
            for (auto& [key, el] : value->GetMapSafe()) {
                Parse(&el);
                if (Replace) {
                    break;
                }
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

    if (Replace) {
        Replace->Apply(message);
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
        Prefix = replace.substr(0, pos);
    }
    if (pos + PLACEHOLDER.size() < replace.size()) {
        Suffix = replace.substr(pos + PLACEHOLDER.size());
    }
    return true;
}

void TJsonEnvelope::TReplace::Apply(const TStringBuf& message) {
    TString result;
    result.reserve(Prefix.size() + Suffix.size() + message.size());
    if (Prefix) {
        result += Prefix;
    }
    result += message;
    if (Suffix) {
        result += Suffix;
    }
    *Value = result;
}

} // namespace NKikimr
