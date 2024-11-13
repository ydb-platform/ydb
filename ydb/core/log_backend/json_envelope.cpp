#include "json_envelope.h"

#include <util/charset/utf8.h>

namespace NKikimr {

const TStringBuf PLACEHOLDER = "%message%";

void TJsonEnvelope::Parse() {
    ReadJsonTree(TemplateString, &Value, true);
    std::vector<TReplace::TPathComponent> path;
    Parse(Value, path);
}

bool TJsonEnvelope::Parse(const NJson::TJsonValue& value, std::vector<TReplace::TPathComponent>& path) {
    Y_ASSERT(!Replace);

    switch (value.GetType()) {
    case NJson::JSON_STRING: {
        if (auto replacePair = Parse(value.GetStringSafe())) {
            Replace.emplace(std::move(path), std::move(replacePair->first), std::move(replacePair->second));
            return true;
        }
        return false;
    }
    case NJson::JSON_ARRAY: {
        const auto& arr = value.GetArraySafe();
        path.emplace_back(size_t(0));
        for (size_t i = 0; i < arr.size(); ++i) {
            path.back() = i;
            if (Parse(arr[i], path)) {
                return true;
            }
        }
        path.pop_back();
        return false;
    }
    case NJson::JSON_MAP: {
        const auto& map = value.GetMapSafe();
        path.emplace_back(TString());
        for (const auto& [key, el] : map) {
            path.back() = key;
            if (Parse(el, path)) {
                return true;
            }
        }
        path.pop_back();
        return false;
    }
    default:
        return false;
    }
}

std::optional<std::pair<TString, TString>> TJsonEnvelope::Parse(const TString& stringValue) {
    std::optional<std::pair<TString, TString>> result;
    size_t pos = stringValue.find(PLACEHOLDER);
    if (pos == TString::npos) {
        return result;
    }

    TString prefix, suffix;
    if (pos != 0) {
        prefix = stringValue.substr(0, pos);
    }
    if (pos + PLACEHOLDER.size() < stringValue.size()) {
        suffix = stringValue.substr(pos + PLACEHOLDER.size());
    }
    result.emplace(std::move(prefix), std::move(suffix));
    return result;
}

TString TJsonEnvelope::ApplyJsonEnvelope(const TStringBuf& message) const {
    if (!IsUtf(message)) {
        throw std::runtime_error("Attempt to write non utf-8 string");
    }

    const NJson::TJsonValue* value = &Value;
    NJson::TJsonValue valueCopy;
    if (Replace) {
        valueCopy = Value;
        value = &valueCopy;
        Replace->Apply(&valueCopy, message);
    }

    TStringStream ss;
    NJson::WriteJson(&ss, value, NJson::TJsonWriterConfig().SetValidateUtf8(true));
    ss << Endl;
    return ss.Str();
}

void TJsonEnvelope::TReplace::Apply(NJson::TJsonValue* value, const TStringBuf& message) const {
    size_t currentEl = 0;
    while (currentEl < Path.size()) {
        std::visit(
            [&](const auto& child) {
                value = &(*value)[child];
            },
            Path[currentEl++]
        );
    }

    Y_ASSERT(value && value->GetType() == NJson::JSON_STRING);
    TString result;
    result.reserve(Prefix.size() + Suffix.size() + message.size());
    if (Prefix) {
        result += Prefix;
    }
    result += message;
    if (Suffix) {
        result += Suffix;
    }
    *value = result;
}

} // namespace NKikimr
