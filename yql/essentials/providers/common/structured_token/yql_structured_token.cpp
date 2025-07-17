#include "yql_structured_token.h"

#include <yql/essentials/utils/utf8.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/string_utils/base64/base64.h>


namespace NYql {

TStructuredToken::TStructuredToken(TMap<TString, TString>&& data)
    : Data_(std::move(data)) {

}

TString TStructuredToken::GetField(const TString& name) const {
    return Data_.at(name);
}

TString TStructuredToken::GetFieldOrDefault(const TString& name, const TString& defaultValue) const {
    return FindField(name).GetOrElse(defaultValue);
}

TMaybe<TString> TStructuredToken::FindField(const TString& name) const {
    auto* r = Data_.FindPtr(name);
    return r ? MakeMaybe(*r) : Nothing();
}

bool TStructuredToken::HasField(const TString& name) const {
    return Data_.contains(name);
}

TStructuredToken& TStructuredToken::SetField(const TString& name, const TString& value) {
    Data_[name] = value;
    return *this;
}

TStructuredToken& TStructuredToken::ClearField(const TString& name) {
    Data_.erase(name);
    return *this;
}

TString TStructuredToken::ToJson() const {
    TStringStream output;
    // set "format output" to false, no need for extra indents
    // "sort keys" value is not used actually
    // turn on UTF8 validation (need for keys checks)
    NJson::TJsonWriter writer(&output, false, true, true);
    writer.OpenMap();

    for (auto&[k, v] : Data_) {
        if (!IsUtf8(v)) {
            writer.Write(k + "(base64)", Base64Encode(TStringBuf(v)));
        } else {
            writer.Write(k, v);
        }
    }
    writer.CloseMap();
    writer.Flush();

    return output.Str();
}

TStructuredToken ParseStructuredToken(const TString& content) {
    Y_ABORT_UNLESS(IsStructuredTokenJson(content));

    NJson::TJsonValue v;
    // will throw on error
    NJson::ReadJsonTree(content, &v, true);
    TMap<TString, TString> data;
    const auto& m = v.GetMapSafe();
    for (auto&[k, v] : m) {
        TStringBuf key(k);
        if (key.ChopSuffix("(base64)")) {
            const auto& s = v.GetStringSafe();
            data[TString(key)] = Base64Decode(TStringBuf(s));
        } else {
            data[k] = v.GetStringSafe();
        }
    }
    return TStructuredToken(std::move(data));
}

bool IsStructuredTokenJson(const TString& content) {
    return content.StartsWith("{") && content.EndsWith("}");
}

} // namespace NYql
