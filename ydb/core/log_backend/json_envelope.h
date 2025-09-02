#pragma once
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>

#include <util/generic/string.h>

#include <optional>
#include <variant>
#include <vector>

namespace NKikimr {

class TJsonEnvelope {
    struct TReplace {
        using TPathComponent = std::variant<size_t, TString>; // field or index
        std::vector<TPathComponent> Path;
        // replace
        TString Prefix;
        TString Suffix;

        TReplace(std::vector<TPathComponent> path, TString prefix, TString suffix)
            : Path(std::move(path))
            , Prefix(std::move(prefix))
            , Suffix(std::move(suffix))
        {}

        void Apply(NJson::TJsonValue* value, const TStringBuf& message) const;
    };

public:
    explicit TJsonEnvelope(const TString& templateString)
        : TemplateString(templateString)
    {
        Parse(); // can throw
    }

    TJsonEnvelope() = delete;
    TJsonEnvelope(const TJsonEnvelope&) = delete;
    TJsonEnvelope(TJsonEnvelope&&) = delete;

    TString ApplyJsonEnvelope(const TStringBuf& message) const;

private:
    void Parse();
    bool Parse(const NJson::TJsonValue& value, std::vector<TReplace::TPathComponent>& path);
    std::optional<std::pair<TString, TString>> Parse(const TString& stringValue); // returns prefix/suffix pair for replace

private:
    TString TemplateString;
    NJson::TJsonValue Value;
    std::optional<TReplace> Replace;
};

} // namespace NKikimr
