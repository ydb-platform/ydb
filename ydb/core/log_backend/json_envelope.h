#pragma once
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>

#include <util/generic/string.h>

#include <vector>

namespace NKikimr {

class TJsonEnvelope {
public:
    explicit TJsonEnvelope(const TString& templateString)
        : TemplateString(templateString)
    {
        Parse(); // can throw
    }

    TJsonEnvelope() = delete;
    TJsonEnvelope(const TJsonEnvelope&) = delete;
    TJsonEnvelope(TJsonEnvelope&&) = delete;

    TString ApplyJsonEnvelope(const TStringBuf& message);

private:
    void Parse();
    void Parse(NJson::TJsonValue* value);

private:
    struct TReplace {
        NJson::TJsonValue* Value = nullptr;
        std::vector<TString> ReplaceSequence; // empty string for placeholder

        TReplace(NJson::TJsonValue* value)
            : Value(value)
        {}

        bool Parse(const TString& replace);
        void Apply(const TStringBuf& message);
    };

private:
    TString TemplateString;
    NJson::TJsonValue Value;
    std::vector<TReplace> Replaces;
};

} // namespace NKikimr
