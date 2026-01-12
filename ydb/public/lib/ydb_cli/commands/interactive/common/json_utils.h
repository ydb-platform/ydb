#pragma once

#include <library/cpp/json/writer/json_value.h>

#include <util/generic/fwd.h>

namespace NYdb::NConsoleClient::NAi {

class TJsonParser {
public:
    TJsonParser();

    explicit TJsonParser(const NJson::TJsonValue& value);

    bool Parse(const TString& value);

    TString ToString() const;

    const NJson::TJsonValue& GetValue() const;

    TString GetFieldName() const;

    TJsonParser GetKey(const TString& key) const;

    std::optional<TJsonParser> MaybeKey(const TString& key) const;

    TJsonParser GetElement(ui64 index) const;

    void Iterate(std::function<void(TJsonParser)> handler) const;

    TString GetString() const;

    bool GetBooleanSafe(bool defaultValue) const;

    bool IsNull() const;

    void ValidateType(NJson::EJsonValueType expectedType) const;

private:
    TJsonParser(std::shared_ptr<NJson::TJsonValue> jsonHolder, const NJson::TJsonValue* state, const TString& fieldName);

    TString AdvancePath(const TString& key) const;

    TString AdvancePath(const ui64& index) const;

    void Fail(const TString& message) const;

private:
    std::shared_ptr<NJson::TJsonValue> JsonHolder;
    const NJson::TJsonValue* State = nullptr;
    std::optional<TString> FieldName;
};

class TJsonSchemaBuilder {
public:
    enum class EType {
        Undefined,
        String,
        Boolean,
        Object,
    };

    TJsonSchemaBuilder() = default;

    explicit TJsonSchemaBuilder(TJsonSchemaBuilder* parent);

    TJsonSchemaBuilder& Type(EType type);

    TJsonSchemaBuilder& Description(const TString& description);

    TJsonSchemaBuilder& Property(const TString& name, bool required = true);

    TJsonSchemaBuilder& Done();

    TJsonSchemaBuilder& Required(const TString& name);

    NJson::TJsonValue Build() const;

private:
    TJsonSchemaBuilder* Parent = nullptr;

    EType TypeValue = EType::Undefined;
    std::unordered_map<TString, std::shared_ptr<TJsonSchemaBuilder>> Properties;
    std::vector<TString> RequiredProperties;
    std::optional<TString> DescriptionValue;
};

TString FormatJsonValue(const NJson::TJsonValue& value);

TString FormatJsonValue(const TString& value);

} // namespace NYdb::NConsoleClient::NAi
