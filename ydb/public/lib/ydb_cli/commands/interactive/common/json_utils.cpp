#include "json_utils.h"

#include <ydb/library/yverify_stream/yverify_stream.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/writer/json.h>

#include <util/string/builder.h>

namespace NYdb::NConsoleClient::NAi {

//// TJsonParser

TJsonParser::TJsonParser()
    : TJsonParser(NJson::TJsonValue())
{}

TJsonParser::TJsonParser(const NJson::TJsonValue& value)
    : JsonHolder(std::make_shared<NJson::TJsonValue>(value))
    , State(JsonHolder.get())
{}

TJsonParser::TJsonParser(std::shared_ptr<NJson::TJsonValue> jsonHolder, const NJson::TJsonValue* state, const TString& fieldName)
    : JsonHolder(jsonHolder)
    , State(state)
    , FieldName(fieldName)
{
    Y_VALIDATE(JsonHolder, "JsonHolder should not be null");
    Y_VALIDATE(State, "State should not be null");
}

bool TJsonParser::Parse(const TString& value) {
    Y_VALIDATE(JsonHolder.get() == State, "Parse should be called on root JSON");
    return NJson::ReadJsonTree(value, JsonHolder.get());
}

TString TJsonParser::ToString() const {
    if (State->IsString()) {
        return State->GetString();
    }

    NJsonWriter::TBuf valueWriter;
    valueWriter.SetIndentSpaces(2);
    valueWriter.WriteJsonValue(State);
    return valueWriter.Str();
}

const NJson::TJsonValue& TJsonParser::GetValue() const {
    return *State;
}

TString TJsonParser::GetFieldName() const {
    return FieldName.value_or("$");
}

TJsonParser TJsonParser::GetKey(const TString& key) const {
    ValidateType(NJson::JSON_MAP);

    const auto* child = State->GetMapSafe().FindPtr(key);
    if (!child) {
        Fail(TStringBuilder() << "does not contain " << key << " field");
    }

    return TJsonParser(JsonHolder, child, AdvancePath(key));
}

std::optional<TJsonParser> TJsonParser::MaybeKey(const TString& key) const {
    if (!State->IsMap()) {
        return std::nullopt;
    }

    if (const auto* child = State->GetMapSafe().FindPtr(key)) {
        return TJsonParser(JsonHolder, child, AdvancePath(key));
    }

    return std::nullopt;
}

TJsonParser TJsonParser::GetElement(ui64 index) const {
    ValidateType(NJson::JSON_ARRAY);

    const auto& array = State->GetArraySafe();
    if (const auto size = array.size(); size <= index) {
        Fail(TStringBuilder() << "does not contain element with index " << index << ", actual array size: " << size);
    }

    return TJsonParser(JsonHolder, &array[index], AdvancePath(index));
}

void TJsonParser::Iterate(std::function<void(TJsonParser)> handler) const {
    ValidateType(NJson::JSON_ARRAY);

    const auto& array = State->GetArraySafe();
    for (ui64 i = 0; i < array.size(); ++i) {
        handler(TJsonParser(JsonHolder, &array[i], AdvancePath(i)));
    }
}

TString TJsonParser::GetString() const {
    ValidateType(NJson::JSON_STRING);
    return State->GetString();
}

bool TJsonParser::GetBooleanSafe(bool defaultValue) const {
    if (State->IsBoolean()) {
        return State->GetBoolean();
    }
    return defaultValue;
}

bool TJsonParser::IsNull() const {
    return State->IsNull();
}

void TJsonParser::ValidateType(NJson::EJsonValueType expectedType) const {
    if (const auto type = State->GetType(); type != expectedType) {
        Fail(TStringBuilder() << "has unexpected type " << type << ", expected type: " << expectedType);
    }
}

TString TJsonParser::AdvancePath(const TString& key) const {
    return TStringBuilder() << GetFieldName() << "." << key;
}

TString TJsonParser::AdvancePath(const ui64& index) const {
    return TStringBuilder() << GetFieldName() << "[" << index << "]";
}

void TJsonParser::Fail(const TString& message) const {
    auto error = yexception() << "JSON";
    if (FieldName) {
        error << " field " << *FieldName;
    }
    throw error << " " << message;
}

//// TJsonSchemaBuilder

TJsonSchemaBuilder::TJsonSchemaBuilder(TJsonSchemaBuilder* parent)
    : Parent(parent)
{
    Y_VALIDATE(Parent, "Parent should not be null");
}

TJsonSchemaBuilder& TJsonSchemaBuilder::Type(EType type) {
    Y_VALIDATE(TypeValue == EType::Undefined, "Type should be defined only once");
    Y_VALIDATE(type != EType::Undefined, "Type should not be undefined");
    TypeValue = type;
    return *this;
}

TJsonSchemaBuilder& TJsonSchemaBuilder::Description(const TString& description) {
    Y_VALIDATE(!DescriptionValue, "Description should be defined only once");
    DescriptionValue = description;
    return *this;
}

TJsonSchemaBuilder& TJsonSchemaBuilder::Done() {
    Y_VALIDATE(Parent, "Done should be called only on child objects");
    return *Parent;
}

TJsonSchemaBuilder& TJsonSchemaBuilder::Required(const TString& name) {
    RequiredProperties.emplace_back(name);
    return *this;
}

NJson::TJsonValue TJsonSchemaBuilder::Build() const {
    NJson::TJsonValue result;

    if (DescriptionValue) {
        result["description"] = *DescriptionValue;
    }

    auto& type = result["type"];
    switch (TypeValue) {
        case EType::Undefined: {
            Y_VALIDATE(false, "Type should not be defined before building");
            break;
        }
        case EType::String: {
            type = "string";
            break;
        }
        case EType::Boolean: {
            type = "boolean";
            break;
        }
        case EType::Object: {
            type = "object";

            if (!Properties.empty()) {
                auto& properties = result["properties"].SetType(NJson::JSON_MAP).GetMapSafe();
                properties.reserve(Properties.size());
                for (const auto& [name, builder] : Properties) {
                    properties.emplace(name, builder->Build());
                }
            }

            if (!RequiredProperties.empty()) {
                auto& required = result["required"].SetType(NJson::JSON_ARRAY).GetArraySafe();
                for (const auto& name : RequiredProperties) {
                    required.emplace_back(name);
                }
            }

            break;
        }
    }

    return result;
}

TJsonSchemaBuilder& TJsonSchemaBuilder::Property(const TString& name, bool required) {
    const auto [it, inserted] = Properties.emplace(name, std::make_shared<TJsonSchemaBuilder>(this));
    Y_VALIDATE(inserted, "Property should not be defined twice");

    if (required) {
        RequiredProperties.emplace_back(name);
    }

    return *it->second;
}

//// Utils

TString FormatJsonValue(const NJson::TJsonValue& value) {
    return TJsonParser(value).ToString();
}

TString FormatJsonValue(const TString& value) {
    TJsonParser parser;
    if (!parser.Parse(value)) {
        return value;
    }
    return parser.ToString();
}

} // namespace NYdb::NConsoleClient::NAi
