#pragma once
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>
#include <ydb/public/lib/json_value/ydb_json_value.h>
#include <util/charset/utf8.h>

namespace NMVP {

constexpr TStringBuf DECIMAL = "DECIMAL";
struct TProcessQueryParametersResult {
    THolder<NYdb::TParamsBuilder> Params = MakeHolder<NYdb::TParamsBuilder>();
    TString InjectData;
    TString ErrorMessage;
    bool HasError = false;
};

bool IsValidParameterName(const TString& param) {
    for (char c : param) {
        if (c != '$' && c != '_' && !std::isalnum(c)) {
            return false;
        }
    }
    return true;
}

TString UnifyPrimitiveTypeName(const TString& name) {
    TString output;
    std::copy_if(name.begin(), name.end(), std::back_inserter(output),
                [](char c) { return c != '_'; });
    return ToUpperUTF8(output);
}

TString GetSimpleTypeName(const Ydb::Type& type) {
    if (type.has_type_id()) {
        return Ydb::Type::PrimitiveTypeId_Name(type.type_id());
    } else if (type.has_decimal_type()) {
        return TStringBuilder() << DECIMAL << "(" << type.decimal_type().precision() << "," << type.decimal_type().scale() << ")";
    }
    return TString();
}

THolder<TProcessQueryParametersResult> ProcessQueryParameters(NJson::TJsonValue& queryParameters, bool injectDeclare) {
    auto result = MakeHolder<TProcessQueryParametersResult>();
    TStringStream data;
    if (queryParameters.GetType() == NJson::JSON_MAP) {
        const NJson::TJsonValue::TMapType& jsonParameters = queryParameters.GetMap();
        for (const auto& [name, value] : jsonParameters) {
            Ydb::Type type;
            const NJson::TJsonValue* jsonValue = &value;
            if (value.GetType() == NJson::JSON_MAP) {
                const NJson::TJsonValue* jsonType;
                if (value.GetValuePointer("type", &jsonType) && jsonType->GetType() == NJson::JSON_STRING) {
                    auto unifyName = UnifyPrimitiveTypeName(jsonType->GetString());
                    for (int i = 0; i < Ydb::Type::PrimitiveTypeId_descriptor()->value_count(); ++i) {
                        const google::protobuf::EnumValueDescriptor* enumValue = Ydb::Type::PrimitiveTypeId_descriptor()->value(i);
                        auto primitiveName = UnifyPrimitiveTypeName(enumValue->name());
                        if (primitiveName == unifyName) {
                            type.set_type_id(static_cast<Ydb::Type::PrimitiveTypeId>(enumValue->number()));
                            break;
                        }
                    }

                    if (!type.has_type_id() && unifyName.StartsWith(DECIMAL)) {
                        TStringBuf specificDecimal(unifyName);
                        specificDecimal.NextTok('(');
                        int precision = std::stoi(TString(specificDecimal.NextTok(',')));
                        int scale = std::stoi(TString(specificDecimal.NextTok(')')));
                        type.mutable_decimal_type()->set_precision(precision);
                        type.mutable_decimal_type()->set_scale(scale);
                    }
                }
                value.GetValuePointer("value", &jsonValue);
            }
            if (!type.has_decimal_type() && type.type_id() == Ydb::Type::PRIMITIVE_TYPE_ID_UNSPECIFIED) {
                switch (jsonValue->GetType()) {
                case NJson::JSON_BOOLEAN:
                    type.set_type_id(Ydb::Type::BOOL);
                    break;
                case NJson::JSON_DOUBLE:
                    type.set_type_id(Ydb::Type::DOUBLE);
                    break;
                case NJson::JSON_INTEGER:
                    type.set_type_id(Ydb::Type::INT64);
                    break;
                case NJson::JSON_NULL: {
                    result->ErrorMessage = "Can't accept nulls in parameters without a type";
                    result->HasError = true;
                    return result;
                }
                case NJson::JSON_STRING:
                    type.set_type_id(Ydb::Type::UTF8);
                    break;
                case NJson::JSON_UINTEGER:
                    type.set_type_id(Ydb::Type::UINT64);
                    break;
                default:
                    break;
                }
            } else if (jsonValue->GetType() == NJson::JSON_NULL) {
                Ydb::Type newType;
                newType.mutable_optional_type()->mutable_item()->CopyFrom(type);
                type = std::move(newType);
            }
            if (type.type_id() != Ydb::Type::PRIMITIVE_TYPE_ID_UNSPECIFIED || type.has_decimal_type() || type.has_optional_type()) {
                NYdb::TValue ydbValue(type, NYdb::TProtoAccessor::GetProto(NYdb::JsonToYdbValue(*jsonValue, type, NYdb::EBinaryStringEncoding::Unicode)));
                result->Params->AddParam(name, ydbValue);
                if (injectDeclare && IsValidParameterName(name)) {
                    TString typeName;
                    if (type.has_optional_type()) {
                        typeName = TStringBuilder() << "Optional<" << GetSimpleTypeName(type.optional_type().item()) << ">";
                    } else {
                        typeName = GetSimpleTypeName(type);
                    }
                    data << "DECLARE " << name << " AS " <<  typeName << ";\n";
                }
            }
        }
    }

    result->InjectData = data.Str();
    return result;
}
} // namespace NMVP
