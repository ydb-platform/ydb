#pragma once

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/value/value.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

#include <array>
#include <chrono>

namespace NFuzzing::NValueFuzz {

struct TValueSchema {
    enum class EKind {
        Primitive,
        Optional,
        List,
        Struct,
        Tuple,
        Dict,
    };

    EKind Kind = EKind::Primitive;
    NYdb::EPrimitiveType Primitive = NYdb::EPrimitiveType::Bool;
    TVector<TValueSchema> Children;
    TVector<TString> Names;
};

struct TGeneratedColumn {
    TString Name;
    NYdb::TType Type;
    NYdb::TValue Value;
};

inline TString ConsumeBytes(FuzzedDataProvider& fdp, size_t maxLen = 64) {
    return fdp.ConsumeBytesAsString(fdp.ConsumeIntegralInRange<size_t>(0, maxLen));
}

inline TString ConsumeAscii(FuzzedDataProvider& fdp, size_t maxLen = 32) {
    TString value = ConsumeBytes(fdp, maxLen);
    for (char& ch : value) {
        ch = static_cast<char>(' ' + (static_cast<unsigned char>(ch) % 95));
    }
    return value;
}

inline TString MakeIdentifier(FuzzedDataProvider& fdp, TStringBuf prefix, size_t index) {
    TString value = ConsumeAscii(fdp, 16);
    if (value.empty()) {
        value = "field";
    }
    for (char& ch : value) {
        if (!(('a' <= ch && ch <= 'z') || ('A' <= ch && ch <= 'Z') || ('0' <= ch && ch <= '9'))) {
            ch = '_';
        }
    }
    return TStringBuilder() << prefix << index << "_" << value;
}

inline TString EscapeJsonString(TStringBuf value) {
    TString escaped;
    escaped.reserve(value.size() * 2);
    static constexpr std::array<char, 16> HexDigits = {
        '0', '1', '2', '3', '4', '5', '6', '7',
        '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
    };

    for (unsigned char ch : value) {
        switch (ch) {
            case '"':
                escaped.append("\\\"");
                break;
            case '\\':
                escaped.append("\\\\");
                break;
            case '\b':
                escaped.append("\\b");
                break;
            case '\f':
                escaped.append("\\f");
                break;
            case '\n':
                escaped.append("\\n");
                break;
            case '\r':
                escaped.append("\\r");
                break;
            case '\t':
                escaped.append("\\t");
                break;
            default:
                if (ch < 0x20) {
                    escaped.append(TStringBuilder() << "\\u00"
                        << HexDigits[ch >> 4]
                        << HexDigits[ch & 0x0f]);
                } else {
                    escaped.push_back(static_cast<char>(ch));
                }
                break;
        }
    }

    return escaped;
}

inline TString MakeJsonPayload(FuzzedDataProvider& fdp) {
    switch (fdp.ConsumeIntegralInRange<int>(0, 5)) {
        case 0:
            return "null";
        case 1:
            return "{}";
        case 2:
            return "[]";
        case 3:
            return "{\"key\":1}";
        case 4:
            return "[true,false,null]";
        default:
            return TStringBuilder() << "\"" << EscapeJsonString(ConsumeAscii(fdp, 24)) << "\"";
    }
}

inline TString MakeDyNumber(FuzzedDataProvider& fdp) {
    const bool negative = fdp.ConsumeBool();
    const ui64 integerPart = fdp.ConsumeIntegralInRange<ui64>(0, 1'000'000);
    const ui64 fractionalPart = fdp.ConsumeIntegralInRange<ui64>(0, 999'999);
    TString fractional = ToString(fractionalPart);
    while (fractional.size() < 6) {
        fractional = TString("0") + fractional;
    }
    return TStringBuilder()
        << (negative ? "-" : "")
        << integerPart
        << "."
        << fractional;
}

inline TString MakeUuidString(FuzzedDataProvider& fdp) {
    auto nextHex = [&fdp]() -> char {
        static constexpr std::array<char, 16> Digits = {
            '0', '1', '2', '3', '4', '5', '6', '7',
            '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
        };
        return Digits[fdp.ConsumeIntegralInRange<size_t>(0, Digits.size() - 1)];
    };

    TString uuid;
    uuid.reserve(36);
    for (size_t i = 0; i < 36; ++i) {
        if (i == 8 || i == 13 || i == 18 || i == 23) {
            uuid.push_back('-');
        } else {
            uuid.push_back(nextHex());
        }
    }
    return uuid;
}

inline std::array<NYdb::EPrimitiveType, 18> PrimitiveKinds() {
    return {
        NYdb::EPrimitiveType::Bool,
        NYdb::EPrimitiveType::Int64,
        NYdb::EPrimitiveType::Uint64,
        NYdb::EPrimitiveType::Double,
        NYdb::EPrimitiveType::String,
        NYdb::EPrimitiveType::Utf8,
        NYdb::EPrimitiveType::Yson,
        NYdb::EPrimitiveType::Json,
        NYdb::EPrimitiveType::JsonDocument,
        NYdb::EPrimitiveType::DyNumber,
        NYdb::EPrimitiveType::Uuid,
        NYdb::EPrimitiveType::Date,
        NYdb::EPrimitiveType::Datetime,
        NYdb::EPrimitiveType::Timestamp,
        NYdb::EPrimitiveType::Interval,
        NYdb::EPrimitiveType::Date32,
        NYdb::EPrimitiveType::Datetime64,
        NYdb::EPrimitiveType::Timestamp64,
    };
}

inline std::array<NYdb::EPrimitiveType, 5> DictKeyPrimitiveKinds() {
    return {
        NYdb::EPrimitiveType::Bool,
        NYdb::EPrimitiveType::Int64,
        NYdb::EPrimitiveType::Uint64,
        NYdb::EPrimitiveType::String,
        NYdb::EPrimitiveType::Utf8,
    };
}

inline TValueSchema GenerateSchema(FuzzedDataProvider& fdp, int depth, bool keyContext = false) {
    TValueSchema schema;

    if (depth <= 0 || keyContext) {
        schema.Kind = TValueSchema::EKind::Primitive;
        if (keyContext) {
            const auto primitiveKinds = DictKeyPrimitiveKinds();
            schema.Primitive = primitiveKinds[fdp.ConsumeIntegralInRange<size_t>(0, primitiveKinds.size() - 1)];
        } else {
            const auto primitiveKinds = PrimitiveKinds();
            schema.Primitive = primitiveKinds[fdp.ConsumeIntegralInRange<size_t>(0, primitiveKinds.size() - 1)];
        }
        return schema;
    }

    switch (fdp.ConsumeIntegralInRange<int>(0, 5)) {
        case 0:
            schema.Kind = TValueSchema::EKind::Primitive;
            schema.Primitive = PrimitiveKinds()[fdp.ConsumeIntegralInRange<size_t>(0, PrimitiveKinds().size() - 1)];
            return schema;
        case 1:
            schema.Kind = TValueSchema::EKind::Optional;
            schema.Children.push_back(GenerateSchema(fdp, depth - 1));
            return schema;
        case 2:
            schema.Kind = TValueSchema::EKind::List;
            schema.Children.push_back(GenerateSchema(fdp, depth - 1));
            return schema;
        case 3: {
            schema.Kind = TValueSchema::EKind::Struct;
            const size_t memberCount = fdp.ConsumeIntegralInRange<size_t>(1, 3);
            schema.Children.reserve(memberCount);
            schema.Names.reserve(memberCount);
            for (size_t i = 0; i < memberCount; ++i) {
                schema.Names.push_back(MakeIdentifier(fdp, "member", i));
                schema.Children.push_back(GenerateSchema(fdp, depth - 1));
            }
            return schema;
        }
        case 4: {
            schema.Kind = TValueSchema::EKind::Tuple;
            const size_t elementCount = fdp.ConsumeIntegralInRange<size_t>(1, 3);
            schema.Children.reserve(elementCount);
            for (size_t i = 0; i < elementCount; ++i) {
                schema.Children.push_back(GenerateSchema(fdp, depth - 1));
            }
            return schema;
        }
        default:
            schema.Kind = TValueSchema::EKind::Dict;
            schema.Children.push_back(GenerateSchema(fdp, 0, true));
            schema.Children.push_back(GenerateSchema(fdp, depth - 1));
            return schema;
    }
}

inline void AppendType(NYdb::TTypeBuilder& builder, const TValueSchema& schema) {
    switch (schema.Kind) {
        case TValueSchema::EKind::Primitive:
            builder.Primitive(schema.Primitive);
            return;
        case TValueSchema::EKind::Optional:
            builder.BeginOptional();
            AppendType(builder, schema.Children.front());
            builder.EndOptional();
            return;
        case TValueSchema::EKind::List:
            builder.BeginList();
            AppendType(builder, schema.Children.front());
            builder.EndList();
            return;
        case TValueSchema::EKind::Struct:
            builder.BeginStruct();
            for (size_t i = 0; i < schema.Children.size(); ++i) {
                builder.AddMember(schema.Names[i]);
                AppendType(builder, schema.Children[i]);
            }
            builder.EndStruct();
            return;
        case TValueSchema::EKind::Tuple:
            builder.BeginTuple();
            for (const auto& child : schema.Children) {
                builder.AddElement();
                AppendType(builder, child);
            }
            builder.EndTuple();
            return;
        case TValueSchema::EKind::Dict:
            builder.BeginDict();
            builder.DictKey();
            AppendType(builder, schema.Children[0]);
            builder.DictPayload();
            AppendType(builder, schema.Children[1]);
            builder.EndDict();
            return;
    }
}

inline NYdb::TType BuildType(const TValueSchema& schema) {
    NYdb::TTypeBuilder builder;
    AppendType(builder, schema);
    return builder.Build();
}

inline void AppendPrimitiveValue(
    NYdb::TValueBuilder& builder,
    NYdb::EPrimitiveType primitive,
    FuzzedDataProvider& fdp)
{
    switch (primitive) {
        case NYdb::EPrimitiveType::Bool:
            builder.Bool(fdp.ConsumeBool());
            return;
        case NYdb::EPrimitiveType::Int64:
            builder.Int64(fdp.ConsumeIntegral<i64>());
            return;
        case NYdb::EPrimitiveType::Uint64:
            builder.Uint64(fdp.ConsumeIntegral<ui64>());
            return;
        case NYdb::EPrimitiveType::Double: {
            const i64 numerator = fdp.ConsumeIntegralInRange<i64>(-1'000'000, 1'000'000);
            const ui64 denominator = fdp.ConsumeIntegralInRange<ui64>(1, 10'000);
            builder.Double(static_cast<double>(numerator) / static_cast<double>(denominator));
            return;
        }
        case NYdb::EPrimitiveType::String:
            builder.String(ConsumeBytes(fdp, 48));
            return;
        case NYdb::EPrimitiveType::Utf8:
            builder.Utf8(ConsumeAscii(fdp, 48));
            return;
        case NYdb::EPrimitiveType::Yson:
            builder.Yson(ConsumeAscii(fdp, 48));
            return;
        case NYdb::EPrimitiveType::Json:
            builder.Json(MakeJsonPayload(fdp));
            return;
        case NYdb::EPrimitiveType::JsonDocument:
            builder.JsonDocument(MakeJsonPayload(fdp));
            return;
        case NYdb::EPrimitiveType::DyNumber:
            builder.DyNumber(MakeDyNumber(fdp));
            return;
        case NYdb::EPrimitiveType::Uuid:
            builder.Uuid(NYdb::TUuidValue(MakeUuidString(fdp)));
            return;
        case NYdb::EPrimitiveType::Date:
            builder.Date(TInstant::Days(fdp.ConsumeIntegralInRange<ui32>(0, 100'000)));
            return;
        case NYdb::EPrimitiveType::Datetime:
            builder.Datetime(TInstant::Seconds(fdp.ConsumeIntegralInRange<ui32>(0, 4'000'000'000u)));
            return;
        case NYdb::EPrimitiveType::Timestamp:
            builder.Timestamp(TInstant::MicroSeconds(fdp.ConsumeIntegralInRange<ui64>(0, 4'000'000'000'000ull)));
            return;
        case NYdb::EPrimitiveType::Interval:
            builder.Interval(fdp.ConsumeIntegralInRange<i64>(-1'000'000'000, 1'000'000'000));
            return;
        case NYdb::EPrimitiveType::Date32:
            builder.Date32(std::chrono::sys_time<NYdb::TWideDays>(
                NYdb::TWideDays(fdp.ConsumeIntegralInRange<i32>(-500'000, 500'000))));
            return;
        case NYdb::EPrimitiveType::Datetime64:
            builder.Datetime64(std::chrono::sys_time<NYdb::TWideSeconds>(
                NYdb::TWideSeconds(fdp.ConsumeIntegralInRange<i64>(-4'000'000'000ll, 4'000'000'000ll))));
            return;
        case NYdb::EPrimitiveType::Timestamp64:
            builder.Timestamp64(std::chrono::sys_time<NYdb::TWideMicroseconds>(
                NYdb::TWideMicroseconds(fdp.ConsumeIntegralInRange<i64>(-4'000'000'000'000ll, 4'000'000'000'000ll))));
            return;
        default:
            builder.Bool(false);
            return;
    }
}

inline void AppendValue(NYdb::TValueBuilder& builder, const TValueSchema& schema, FuzzedDataProvider& fdp) {
    switch (schema.Kind) {
        case TValueSchema::EKind::Primitive:
            AppendPrimitiveValue(builder, schema.Primitive, fdp);
            return;
        case TValueSchema::EKind::Optional:
            if (fdp.ConsumeBool()) {
                builder.EmptyOptional(BuildType(schema.Children.front()));
            } else {
                builder.BeginOptional();
                AppendValue(builder, schema.Children.front(), fdp);
                builder.EndOptional();
            }
            return;
        case TValueSchema::EKind::List: {
            const size_t itemCount = fdp.ConsumeIntegralInRange<size_t>(0, 3);
            if (itemCount == 0) {
                builder.EmptyList(BuildType(schema.Children.front()));
            } else {
                builder.BeginList();
                for (size_t i = 0; i < itemCount; ++i) {
                    builder.AddListItem();
                    AppendValue(builder, schema.Children.front(), fdp);
                }
                builder.EndList();
            }
            return;
        }
        case TValueSchema::EKind::Struct:
            builder.BeginStruct();
            for (size_t i = 0; i < schema.Children.size(); ++i) {
                builder.AddMember(schema.Names[i]);
                AppendValue(builder, schema.Children[i], fdp);
            }
            builder.EndStruct();
            return;
        case TValueSchema::EKind::Tuple:
            builder.BeginTuple();
            for (const auto& child : schema.Children) {
                builder.AddElement();
                AppendValue(builder, child, fdp);
            }
            builder.EndTuple();
            return;
        case TValueSchema::EKind::Dict: {
            const size_t itemCount = fdp.ConsumeIntegralInRange<size_t>(0, 3);
            if (itemCount == 0) {
                builder.EmptyDict(BuildType(schema.Children[0]), BuildType(schema.Children[1]));
            } else {
                builder.BeginDict();
                for (size_t i = 0; i < itemCount; ++i) {
                    builder.AddDictItem();
                    builder.DictKey();
                    AppendValue(builder, schema.Children[0], fdp);
                    builder.DictPayload();
                    AppendValue(builder, schema.Children[1], fdp);
                }
                builder.EndDict();
            }
            return;
        }
    }
}

inline NYdb::TValue BuildValue(const TValueSchema& schema, FuzzedDataProvider& fdp) {
    NYdb::TValueBuilder builder;
    AppendValue(builder, schema, fdp);
    return builder.Build();
}

inline TGeneratedColumn GenerateColumn(FuzzedDataProvider& fdp, size_t index, int depth = 3) {
    TValueSchema schema = GenerateSchema(fdp, depth);
    return TGeneratedColumn{
        MakeIdentifier(fdp, "col", index),
        BuildType(schema),
        BuildValue(schema, fdp),
    };
}

inline NYdb::TResultSet BuildResultSet(const TVector<TGeneratedColumn>& columns) {
    Ydb::ResultSet proto;

    for (const auto& column : columns) {
        auto* protoColumn = proto.add_columns();
        protoColumn->set_name(column.Name);
        *protoColumn->mutable_type() = NYdb::TProtoAccessor::GetProto(column.Type);
    }

    auto* row = proto.add_rows();
    for (const auto& column : columns) {
        *row->add_items() = NYdb::TProtoAccessor::GetProto(column.Value);
    }

    return NYdb::TResultSet(std::move(proto));
}

} // namespace NFuzzing::NValueFuzz
