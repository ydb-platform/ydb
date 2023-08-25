#pragma once
#include <library/cpp/string_utils/base64/base64.h>

#include <ydb/core/pgproxy/pg_proxy_types.h>
#include <ydb/core/pgproxy/pg_proxy_events.h>

#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <ydb/public/sdk/cpp/client/draft/ydb_scripting.h>
#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/plain_status/status.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_types/operation/operation.h>
#include <ydb/public/sdk/cpp/client/ydb_value/value.h>
#include <ydb/public/api/grpc/ydb_scripting_v1.grpc.pb.h>

#include <util/string/builder.h>
#include <util/generic/string.h>

// temporary borrowed from postgresql/src/backend/catalog/pg_type_d.h

#define BOOLOID 16
#define BYTEAOID 17
#define CHAROID 18
#define NAMEOID 19
#define INT8OID 20
#define INT2OID 21
#define INT2VECTOROID 22
#define INT4OID 23
#define REGPROCOID 24
#define TEXTOID 25

//

namespace NLocalPgWire {

struct TTransactionState {
    char Status = 0;
    TString Id;
};

struct TConnectionState {
    TString SessionId;
    TTransactionState Transaction;
};

struct TParsedStatement {
    NPG::TPGParse::TQueryData QueryData;
    NPG::TPGBind::TBindData BindData;
    std::vector<Ydb::Type> ParameterTypes;
    std::vector<NPG::TEvPGEvents::TRowDescriptionField> DataFields;
};

struct TEvEvents {
    enum EEv {
        EvProxyCompleted = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "ES_PRIVATE event space is too small.");

    struct TEvProxyCompleted : NActors::TEventLocal<TEvProxyCompleted, EvProxyCompleted> {
        std::optional<TConnectionState> Connection;
        std::optional<TParsedStatement> ParsedStatement;

        TEvProxyCompleted() = default;

        TEvProxyCompleted(const TConnectionState& connection)
            : Connection(connection)
        {}

        TEvProxyCompleted(const TParsedStatement& parsedStatement)
            : ParsedStatement(parsedStatement)
        {}
    };
};

inline TString ColumnPrimitiveValueToString(NYdb::TValueParser& valueParser) {
    switch (valueParser.GetPrimitiveType()) {
        case NYdb::EPrimitiveType::Bool:
            return TStringBuilder() << valueParser.GetBool();
        case NYdb::EPrimitiveType::Int8:
            return TStringBuilder() << valueParser.GetInt8();
        case NYdb::EPrimitiveType::Uint8:
            return TStringBuilder() << valueParser.GetUint8();
        case NYdb::EPrimitiveType::Int16:
            return TStringBuilder() << valueParser.GetInt16();
        case NYdb::EPrimitiveType::Uint16:
            return TStringBuilder() << valueParser.GetUint16();
        case NYdb::EPrimitiveType::Int32:
            return TStringBuilder() << valueParser.GetInt32();
        case NYdb::EPrimitiveType::Uint32:
            return TStringBuilder() << valueParser.GetUint32();
        case NYdb::EPrimitiveType::Int64:
            return TStringBuilder() << valueParser.GetInt64();
        case NYdb::EPrimitiveType::Uint64:
            return TStringBuilder() << valueParser.GetUint64();
        case NYdb::EPrimitiveType::Float:
            return TStringBuilder() << valueParser.GetFloat();
        case NYdb::EPrimitiveType::Double:
            return TStringBuilder() << valueParser.GetDouble();
        case NYdb::EPrimitiveType::Utf8:
            return TStringBuilder() << valueParser.GetUtf8();
        case NYdb::EPrimitiveType::Date:
            return valueParser.GetDate().ToString();
        case NYdb::EPrimitiveType::Datetime:
            return valueParser.GetDatetime().ToString();
        case NYdb::EPrimitiveType::Timestamp:
            return valueParser.GetTimestamp().ToString();
        case NYdb::EPrimitiveType::Interval:
            return TStringBuilder() << valueParser.GetInterval();
        case NYdb::EPrimitiveType::TzDate:
            return valueParser.GetTzDate();
        case NYdb::EPrimitiveType::TzDatetime:
            return valueParser.GetTzDatetime();
        case NYdb::EPrimitiveType::TzTimestamp:
            return valueParser.GetTzTimestamp();
        case NYdb::EPrimitiveType::String:
            return Base64Encode(valueParser.GetString());
        case NYdb::EPrimitiveType::Yson:
            return valueParser.GetYson();
        case NYdb::EPrimitiveType::Json:
            return valueParser.GetJson();
        case NYdb::EPrimitiveType::JsonDocument:
            return valueParser.GetJsonDocument();
        case NYdb::EPrimitiveType::DyNumber:
            return valueParser.GetDyNumber();
        case NYdb::EPrimitiveType::Uuid:
            return {};
    }
    return {};
}

inline TString ColumnValueToString(NYdb::TValueParser& valueParser) {
    switch (valueParser.GetKind()) {
    case NYdb::TTypeParser::ETypeKind::Primitive:
        return ColumnPrimitiveValueToString(valueParser);
    case NYdb::TTypeParser::ETypeKind::Optional: {
        TString value;
        valueParser.OpenOptional();
        if (valueParser.IsNull()) {
            value = "NULL";
        } else {
            value = ColumnValueToString(valueParser);
        }
        valueParser.CloseOptional();
        return value;
    }
    case NYdb::TTypeParser::ETypeKind::Tuple: {
        TString value;
        valueParser.OpenTuple();
        while (valueParser.TryNextElement()) {
            if (!value.empty()) {
                value += ',';
            }
            value += ColumnValueToString(valueParser);
        }
        valueParser.CloseTuple();
        return value;
    }
    case NYdb::TTypeParser::ETypeKind::Pg: {
        return valueParser.GetPg().Content_;
    }
    default:
        return {};
    }
}

inline NPG::TEvPGEvents::TRowValueField ColumnValueToRowValueField(NYdb::TValueParser& valueParser) {
    switch (valueParser.GetKind()) {
    case NYdb::TTypeParser::ETypeKind::Primitive:
        return {.Value = ColumnPrimitiveValueToString(valueParser)};
    case NYdb::TTypeParser::ETypeKind::Optional: {
        NPG::TEvPGEvents::TRowValueField value;
        valueParser.OpenOptional();
        if (!valueParser.IsNull()) {
            value = ColumnValueToRowValueField(valueParser);
        }
        valueParser.CloseOptional();
        return value;
    }
    case NYdb::TTypeParser::ETypeKind::Tuple: {
        TString value;
        valueParser.OpenTuple();
        while (valueParser.TryNextElement()) {
            if (!value.empty()) {
                value += ',';
            }
            value += ColumnValueToString(valueParser);
        }
        valueParser.CloseTuple();
        return {.Value = value};
    }
    case NYdb::TTypeParser::ETypeKind::Pg: {
        auto pg = valueParser.GetPg();
        if (!pg.IsNull()) {
            return {.Value = valueParser.GetPg().Content_};
        } else {
            return {};
        }
    }
    default:
        return {};
    }
}

inline uint32_t GetPgOidFromYdbType(NYdb::TType type) {
    NYdb::TTypeParser parser(type);
    switch (parser.GetKind()) {
        case NYdb::TTypeParser::ETypeKind::Pg: {
            return parser.GetPg().Oid;
        default:
            return {};
        }
    }
}

inline std::optional<NYdb::TPgType> GetPgTypeFromYdbType(NYdb::TType type) {
    NYdb::TTypeParser parser(type);
    switch (parser.GetKind()) {
        case NYdb::TTypeParser::ETypeKind::Pg: {
            return parser.GetPg();
        default:
            return {};
        }
    }
}

inline NYdb::NScripting::TExecuteYqlResult ConvertProtoResponseToSdkResult(Ydb::Scripting::ExecuteYqlResponse&& proto) {
    TVector<NYdb::TResultSet> res;
    TMaybe<NYdb::NTable::TQueryStats> queryStats;
    {
        Ydb::Scripting::ExecuteYqlResult result;
        proto.mutable_operation()->mutable_result()->UnpackTo(&result);
        for (int i = 0; i < result.result_sets_size(); i++) {
            res.emplace_back(std::move(*result.mutable_result_sets(i)));
        }
        if (result.has_query_stats()) {
            queryStats = NYdb::NTable::TQueryStats(std::move(*result.mutable_query_stats()));
        }
    }
    NYdb::TPlainStatus alwaysSuccess;
    return {NYdb::TStatus(std::move(alwaysSuccess)), std::move(res), queryStats};
}

inline bool IsQueryEmptyChar(char c) {
    return c == ' ' || c == ';' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v';
}

inline bool IsQueryEmpty(TStringBuf query) {
    for (char c : query) {
        if (!IsQueryEmptyChar(c)) {
            return false;
        }
    }
    return true;
}

} //namespace NLocalPgWire
