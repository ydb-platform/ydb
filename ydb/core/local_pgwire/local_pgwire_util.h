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

namespace NLocalPgWire {

struct TTransactionState {
    char Status = 0;
    TString Id;
};

struct TConnectionState {
    TString SessionId;
    TTransactionState Transaction;
    uint32_t ConnectionNum = 0;
};

struct TPgWireAuthData {
    TActorId Sender;
    TString UserName;
    TString DatabasePath;
    TString Password;
    TString PeerName;
};

struct TParsedStatement {
    NPG::TPGParse::TQueryData QueryData;
    std::vector<Ydb::Type> ParameterTypes;
    std::vector<NPG::TEvPGEvents::TRowDescriptionField> DataFields;
};

struct TPortal : TParsedStatement {
    NPG::TPGBind::TBindData BindData;

    void Construct(const TParsedStatement& parsedStatement, NPG::TPGBind::TBindData&& bindData) {
        (TParsedStatement&)(*this) = parsedStatement;
        BindData = std::move(bindData);
    }
};

enum EFormatType : int16_t {
    EFormatText = 0,
    EFormatBinary = 1,
};

struct TEvEvents {
    enum EEv {
        EvProxyCompleted = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvUpdateStatement,
        EvSingleQuery,
        EvCancelRequest,
        EvAuthResponse,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "ES_PRIVATE event space is too small.");

    struct TEvProxyCompleted : NActors::TEventLocal<TEvProxyCompleted, EvProxyCompleted> {
        TConnectionState Connection;

        TEvProxyCompleted() = default;

        TEvProxyCompleted(const TConnectionState& connection)
            : Connection(connection)
        {}
    };

    struct TEvUpdateStatement : NActors::TEventLocal<TEvUpdateStatement, EvUpdateStatement> {
        TParsedStatement ParsedStatement;

        TEvUpdateStatement() = default;

        TEvUpdateStatement(const TParsedStatement& parsedStatement)
            : ParsedStatement(parsedStatement)
        {}
    };

    struct TEvSingleQuery : NActors::TEventLocal<TEvSingleQuery, EvSingleQuery> {
        TString Query;
        bool FinalQuery = true;

        TEvSingleQuery(const TString& query, bool finalQuery)
            : Query(query)
            , FinalQuery(finalQuery)
        {}

        static std::unique_ptr<NPG::TEvPGEvents::TEvQueryResponse> Reply() {
            return std::make_unique<NPG::TEvPGEvents::TEvQueryResponse>();
        }
    };

    struct TEvCancelRequest : NActors::TEventLocal<TEvCancelRequest, EvCancelRequest> {
        TEvCancelRequest() = default;
    };

    struct TEvAuthResponse : NActors::TEventLocal<TEvAuthResponse, EvAuthResponse> {
        TString SerializedToken;
        TString Ticket;

        TString ErrorMessage;

        TActorId Sender;

        TEvAuthResponse(const TString& serializedToken, const TString& ticket, const TActorId& sender)
            : SerializedToken(serializedToken)
            , Ticket(ticket)
            , Sender(sender)
        {}

        TEvAuthResponse(const TString& errorMessage, const TActorId& sender)
            : ErrorMessage(errorMessage)
            , Sender(sender)
        {}
    };
};

TString ColumnPrimitiveValueToString(NYdb::TValueParser& valueParser);
TString ColumnValueToString(NYdb::TValueParser& valueParser);
NPG::TEvPGEvents::TRowValueField ColumnValueToRowValueField(NYdb::TValueParser& valueParser, int16_t format = EFormatText);
uint32_t GetPgOidFromYdbType(NYdb::TType type);
std::optional<NYdb::TPgType> GetPgTypeFromYdbType(NYdb::TType type);
Ydb::TypedValue GetTypedValueFromParam(int16_t format, const std::vector<uint8_t>& value, const Ydb::Type& type);
NYdb::NScripting::TExecuteYqlResult ConvertProtoResponseToSdkResult(Ydb::Scripting::ExecuteYqlResponse&& proto);
void FillResultSet(const NYdb::TResultSet& resultSet, std::vector<NPG::TEvPGEvents::TDataRow>& dataRows, const std::vector<int16_t>& format = {});
bool IsQueryEmpty(TStringBuf query);

} //namespace NLocalPgWire
