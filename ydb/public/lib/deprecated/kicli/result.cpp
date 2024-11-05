#include "kicli.h"

#include <ydb/public/lib/deprecated/client/msgbus_client.h>
#include <ydb/library/yql/public/decimal/yql_decimal.h>

#include <util/generic/ymath.h>

namespace NKikimr {
namespace NClient {

TResult::TResult(NBus::EMessageStatus transportStatus)
    : TransportStatus(transportStatus)
{}

TResult::TResult(NBus::EMessageStatus transportStatus, const TString& message)
    : TransportStatus(transportStatus)
    , TransportErrorMessage(message)
{}

TResult::TResult(TAutoPtr<NBus::TBusMessage> reply)
    : TransportStatus(NBus::MESSAGE_OK)
    , Reply(reply.Release())
{}

ui16 TResult::GetType() const {
    return Reply == nullptr ? 0 : Reply.Get()->GetHeader()->Type;
}

template <> const NKikimrClient::TResponse& TResult::GetResult<NKikimrClient::TResponse>() const {
    Y_ABORT_UNLESS(GetType() == NMsgBusProxy::MTYPE_CLIENT_RESPONSE, "Unexpected response type: %d", GetType());
    return static_cast<NMsgBusProxy::TBusResponse*>(Reply.Get())->Record;
}

NMsgBusProxy::EResponseStatus TResult::GetStatus() const {
    if (TransportStatus != NBus::MESSAGE_OK) {
        switch (TransportStatus) {
        case NBus::MESSAGE_CONNECT_FAILED:
        case NBus::MESSAGE_SERVICE_UNKNOWN:
        case NBus::MESSAGE_DESERIALIZE_ERROR:
        case NBus::MESSAGE_HEADER_CORRUPTED:
        case NBus::MESSAGE_DECOMPRESS_ERROR:
        case NBus::MESSAGE_MESSAGE_TOO_LARGE:
        case NBus::MESSAGE_REPLY_FAILED:
        case NBus::MESSAGE_DELIVERY_FAILED:
        case NBus::MESSAGE_INVALID_VERSION:
        case NBus::MESSAGE_SERVICE_TOOMANY:
            return NMsgBusProxy::MSTATUS_ERROR;
        case NBus::MESSAGE_TIMEOUT:
            return NMsgBusProxy::MSTATUS_TIMEOUT;
        case NBus::MESSAGE_UNKNOWN:
            return NMsgBusProxy::MSTATUS_UNKNOWN;
        case NBus::MESSAGE_BUSY:
            return NMsgBusProxy::MSTATUS_REJECTED;
        case NBus::MESSAGE_SHUTDOWN:
            return NMsgBusProxy::MSTATUS_NOTREADY;
        case NBus::MESSAGE_OK:
            return NMsgBusProxy::MSTATUS_OK;
        default:
            return NMsgBusProxy::MSTATUS_INTERNALERROR;
        };
    } else
    if (GetType() == NMsgBusProxy::MTYPE_CLIENT_RESPONSE) {
        return static_cast<NMsgBusProxy::EResponseStatus>(GetResult<NKikimrClient::TResponse>().GetStatus());
    } else
    return NMsgBusProxy::MSTATUS_INTERNALERROR;
}

TError TResult::GetError() const {
    return TError(*this);
}

TQueryResult::TQueryResult(const TResult& result)
    : TResult(result)
{}

TValue TQueryResult::GetValue() const {
    const NKikimrClient::TResponse& response = GetResult<NKikimrClient::TResponse>();
    Y_ABORT_UNLESS(response.HasExecutionEngineEvaluatedResponse());
    const auto& result = response.GetExecutionEngineEvaluatedResponse();
    // TODO: type caching
    return TValue::Create(result.GetValue(), result.GetType());
}

TReadTableResult::TReadTableResult(const TResult& result)
    : TResult(result)
{}

const YdbOld::ResultSet &TReadTableResult::GetResultSet() const {
    if (!Parsed) {
        const NKikimrClient::TResponse& response = GetResult<NKikimrClient::TResponse>();
        const auto& result = response.GetSerializedReadTableResponse();
        Y_PROTOBUF_SUPPRESS_NODISCARD Result.ParseFromArray(result.data(), result.size());
    }
    return Result;
}

template <> TString TReadTableResult::ValueToString<TFormatCSV>(const YdbOld::Value &value,
                                                                const YdbOld::DataType &type) {
    switch (type.id()) {
    case NYql::NProto::Bool:
        return value.bool_value() ? "true" : "false";
    case NYql::NProto::Uint64:
        return ToString(value.uint64_value());
    case NScheme::NTypeIds::Int64:
        return ToString(value.int64_value());
    case NScheme::NTypeIds::Uint32:
        return ToString(value.uint32_value());
    case NScheme::NTypeIds::Int32:
        return ToString(value.int32_value());
    case NScheme::NTypeIds::Uint16:
        return ToString(static_cast<ui16>(value.uint32_value()));
    case NScheme::NTypeIds::Int16:
        return ToString(static_cast<i16>(value.int32_value()));
    case NScheme::NTypeIds::Uint8:
        return ToString(static_cast<ui8>(value.uint32_value()));
    case NScheme::NTypeIds::Int8:
        return ToString(static_cast<i8>(value.int32_value()));
    case NScheme::NTypeIds::Double:
        return ToString(value.double_value());
    case NScheme::NTypeIds::Float:
        return ToString(value.float_value());
    case NScheme::NTypeIds::Utf8:
    case NScheme::NTypeIds::Json:
    case NScheme::NTypeIds::Yson:
        return "\"" + TFormatCSV::EscapeString(value.text_value()) + "\"";
    case NScheme::NTypeIds::String:
        return value.bytes_value();
    case NScheme::NTypeIds::Date:
        {
            auto val = TInstant::Days(value.uint32_value());
            return val.FormatGmTime("%Y-%m-%d");
        }
    case NScheme::NTypeIds::Datetime:
        {
            auto val = TInstant::Seconds(value.uint32_value());
            return val.FormatGmTime("%Y-%m-%dT%H:%M:%SZ");
        }
    case NScheme::NTypeIds::Timestamp:
        {
            auto val = TInstant::MicroSeconds(value.uint64_value());
            return val.ToString();
        }
    case NScheme::NTypeIds::Interval:
        {
            i64 val = value.int64_value();
            if (val >= 0)
                return TDuration::MicroSeconds(static_cast<ui64>(val)).ToString();
            return TString("-") + TDuration::MicroSeconds(static_cast<ui64>(-val)).ToString();
        }
    case NScheme::NTypeIds::Date32:
        return ToString(value.int32_value());
    case NScheme::NTypeIds::Datetime64:
    case NScheme::NTypeIds::Timestamp64:
    case NScheme::NTypeIds::Interval64:
        return ToString(value.int64_value());
    case NScheme::NTypeIds::Decimal:
        {
            NYql::NDecimal::TInt128 val;
            auto p = reinterpret_cast<char*>(&val);
            reinterpret_cast<ui64*>(p)[0] = value.low_128();
            reinterpret_cast<ui64*>(p)[1] = value.high_128();
            // In Kikimr the only decimal column type supported is Decimal(22,9).
            return NYql::NDecimal::ToString(val, NScheme::DECIMAL_PRECISION, NScheme::DECIMAL_SCALE);
        }
    default:
        return "<UNSUPPORTED VALUE>";
    }
}

template <> TString TReadTableResult::ValueToString<TFormatCSV>(const YdbOld::Value &value,
                                                                const YdbOld::Type &type) {
    switch (type.type_type_case()) {
    case YdbOld::Type::kDataType:
        return ValueToString<TFormatCSV>(value, type.data_type());
    case YdbOld::Type::kOptionalType:
        if (value.Hasnull_flag_value())
            return "NULL";
        // If we have Value field for optional type then it is always
        // should be followed (even for Optional<Variant<T>> case).
        if (value.Hasnested_value())
            return ValueToString<TFormatCSV>(value.nested_value(), type.optional_type().item());
        return ValueToString<TFormatCSV>(value, type.optional_type().item());
    case YdbOld::Type::kListType:
        {
            TString res = "[";
            const auto &items = value.items();
            const auto &itemType = type.list_type().item();
            for (auto it = items.begin(); it != items.end(); ++it) {
                if (it != items.begin())
                    res += ",";
                res += ValueToString<TFormatCSV>(*it, itemType);
            }
            res += "]";
            return res;
        }
    case YdbOld::Type::kTupleType:
        {
            TString res = "[";
            const auto &items = value.items();
            const auto &types = type.tuple_type().elements();
            for (int i = 0; i < items.size(); ++i) {
                if (i)
                    res += ",";
                res += ValueToString<TFormatCSV>(items[i], types[i]);
            }
            res += "]";
            return res;
        }
    case YdbOld::Type::kStructType:
        {
            TString res = "{";
            const auto &items = value.items();
            const auto &members = type.struct_type().members();
            for (int i = 0; i < members.size(); ++i) {
                if (i)
                    res += ",";
                res += ValueToString<TFormatCSV>(items[i], members[i].type());
            }
            res += "}";
            return res;
        }
    case YdbOld::Type::kDictType:
        {
            TString res = "[";
            const auto &pairs = value.pairs();
            const auto &dictType = type.dict_type();
            for (int i = 0; i < pairs.size(); ++i) {
                if (i)
                    res += ",";
                res += ValueToString<TFormatCSV>(pairs[i].key(), dictType.key());
                res += ":";
                res += ValueToString<TFormatCSV>(pairs[i].payload(), dictType.payload());
            }
            res += "]";
            return res;
        }
    case YdbOld::Type::kVariantType:
        return "<VARIANT IS NOT SUPPORTED>";
    default:
        return "";
    }
}

template <> TString TReadTableResult::GetTypeText<TFormatCSV>(const TFormatCSV &format) const {
    auto &proto = GetResultSet();
    TString res;
    bool first = true;
    for (auto &meta : proto.column_meta()) {
        if (!first)
            res += format.Delim;
        first = false;
        res += meta.name();
    }
    return res;
}

template <> TString TReadTableResult::GetValueText<TFormatCSV>(const TFormatCSV &format) const {
    auto &proto = GetResultSet();
    TString res;

    if (format.PrintHeader) {
        res += GetTypeText(format);
        res += "\n";
    }

    auto &colTypes = proto.column_meta();
    bool first = true;
    for (auto &row : proto.rows()) {
        if (!first)
            res += "\n";
        first = false;
        for (int i = 0; i < colTypes.size(); ++i) {
            if (i)
                res += format.Delim;
            res += ValueToString<TFormatCSV>(row.items(i), colTypes[i].type());
        }
    }

    return res;
}

/// @warning It's mistake to store Query as a row pointer here. TQuery could be a tmp object.
///          TPrepareResult result = kikimr.Query(some).SyncPrepare();
///          TPreparedQuery query = result.GetQuery(); //< error here. Query is obsolete.
TPrepareResult::TPrepareResult(const TResult& result, const TQuery& query)
    : TResult(result)
    , Query(&query)
{}

TPreparedQuery TPrepareResult::GetQuery() const {
    const NKikimrClient::TResponse& response = GetResult<NKikimrClient::TResponse>();
    Y_ABORT_UNLESS(response.HasMiniKQLCompileResults());
    const auto& compileResult = response.GetMiniKQLCompileResults();
    Y_ABORT_UNLESS(compileResult.HasCompiledProgram(), "Compile error (%" PRIu64 "): %" PRIu32 ":%" PRIu32 " %s",
        compileResult.ProgramCompileErrorsSize(),
        (compileResult.ProgramCompileErrorsSize() ? compileResult.GetProgramCompileErrors(0).position().row() : 0u),
        (compileResult.ProgramCompileErrorsSize() ? compileResult.GetProgramCompileErrors(0).position().column() : 0u),
        (compileResult.ProgramCompileErrorsSize() ? compileResult.GetProgramCompileErrors(0).message().data() : "")
    );
    return TPreparedQuery(*Query, compileResult.GetCompiledProgram());
}

}
}
