#include "user_facing.h"

#include <ydb/core/kqp/common/simple/helpers.h>
#include <ydb/library/security/util.h>

#include <google/protobuf/any.pb.h>
#include <util/string/builder.h>

namespace NKikimr::NKqp {

namespace {

const char* GetTableSinkModeVerb(NKikimrKqp::TKqpTableSinkSettings::EType mode) {
    switch (mode) {
        case NKikimrKqp::TKqpTableSinkSettings::MODE_FILL:             return "FILL";
        case NKikimrKqp::TKqpTableSinkSettings::MODE_REPLACE:          return "REPLACE";
        case NKikimrKqp::TKqpTableSinkSettings::MODE_UPSERT:           return "UPSERT";
        case NKikimrKqp::TKqpTableSinkSettings::MODE_UPSERT_INCREMENT: return "UPSERT INCREMENT";
        case NKikimrKqp::TKqpTableSinkSettings::MODE_INSERT:           return "INSERT";
        case NKikimrKqp::TKqpTableSinkSettings::MODE_DELETE:           return "DELETE";
        case NKikimrKqp::TKqpTableSinkSettings::MODE_UPDATE:           return "UPDATE";
        default:                                                       return nullptr;
    }
}

TUserFacingQueryDescription DescribePhysicalQuery(const NKqpProto::TKqpPhyQuery& query,
        const TMaybe<TString>& commandTag) {
    TString inferredWriteVerb;
    TString writeTable;
    TString readTable;
    bool hasReads = false;
    bool multiWrite = false;
    bool multiRead = false;
    bool mixedWriteVerbs = false;
    auto noteWriteVerb = [&](TStringBuf verb) {
        mixedWriteVerbs = mixedWriteVerbs || (inferredWriteVerb && inferredWriteVerb != verb);
        if (!inferredWriteVerb) {
            inferredWriteVerb = verb;
        }
    };
    auto noteTable = [](TString& table, bool& multi, const TString& path) {
        if (path) {
            multi = multi || (table && table != path);
            table = table ? table : path;
        }
    };
    auto noteTableSink = [&](const NKqpProto::TKqpInternalSink& sink) {
        if (!sink.GetSettings().Is<NKikimrKqp::TKqpTableSinkSettings>()) {
            return;
        }
        NKikimrKqp::TKqpTableSinkSettings settings;
        if (sink.GetSettings().UnpackTo(&settings)) {
            if (const char* verb = GetTableSinkModeVerb(settings.GetType())) {
                noteWriteVerb(verb);
                noteTable(writeTable, multiWrite, settings.GetTable().GetPath());
            }
        }
    };
    for (const auto& tx : query.GetTransactions()) {
        if (tx.GetType() == NKqpProto::TKqpPhyTx::TYPE_SCHEME) {
            return {"DDL", "DDL"};
        }
        for (const auto& stage : tx.GetStages()) {
            for (const auto& sink : stage.GetSinks()) {
                if (sink.GetTypeCase() == NKqpProto::TKqpSink::kInternalSink) {
                    noteTableSink(sink.GetInternalSink());
                }
            }
            for (const auto& transform : stage.GetOutputTransforms()) {
                if (transform.GetTypeCase() == NKqpProto::TKqpOutputTransform::kInternalSink) {
                    noteTableSink(transform.GetInternalSink());
                }
            }
            for (const auto& op : stage.GetTableOps()) {
                switch (op.GetTypeCase()) {
                    case NKqpProto::TKqpPhyTableOperation::kUpsertRows:
                        noteWriteVerb("UPSERT");
                        noteTable(writeTable, multiWrite, op.GetTable().GetPath());
                        break;
                    case NKqpProto::TKqpPhyTableOperation::kDeleteRows:
                        noteWriteVerb("DELETE");
                        noteTable(writeTable, multiWrite, op.GetTable().GetPath());
                        break;
                    case NKqpProto::TKqpPhyTableOperation::kReadRange:
                    case NKqpProto::TKqpPhyTableOperation::kReadOlapRange:
                    case NKqpProto::TKqpPhyTableOperation::kReadRanges:
                        hasReads = true;
                        noteTable(readTable, multiRead, op.GetTable().GetPath());
                        break;
                    case NKqpProto::TKqpPhyTableOperation::TYPE_NOT_SET:
                        break;
                }
            }
            for (const auto& source : stage.GetSources()) {
                hasReads = true;
                switch (source.GetTypeCase()) {
                    case NKqpProto::TKqpSource::kReadRangesSource:
                        noteTable(readTable, multiRead, source.GetReadRangesSource().GetTable().GetPath());
                        break;
                    case NKqpProto::TKqpSource::kFullTextSource:
                        noteTable(readTable, multiRead, source.GetFullTextSource().GetTable().GetPath());
                        break;
                    case NKqpProto::TKqpSource::kSysViewSource:
                        noteTable(readTable, multiRead, source.GetSysViewSource().GetTable().GetPath());
                        break;
                    case NKqpProto::TKqpSource::kExternalSource:
                    case NKqpProto::TKqpSource::TYPE_NOT_SET:
                        break;
                }
            }
            for (const auto& input : stage.GetInputs()) {
                switch (input.GetTypeCase()) {
                    case NKqpProto::TKqpPhyConnection::kStreamLookup:
                        hasReads = true;
                        noteTable(readTable, multiRead, input.GetStreamLookup().GetTable().GetPath());
                        break;
                    case NKqpProto::TKqpPhyConnection::kVectorResolve:
                        hasReads = true;
                        noteTable(readTable, multiRead, input.GetVectorResolve().GetTable().GetPath());
                        break;
                    case NKqpProto::TKqpPhyConnection::kVectorSearch:
                        hasReads = true;
                        noteTable(readTable, multiRead, input.GetVectorSearch().GetTable().GetPath());
                        break;
                    default:
                        break;
                }
            }
        }
    }

    TString operation = commandTag.GetOrElse(TString{});
    if (!operation) {
        if (inferredWriteVerb) {
            if (mixedWriteVerbs) {
                return {"EXECUTE SCRIPT", "EXECUTE SCRIPT"};
            }
            operation = inferredWriteVerb;
        } else if (hasReads || query.ResultBindingsSize() > 0) {
            operation = "SELECT";
        }
    }
    if (!operation) {
        return {};
    }

    const bool isRead = operation == "SELECT";
    const TString& table = isRead ? readTable : writeTable;
    const bool ambiguousTable = isRead ? multiRead : multiWrite;
    return {
        !table || ambiguousTable
            ? operation : TStringBuilder() << operation << " " << table,
        operation,
    };
}

} // namespace

TUserFacingQueryDescription DescribeUserFacingQuery(NKikimrKqp::EQueryType queryType,
        size_t statementCount, const NKqpProto::TKqpPhyQuery& physicalQuery,
        const TMaybe<TString>& commandTag) {
    switch (queryType) {
        case NKikimrKqp::QUERY_TYPE_SQL_SCRIPT:
        case NKikimrKqp::QUERY_TYPE_SQL_SCRIPT_STREAMING:
        case NKikimrKqp::QUERY_TYPE_SQL_GENERIC_SCRIPT:
            return {"EXECUTE SCRIPT", "EXECUTE SCRIPT"};
        default:
            break;
    }
    if (statementCount > 1) {
        return {"EXECUTE SCRIPT", "EXECUTE SCRIPT"};
    }
    return DescribePhysicalQuery(physicalQuery, commandTag);
}

TString ProtectUserFacingQueryText(const TString& text) {
    return NKikimr::ProtectQueryForLoggingIfSensitive(text);
}

TString FallbackUserFacingQueryName(NKikimrKqp::EQueryType queryType,
        NKikimrKqp::EQueryAction queryAction) {
    switch (queryType) {
        case NKikimrKqp::QUERY_TYPE_SQL_DDL:
            return "DDL";
        case NKikimrKqp::QUERY_TYPE_SQL_SCRIPT:
        case NKikimrKqp::QUERY_TYPE_SQL_SCRIPT_STREAMING:
        case NKikimrKqp::QUERY_TYPE_SQL_GENERIC_SCRIPT:
            return "EXECUTE SCRIPT";
        default:
            break;
    }
    TString name = NKikimrKqp::EQueryAction_Name(queryAction);
    constexpr TStringBuf prefix = "QUERY_ACTION_";
    if (name.StartsWith(prefix)) {
        name = name.substr(prefix.size());
    }
    return name;
}

} // namespace NKikimr::NKqp
