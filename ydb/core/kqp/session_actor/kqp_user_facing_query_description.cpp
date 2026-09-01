#include "kqp_user_facing_tracing.h"

#include "kqp_query_state.h"

#include <ydb/core/kqp/common/simple/helpers.h>
#include <ydb/library/security/util.h>
#include <yql/essentials/sql/v1/format/sql_format.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4_ansi/proto_parser.h>

#include <google/protobuf/arena.h>
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

TUserFacingQueryDescription DescribeUserFacingQuery(const TKqpQueryState& state) {
    switch (state.GetType()) {
        case NKikimrKqp::QUERY_TYPE_SQL_SCRIPT:
        case NKikimrKqp::QUERY_TYPE_SQL_SCRIPT_STREAMING:
        case NKikimrKqp::QUERY_TYPE_SQL_GENERIC_SCRIPT:
            return {"EXECUTE SCRIPT", "EXECUTE SCRIPT"};
        default:
            break;
    }
    if (state.Statements.size() > 1) {
        return {"EXECUTE SCRIPT", "EXECUTE SCRIPT"};
    }
    return DescribePhysicalQuery(state.PreparedQuery->GetPhysicalQuery(), state.CommandTagName);
}

TString SanitizeUserFacingQueryText(const TString& text) {
    TString protectedText;
    if (NKikimr::ProtectQueryForLoggingIfSensitive(text, protectedText)) {
        return protectedText;
    }
    struct TSqlFactories {
        NSQLTranslationV1::TLexers Lexers;
        NSQLTranslationV1::TParsers Parsers;
    };
    static const TSqlFactories factories = [] {
        TSqlFactories result;
        result.Lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
        result.Lexers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiLexerFactory();
        result.Parsers.Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory();
        result.Parsers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiParserFactory();
        return result;
    }();
    try {
        google::protobuf::Arena arena;
        NSQLTranslation::TTranslationSettings settings;
        settings.Arena = &arena;
        TString obfuscated;
        NYql::TIssues issues;
        if (NSQLFormat::MakeSqlFormatter(factories.Lexers, factories.Parsers, settings)->Format(
                text, obfuscated, issues, NSQLFormat::EFormatMode::Obfuscate)) {
            return obfuscated;
        }
    } catch (const yexception&) {
    }
    return {};
}

TString FallbackUserFacingQueryName(const TKqpQueryState& state) {
    switch (state.GetType()) {
        case NKikimrKqp::QUERY_TYPE_SQL_DDL:
            return "DDL";
        case NKikimrKqp::QUERY_TYPE_SQL_SCRIPT:
        case NKikimrKqp::QUERY_TYPE_SQL_SCRIPT_STREAMING:
        case NKikimrKqp::QUERY_TYPE_SQL_GENERIC_SCRIPT:
            return "EXECUTE SCRIPT";
        default:
            break;
    }
    TString name = NKikimrKqp::EQueryAction_Name(state.GetAction());
    constexpr TStringBuf prefix = "QUERY_ACTION_";
    if (name.StartsWith(prefix)) {
        name = name.substr(prefix.size());
    }
    return name;
}

} // namespace NKikimr::NKqp
