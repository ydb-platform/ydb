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

TString DescribePhysicalQuery(const NKqpProto::TKqpPhyQuery& query) {
    TString writeVerb;
    TString writeTable;
    TString readTable;
    bool hasReads = false;
    bool multiWrite = false;
    bool multiRead = false;
    bool mixedWriteVerbs = false;
    auto noteWriteVerb = [&](TStringBuf verb) {
        mixedWriteVerbs = mixedWriteVerbs || (writeVerb && writeVerb != verb);
        if (!writeVerb) {
            writeVerb = verb;
        }
    };
    auto noteTable = [](TString& table, bool& multi, const TString& path) {
        if (path) {
            multi = multi || (table && table != path);
            table = table ? table : path;
        }
    };
    for (const auto& tx : query.GetTransactions()) {
        if (tx.GetType() == NKqpProto::TKqpPhyTx::TYPE_SCHEME) {
            return "DDL";
        }
        for (const auto& stage : tx.GetStages()) {
            for (const auto& sink : stage.GetSinks()) {
                if (sink.GetTypeCase() != NKqpProto::TKqpSink::kInternalSink
                        || !sink.GetInternalSink().GetSettings().Is<NKikimrKqp::TKqpTableSinkSettings>()) {
                    continue;
                }
                NKikimrKqp::TKqpTableSinkSettings settings;
                if (sink.GetInternalSink().GetSettings().UnpackTo(&settings)) {
                    if (const char* verb = GetTableSinkModeVerb(settings.GetType())) {
                        noteWriteVerb(verb);
                        noteTable(writeTable, multiWrite, settings.GetTable().GetPath());
                    }
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
                    default:
                        hasReads = true;
                        noteTable(readTable, multiRead, op.GetTable().GetPath());
                        break;
                }
            }
            for (const auto& source : stage.GetSources()) {
                hasReads = true;
                if (source.GetTypeCase() == NKqpProto::TKqpSource::kReadRangesSource) {
                    noteTable(readTable, multiRead, source.GetReadRangesSource().GetTable().GetPath());
                }
            }
        }
    }
    if (writeVerb) {
        if (mixedWriteVerbs) {
            return "WRITE";
        }
        return multiWrite || !writeTable
            ? writeVerb : TStringBuilder() << writeVerb << " " << writeTable;
    }
    if (hasReads || query.ResultBindingsSize() > 0) {
        return multiRead || !readTable
            ? TString("SELECT") : TStringBuilder() << "SELECT " << readTable;
    }
    return {};
}

} // namespace

TString DescribeUserFacingQuery(const TKqpQueryState& state) {
    switch (state.GetType()) {
        case NKikimrKqp::QUERY_TYPE_SQL_SCRIPT:
        case NKikimrKqp::QUERY_TYPE_SQL_SCRIPT_STREAMING:
        case NKikimrKqp::QUERY_TYPE_SQL_GENERIC_SCRIPT:
            return "EXECUTE SCRIPT";
        default:
            break;
    }
    if (state.Statements.size() > 1) {
        return "EXECUTE SCRIPT";
    }
    TString result = DescribePhysicalQuery(state.PreparedQuery->GetPhysicalQuery());
    if (!result && state.CommandTagName) {
        result = *state.CommandTagName;
    }
    return result;
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
    TString name = NKikimrKqp::EQueryAction_Name(state.GetAction());
    constexpr TStringBuf prefix = "QUERY_ACTION_";
    if (name.StartsWith(prefix)) {
        name = name.substr(prefix.size());
    }
    return name;
}

} // namespace NKikimr::NKqp
