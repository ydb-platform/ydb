#include "proto_parser.h"

#include <yql/essentials/utils/yql_panic.h>

#include <yql/essentials/parser/proto_ast/collect_issues/collect_issues.h>


#include <yql/essentials/sql/v1/proto_parser/antlr3/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr3_ansi/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4_ansi/proto_parser.h>

#include <util/generic/algorithm.h>

#if defined(_tsan_enabled_)
#include <util/system/mutex.h>
#endif

using namespace NYql;

namespace NSQLTranslationV1 {


#if defined(_tsan_enabled_)
    TMutex SanitizerSQLTranslationMutex;
#endif

namespace {

void ReportError(NAST::IErrorCollector& err, const TString& name) {
    err.Error(0, 0, TStringBuilder() << "Parser " << name << " is not supported");
}

}

google::protobuf::Message* SqlAST(const TParsers& parsers, const TString& query, const TString& queryName, TIssues& err,
    size_t maxErrors, bool ansiLexer, bool anlr4Parser, google::protobuf::Arena* arena) {
    NSQLTranslation::TErrorCollectorOverIssues collector(err, maxErrors, queryName);
    return SqlAST(parsers, query, queryName, collector, ansiLexer, anlr4Parser, arena);
}

google::protobuf::Message* SqlAST(const TParsers& parsers, const TString& query, const TString& queryName, NAST::IErrorCollector& err,
    bool ansiLexer, bool anlr4Parser, google::protobuf::Arena* arena) {
    YQL_ENSURE(arena);
#if defined(_tsan_enabled_)
    TGuard<TMutex> grd(SanitizerSQLTranslationMutex);
#endif
    if (ansiLexer && !anlr4Parser) {
        google::protobuf::Message* res = nullptr;
        if (parsers.Antlr3Ansi) {
            res = parsers.Antlr3Ansi->MakeParser()->Parse(query, queryName, err, arena);
            if (!res) {
                return res;
            }
        } else {
            ReportError(err, "antlr3_ansi");
            return nullptr;
        }

        return res;
    } else if (!ansiLexer && !anlr4Parser) {
        google::protobuf::Message* res = nullptr;
        if (parsers.Antlr3) {
            res = parsers.Antlr3->MakeParser()->Parse(query, queryName, err, arena);
            if (!res) {
                return res;
            }
        } else {
            ReportError(err, "antlr3");
            return nullptr;
        }

        return res;
    } else if (ansiLexer && anlr4Parser) {
        google::protobuf::Message* res = nullptr;
        if (parsers.Antlr4Ansi) {
            res = parsers.Antlr4Ansi->MakeParser()->Parse(query, queryName, err, arena);
            if (!res) {
                return res;
            }
        } else {
            ReportError(err, "antlr4_ansi");
            return nullptr;
        }

        return res;
    } else {
        google::protobuf::Message* res = nullptr;
        if (parsers.Antlr4) {
            res = parsers.Antlr4->MakeParser()->Parse(query, queryName, err, arena);
            if (!res) {
                return res;
            }
        } else {
            ReportError(err, "antlr4");
            return nullptr;
        }

        return res;
    }
}

} // namespace NSQLTranslationV1
