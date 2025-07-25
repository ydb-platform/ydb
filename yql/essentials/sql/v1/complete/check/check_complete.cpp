#include "check_complete.h"

#include <yql/essentials/sql/v1/complete/sql_complete.h>
#include <yql/essentials/sql/v1/complete/analysis/yql/yql.h>
#include <yql/essentials/sql/v1/complete/name/cluster/static/discovery.h>
#include <yql/essentials/sql/v1/complete/name/object/simple/static/schema.h>
#include <yql/essentials/sql/v1/complete/name/service/cluster/name_service.h>
#include <yql/essentials/sql/v1/complete/name/service/schema/name_service.h>
#include <yql/essentials/sql/v1/complete/name/service/static/name_service.h>
#include <yql/essentials/sql/v1/complete/name/service/union/name_service.h>

#include <yql/essentials/sql/v1/lexer/antlr4_pure/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_pure_ansi/lexer.h>

#include <util/charset/utf8.h>
#include <util/random/random.h>

namespace NSQLComplete {

    namespace {

        TLexerSupplier MakePureLexerSupplier() {
            NSQLTranslationV1::TLexers lexers;
            lexers.Antlr4Pure = NSQLTranslationV1::MakeAntlr4PureLexerFactory();
            lexers.Antlr4PureAnsi = NSQLTranslationV1::MakeAntlr4PureAnsiLexerFactory();
            return [lexers = std::move(lexers)](bool ansi) {
                return NSQLTranslationV1::MakeLexer(
                    lexers, ansi, /* antlr4 = */ true,
                    NSQLTranslationV1::ELexerFlavor::Pure);
            };
        }

        INameService::TPtr MakeClusterNameService(const TYqlContext& ctx) {
            THashSet<TString> clusterSet = ctx.Clusters();
            TVector<TString> clusterVec(begin(clusterSet), end(clusterSet));
            Sort(clusterVec);

            return MakeClusterNameService(MakeStaticClusterDiscovery(std::move(clusterVec)));
        }

        INameService::TPtr MakeSchemaNameService(const TYqlContext& ctx) {
            THashMap<TString, THashMap<TString, TVector<TFolderEntry>>> fs;
            for (const auto& [cluster, tables] : ctx.TablesByCluster) {
                for (TString table : tables) {
                    fs[cluster]["/"].push_back(TFolderEntry{
                        .Type = TFolderEntry::Table,
                        .Name = std::move(table),
                    });
                }
            }

            return MakeSchemaNameService(MakeSimpleSchema(MakeStaticSimpleSchema({.Folders = std::move(fs)})));
        }

    } // namespace

    bool CheckComplete(TStringBuf query, TYqlContext ctx) {
        constexpr size_t Seed = 97651231;
        constexpr size_t Attempts = 64;
        constexpr size_t MaxAttempts = 256;
        SetRandomSeed(Seed);

        auto service = MakeUnionNameService(
            {
                MakeClusterNameService(ctx),
                MakeSchemaNameService(ctx),
            },
            MakeDefaultRanking());

        auto engine = MakeSqlCompletionEngine(
            MakePureLexerSupplier(),
            std::move(service),
            MakeYQLConfiguration());

        for (size_t i = 0, j = 0; i < Attempts && j < MaxAttempts; ++j) {
            size_t pos = RandomNumber<size_t>(query.size() + 1);
            if (pos < query.size() && IsUTF8ContinuationByte(query.at(pos))) {
                continue;
            }

            TCompletionInput input = {
                .Text = query,
                .CursorPosition = pos,
            };

            auto output = engine->Complete(input).ExtractValueSync();
            Y_DO_NOT_OPTIMIZE_AWAY(output);

            i += 1;
        }

        return true;
    }

    bool CheckComplete(TStringBuf query, NYql::TExprNode::TPtr root, NYql::TExprContext& ctx, NYql::TIssues& issues) try {
        return CheckComplete(query, MakeYqlAnalysis()->Analyze(root, ctx));
    } catch (...) {
        issues.AddIssue(FormatCurrentException());
        return false;
    }

    bool CheckComplete(TStringBuf query, NYql::TAstNode& root, NYql::TIssues& issues) try {
        return CheckComplete(query, MakeYqlAnalysis()->Analyze(root, issues).GetOrElse({}));
    } catch (...) {
        issues.AddIssue(FormatCurrentException());
        return false;
    }

} // namespace NSQLComplete
