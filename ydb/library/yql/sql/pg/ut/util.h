#include <contrib/libs/fmt/include/fmt/format.h>

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/sql/sql.h>
#include <ydb/library/yql/parser/pg_catalog/catalog.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/config.h>

enum class EDebugOutput {
    None,
    ToCerr,
};

inline TString Err2Str(NYql::TAstParseResult& res, EDebugOutput debug = EDebugOutput::None) {
    TStringStream s;
    res.Issues.PrintTo(s);

    if (debug == EDebugOutput::ToCerr) {
        Cerr << s.Str() << Endl;
    }
    return s.Str();
}

inline NYql::TAstParseResult SqlToYqlWithMode(const TString& query, NSQLTranslation::ESqlMode mode = NSQLTranslation::ESqlMode::QUERY, size_t maxErrors = 10, const TString& provider = {},
    EDebugOutput debug = EDebugOutput::None, bool ansiLexer = false, NSQLTranslation::TTranslationSettings settings = {})
{
    google::protobuf::Arena arena;
    const auto service = provider ? provider : TString(NYql::YtProviderName);
    const TString cluster = "plato";
    settings.ClusterMapping[cluster] = service;
    settings.ClusterMapping["hahn"] = NYql::YtProviderName;
    settings.ClusterMapping["mon"] = NYql::SolomonProviderName;
    settings.ClusterMapping[""] = NYql::KikimrProviderName;
    settings.MaxErrors = maxErrors;
    settings.Mode = mode;
    settings.Arena = &arena;
    settings.AnsiLexer = ansiLexer;
    settings.SyntaxVersion = 1;
    settings.PgParser = true;
    auto res = SqlToYql(query, settings);
    if (debug == EDebugOutput::ToCerr) {
        Err2Str(res, debug);
    }
    return res;
}

inline NYql::TAstParseResult PgSqlToYql(const TString& query, size_t maxErrors = 10, const TString& provider = {}, EDebugOutput debug = EDebugOutput::None) {
    return SqlToYqlWithMode(query, NSQLTranslation::ESqlMode::QUERY, maxErrors, provider, debug);
}

using TAstNodeVisitFunc = std::function<void(const NYql::TAstNode& root)>;

inline void VisitAstNodes(const NYql::TAstNode& root, const TAstNodeVisitFunc& visitFunc) {
    visitFunc(root);
    if (!root.IsList()) {
        return;
    }
    for (size_t childIdx = 0; childIdx < root.GetChildrenCount(); ++childIdx) {
        VisitAstNodes(*root.GetChild(childIdx), visitFunc);
    }
}

    
inline TMaybe<const NYql::TAstNode*> MaybeGetQuotedValue(const NYql::TAstNode& node) {
    const bool isQuotedList =
        node.IsListOfSize(2) && node.GetChild(0)->IsAtom() 
        && node.GetChild(0)->GetContent() == "quote";
    if (isQuotedList) {
        return node.GetChild(1);
    }
    return {};
}
