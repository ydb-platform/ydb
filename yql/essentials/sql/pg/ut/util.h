#include <contrib/libs/fmt/include/fmt/format.h>

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/sql/sql.h>
#include <yql/essentials/parser/pg_catalog/catalog.h>
#include <yql/essentials/parser/pg_wrapper/interface/config.h>
#include <yql/essentials/parser/pg_wrapper/interface/parser.h>

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

class TTestAutoParamBuilder : public NYql::IAutoParamBuilder {
public:
    TString GetParamValue(const TString& param) const {
        auto ptr = State.FindPtr(param);
        Y_ENSURE(ptr);
        TStringBuilder res;
        if (ListParams.contains(param)) {
            res << '[';
            bool first = true;
            for (const auto& t : ptr->first) {
                if (!first) {
                    res << ',';
                } else {
                    first = false;
                }

                res << t;
            }

            res << ':';
            first = true;
            for (const auto& v : ptr->second) {
                if (!first) {
                    res << ',';
                } else {
                    first = false;
                }

                res << v.GetOrElse("#");
            }

            res << ']';
        } else {
            res << ptr->first.front();
            res << ':';
            res << ptr->second.front().GetOrElse("#");
        }

        return res;
    }

    ui32 Size() const final {
        return State.size();
    }

    bool Contains(const TString& name) const final {
        return State.contains(name);
    }

    NYql::IAutoParamTypeBuilder& Add(const TString& name) final {
        CurrentParam = name;
        return Type;
    }

    class TTypeProxy : public NYql::IAutoParamTypeBuilder {
    public:
        TTypeProxy(TTestAutoParamBuilder& owner)
            : Owner(owner)
        {}

        void Pg(const TString& name) {
            Owner.State[Owner.CurrentParam].first.push_back(name);
        }

        void BeginList() final {
            Owner.ListParams.insert(Owner.CurrentParam);
        }

        void EndList() final {
        }

        void BeginTuple() final {
        }

        void EndTuple() final {
        }

        void BeforeItem() final {
        }

        void AfterItem() final {
        }

        NYql::IAutoParamDataBuilder& FinishType() final {
            return Owner.Data;
        }

        TTestAutoParamBuilder& Owner;
    };

    class TDataProxy : public NYql::IAutoParamDataBuilder {
    public:
        TDataProxy(TTestAutoParamBuilder& owner)
            : Owner(owner)
        {}

        void Pg(const TMaybe<TString>& value) final {
            Owner.State[Owner.CurrentParam].second.push_back(value);
        }

        void BeginList() final {
        }

        void EndList() final {
        }

        void BeginTuple() final {
        }

        void EndTuple() final {
        }

        void BeforeItem() final {
        }

        void AfterItem() final {
        }

        NYql::IAutoParamBuilder& FinishData() final {
            return Owner;
        }

        TTestAutoParamBuilder& Owner;
    };

    TMap<TString, std::pair<TVector<TString>, TVector<TMaybe<TString>>>> State;
    THashSet<TString> ListParams;
    TTypeProxy Type;
    TDataProxy Data;
    TString CurrentParam;

    TTestAutoParamBuilder()
        : Type(*this)
        , Data(*this)
    {}
};

class TTestAutoParamBuilderFactory : public NYql::IAutoParamBuilderFactory {
public:
    NYql::IAutoParamBuilderPtr MakeBuilder() final {
        return MakeIntrusive<TTestAutoParamBuilder>();
    }
};

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
    TTestAutoParamBuilderFactory autoParamFactory;
    settings.AutoParamBuilderFactory = &autoParamFactory;

    NSQLTranslation::TTranslators translators(
        nullptr,
        nullptr,
        NSQLTranslationPG::MakeTranslator()
    );

    auto res = SqlToYql(translators, query, settings);
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
