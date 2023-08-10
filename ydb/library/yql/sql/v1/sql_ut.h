
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/sql/sql.h>
#include <util/generic/map.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/split.h>
#include <deque>
#include <unordered_set>
using namespace NSQLTranslation;

enum class EDebugOutput {
    None,
    ToCerr,
};

const ui32 PRETTY_FLAGS = NYql::TAstPrintFlags::PerLine | NYql::TAstPrintFlags::ShortQuote |
                          NYql::TAstPrintFlags::AdaptArbitraryContent;

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
    settings.MaxErrors = maxErrors;
    settings.Mode = mode;
    settings.Arena = &arena;
    settings.AnsiLexer = ansiLexer;
    settings.SyntaxVersion = 1;
    auto res = SqlToYql(query, settings);
    if (debug == EDebugOutput::ToCerr) {
        Err2Str(res, debug);
    }
    return res;
}

inline NYql::TAstParseResult SqlToYql(const TString& query, size_t maxErrors = 10, const TString& provider = {}, EDebugOutput debug = EDebugOutput::None) {
    return SqlToYqlWithMode(query, NSQLTranslation::ESqlMode::QUERY, maxErrors, provider, debug);
}

inline NYql::TAstParseResult SqlToYqlWithSettings(const TString& query, const NSQLTranslation::TTranslationSettings& settings) {
    return SqlToYqlWithMode(query, NSQLTranslation::ESqlMode::QUERY, 10, {}, EDebugOutput::None, false, settings);
}

inline void ExpectFailWithError(const TString& query, const TString& error) {
    NYql::TAstParseResult res = SqlToYql(query);

    UNIT_ASSERT(!res.Root);
    UNIT_ASSERT_NO_DIFF(Err2Str(res), error);
}

inline NYql::TAstParseResult SqlToYqlWithAnsiLexer(const TString& query, size_t maxErrors = 10, const TString& provider = {}, EDebugOutput debug = EDebugOutput::None) {
    bool ansiLexer = true;
    return SqlToYqlWithMode(query, NSQLTranslation::ESqlMode::QUERY, maxErrors, provider, debug, ansiLexer);
}

inline void ExpectFailWithErrorForAnsiLexer(const TString& query, const TString& error) {
    NYql::TAstParseResult res = SqlToYqlWithAnsiLexer(query);

    UNIT_ASSERT(!res.Root);
    UNIT_ASSERT_NO_DIFF(Err2Str(res), error);
}

inline TString GetPrettyPrint(const NYql::TAstParseResult& res) {
    TStringStream yqlProgram;
    res.Root->PrettyPrintTo(yqlProgram, NYql::TAstPrintFlags::PerLine | NYql::TAstPrintFlags::ShortQuote);
    return yqlProgram.Str();
}

inline TString Quote(const char* str) {
    return TStringBuilder() << "'\"" << str << "\"";
}

class TWordCountHive: public TMap<TString, unsigned> {
public:
    TWordCountHive(std::initializer_list<TString> strings) {
        for (auto& str: strings) {
            emplace(str, 0);
        }
    }

    TWordCountHive(std::initializer_list<std::pair<const TString, unsigned>> list)
        : TMap(list)
    {
    }
};

typedef std::function<void (const TString& word, const TString& line)> TVerifyLineFunc;

inline TString VerifyProgram(const NYql::TAstParseResult& res, TWordCountHive& wordCounter, TVerifyLineFunc verifyLine = TVerifyLineFunc()) {
    const auto programm = GetPrettyPrint(res);
    TVector<TString> yqlProgram;
    Split(programm, "\n", yqlProgram);
    for (const auto& line: yqlProgram) {
        for (auto& counterIter: wordCounter) {
            const auto& word = counterIter.first;
            auto pos = line.find(word);
            while (pos != TString::npos) {
                ++counterIter.second;
                if (verifyLine) {
                    verifyLine(word, line);
                }
                pos = line.find(word, pos + word.length());
            }
        }
    }
    return programm;
}

inline void VerifySqlInHints(const TString& query, const THashSet<TString>& expectedHints, TMaybe<bool> ansi) {
    TString pragma;
    if (ansi.Defined()) {
        pragma = *ansi ? "PRAGMA AnsiInForEmptyOrNullableItemsCollections;" :
                         "PRAGMA DisableAnsiInForEmptyOrNullableItemsCollections;";
    }

    NYql::TAstParseResult res = SqlToYql(pragma + query);
    UNIT_ASSERT(res.Root);

    TVerifyLineFunc verifyLine = [&](const TString& word, const TString& line) {
        Y_UNUSED(word);
        if (!ansi.Defined()) {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'('warnNoAnsi)"));
        } else if (*ansi) {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'('ansi)"));
        }
        for (auto& hint : expectedHints)  {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(hint));
        }
    };
    TWordCountHive elementStat = {{TString("SqlIn"), 0}};
    VerifyProgram(res, elementStat, verifyLine);
}

inline void VerifySqlInHints(const TString& query, const THashSet<TString>& expectedHints) {
    VerifySqlInHints(query, expectedHints, false);
    VerifySqlInHints(query, expectedHints, true);
}

inline NSQLTranslation::TTranslationSettings GetSettingsWithS3Binding(const TString& name) {
    NSQLTranslation::TTranslationSettings settings;
    NSQLTranslation::TTableBindingSettings bindSettings;
    bindSettings.ClusterType = "s3";
    bindSettings.Settings["cluster"] = "cluster";
    bindSettings.Settings["path"] = "path";
    bindSettings.Settings["format"] = "format";
    bindSettings.Settings["compression"] = "ccompression";
    bindSettings.Settings["bar"] = "1";
    // schema is not validated in this test but should be valid YSON text
    bindSettings.Settings["schema"] = R"__("[
                        "StructType";
                        [
                            [
                                "key";
                                [
                                    "DataType";
                                    "String"
                                ]
                            ];
                            [
                                "subkey";
                                [
                                    "DataType";
                                    "String"
                                ]
                            ];
                            [
                                "value";
                                [
                                    "DataType";
                                    "String"
                                ]
                            ]
    ]])__";
    bindSettings.Settings["partitioned_by"] = "[\"key\", \"subkey\"]";
    settings.Bindings[name] = bindSettings;
    return settings;
}

inline void AstBfs(NYql::TAstNode const* root, std::function<bool (NYql::TAstNode const*)> visitor) {
    std::deque<NYql::TAstNode const*> wishList{ root };
    std::unordered_set<NYql::TAstNode const*> visited;
    while(!wishList.empty()){
        auto v = wishList.front();
        wishList.pop_front();
        if (!visitor(v))
            return;
        visited.insert(v);
        if (v->IsList()) {
            for (ui32 i = 0; i != v->GetChildrenCount(); ++i) {
                auto child = v->GetChild(i);
                if (visited.find(child) == visited.cend()) {
                    wishList.push_back(child);
                }
            }
        }
    }
}

inline const NYql::TAstNode* FindNodeByChildAtomContent(const NYql::TAstNode* root, uint32_t childIndex, TStringBuf name){
    const NYql::TAstNode* result = nullptr;
    AstBfs(root, [&result, childIndex, name](auto v) {
        if (v->IsList() && v->GetChildrenCount() > childIndex &&
            v->GetChild(childIndex)->IsAtom() && v->GetChild(childIndex)->GetContent() == name) {
            result = v;
            return false;
        }
        return true; });
    return result;
}
