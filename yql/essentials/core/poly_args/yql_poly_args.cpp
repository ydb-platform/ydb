#include "yql_poly_args.h"

#include <util/string/builder.h>
#include <util/generic/overloaded.h>

#include <variant>

namespace NYql {

namespace {

#define CHECK_CONFIG(CONDITION, ...)                                                                                                                                                              \
    do {                                                                                                                                                                                          \
        static_assert(!std::is_array_v<std::remove_cvref_t<decltype(CONDITION)>>, "An array type always evaluates to true in a condition; this is likely an error in the condition expression."); \
        if (Y_UNLIKELY(!(CONDITION))) {                                                                                                                                                           \
            throw TConfigException() << #CONDITION << " " __VA_ARGS__;                                                                                                                            \
        }                                                                                                                                                                                         \
    } while (0)

#define CHECK_CONFIG_FAIL(...) \
    throw TConfigException() << " " __VA_ARGS__;

constexpr TStringBuf PredicateCmdAttribute = "cmd";
constexpr TStringBuf VerCmd = "ver";
constexpr TStringBuf TypeCmd = "type";
constexpr TStringBuf KindCmd = "kind";
constexpr TStringBuf OrCmd = "or";

constexpr TStringBuf ArgAttribute = "arg";
constexpr TStringBuf ValueAttribute = "value";

constexpr TStringBuf VerAction = "ver";
constexpr TStringBuf ArgsAction = "args";
constexpr TStringBuf TypeAction = "type";
constexpr TStringBuf RunConfigAction = "runConfig";

class TPolyArgs: public IPolyArgs {
public:
    explicit TPolyArgs(const NYT::TNode& config)
        : Parsed_(Parse(config))
    {
    }

    ui32 GetPredicatesCount() const final {
        return Parsed_.size();
    }

    TMaybe<TUnresolvedInput> GetUnresolvedInput(ui32 index) const final {
        Y_ENSURE(index < Parsed_.size());
        const auto& action = Parsed_[index].second;
        if (action.Type) {
            return Nothing();
        }

        return TUnresolvedInput{
            .UserTypeArgs = action.Args,
            .LangVer = action.LangVer.GetOrElse(MinLangVersion)};
    }

    TMatchResult Match(const TArgs& args, TLangVersion version) const final {
        for (ui32 i = 0; i < Parsed_.size(); ++i) {
            TMatchResult ret;
            if (MatchPredicate(Parsed_[i].first, args, version)) {
                ret.Index = i;
                ret.CallableType = Parsed_[i].second.Type;
                ret.RunConfigType = Parsed_[i].second.RunConfig;
                return ret;
            }
        }
        Y_ENSURE(false, "Unreachable");
    }

private:
    struct TPredicate;
    using TPredicatePtr = std::shared_ptr<TPredicate>;

    struct TAndPredicate {
        TVector<TPredicatePtr> Children;
    };

    struct TOrPredicate {
        TVector<TPredicatePtr> Children;
    };

    struct TTypePredicate {
        TString Arg;
        NYT::TNode Value;
    };

    struct TKindPredicate {
        TString Arg;
        TString Value;
    };

    struct TVerPredicate {
        TLangVersion LangVer = MinLangVersion;
    };

    struct TPredicate {
        std::variant<TTypePredicate, TKindPredicate, TVerPredicate, TAndPredicate, TOrPredicate> Value;
    };

    struct TAction {
        TMaybe<TLangVersion> LangVer;
        TMaybe<TVector<NYT::TNode>> Args;
        TMaybe<NYT::TNode> Type;
        TMaybe<NYT::TNode> RunConfig;
    };

    using TParsedData = TVector<std::pair<TPredicate, TAction>>;
    const TParsedData Parsed_;

    static TParsedData Parse(const NYT::TNode& config) {
        TParsedData ret;
        CHECK_CONFIG(config.IsList());
        CHECK_CONFIG(!config.AsList().empty());
        for (const auto& x : config.AsList()) {
            CHECK_CONFIG(x.IsList() && x.AsList().size() == 2);
            ret.emplace_back(ParsePredicate(x.AsList()[0]), ParseAction(x.AsList()[1]));
        }

        const auto& lastPredicate = config.AsList().back().AsList()[0];
        CHECK_CONFIG(lastPredicate.IsList() && lastPredicate.AsList().empty());
        return ret;
    }

    static TPredicate ParsePredicate(const NYT::TNode& predicate) {
        if (predicate.IsList()) {
            return ParseAndPredicate(predicate);
        }

        CHECK_CONFIG(predicate.IsMap());
        const auto& map = predicate.AsMap();
        auto it = map.find(PredicateCmdAttribute);
        CHECK_CONFIG(it != map.end());
        CHECK_CONFIG(it->second.IsString());
        if (it->second.AsString() == TypeCmd) {
            return ParseTypePredicate(predicate);
        }

        if (it->second.AsString() == KindCmd) {
            return ParseKindPredicate(predicate);
        }

        if (it->second.AsString() == VerCmd) {
            return ParseVerPredicate(predicate);
        }

        if (it->second.AsString() == OrCmd) {
            return ParseOrPredicate(predicate);
        }

        CHECK_CONFIG_FAIL("Unsupported cmd: " << it->second.AsString());
    }

    static TPredicate ParseAndPredicate(const NYT::TNode& predicate) {
        TAndPredicate ret;
        for (const auto& x : predicate.AsList()) {
            ret.Children.push_back(std::make_shared<TPredicate>(ParsePredicate(x)));
        }

        return TPredicate{.Value = ret};
    }

    static TPredicate ParseOrPredicate(const NYT::TNode& predicate) {
        TOrPredicate ret;
        const auto& map = predicate.AsMap();
        auto itValue = map.find(ValueAttribute);
        CHECK_CONFIG(itValue != map.end());
        CHECK_CONFIG(itValue->second.IsList());
        for (const auto& x : itValue->second.AsList()) {
            ret.Children.push_back(std::make_shared<TPredicate>(ParsePredicate(x)));
        }

        return TPredicate{.Value = ret};
    }

    static TPredicate ParseTypePredicate(const NYT::TNode& predicate) {
        TTypePredicate ret;
        const auto& map = predicate.AsMap();
        auto itArg = map.find(ArgAttribute);
        CHECK_CONFIG(itArg != map.end());
        CHECK_CONFIG(itArg->second.IsString());
        auto valueArg = map.find(ValueAttribute);
        CHECK_CONFIG(valueArg != map.end());
        ret.Arg = itArg->second.AsString();
        ret.Value = valueArg->second;
        return TPredicate{
            .Value = ret};
    }

    static TPredicate ParseKindPredicate(const NYT::TNode& predicate) {
        TKindPredicate ret;
        const auto& map = predicate.AsMap();
        auto itArg = map.find(ArgAttribute);
        CHECK_CONFIG(itArg != map.end());
        CHECK_CONFIG(itArg->second.IsString());
        auto valueArg = map.find(ValueAttribute);
        CHECK_CONFIG(valueArg != map.end());
        ret.Arg = itArg->second.AsString();
        CHECK_CONFIG(valueArg->second.IsString());
        ret.Value = valueArg->second.AsString() + "Type";
        return TPredicate{
            .Value = ret};
    }

    static TPredicate ParseVerPredicate(const NYT::TNode& predicate) {
        TVerPredicate ret;
        const auto& map = predicate.AsMap();
        auto itArg = map.find(ValueAttribute);
        CHECK_CONFIG(itArg != map.end());
        CHECK_CONFIG(itArg->second.IsString());
        CHECK_CONFIG(ParseLangVersion(itArg->second.AsString(), ret.LangVer));
        return TPredicate{
            .Value = ret};
    }

    static TAction ParseAction(const NYT::TNode& action) {
        TAction ret;
        CHECK_CONFIG(action.IsMap());
        const auto& map = action.AsMap();
        auto verIt = map.find(VerAction);
        bool hasUnresolved = false;
        if (verIt != map.end()) {
            ret.LangVer = ParseVer(verIt->second);
            hasUnresolved = true;
        }

        auto argsIt = map.find(ArgsAction);
        if (argsIt != map.end()) {
            ret.Args = ParseArgs(argsIt->second);
            hasUnresolved = true;
        }

        auto typeIt = map.find(TypeAction);
        if (typeIt != map.end()) {
            ret.Type = typeIt->second;
        }

        auto runConfigIt = map.find(RunConfigAction);
        if (runConfigIt != map.end()) {
            CHECK_CONFIG(ret.Type.Defined(), "runConfig requires type action");
            ret.RunConfig = runConfigIt->second;
        }

        CHECK_CONFIG(!ret.Type.Defined() || !hasUnresolved);
        return ret;
    }

    static TLangVersion ParseVer(const NYT::TNode& value) {
        TLangVersion ret;
        CHECK_CONFIG(value.IsString());
        CHECK_CONFIG(ParseLangVersion(value.AsString(), ret));
        return ret;
    }

    static TVector<NYT::TNode> ParseArgs(const NYT::TNode& value) {
        CHECK_CONFIG(value.IsList());
        return value.AsList();
    }

    static bool MatchTypePredicate(const TTypePredicate& predicate, const TArgs& args) {
        auto it = args.find(predicate.Arg);
        if (it == args.end()) {
            return false;
        }

        return it->second == predicate.Value;
    }

    static bool MatchKindPredicate(const TKindPredicate& predicate, const TArgs& args) {
        auto it = args.find(predicate.Arg);
        if (it == args.end()) {
            return false;
        }

        CHECK_CONFIG(it->second.IsList());
        CHECK_CONFIG(it->second.AsList()[0].IsString());
        const auto& str = it->second.AsList()[0].AsString();
        return str == predicate.Value;
    }

    static bool MatchVerPredicate(const TVerPredicate& predicate, TLangVersion version) {
        return version >= predicate.LangVer;
    }

    static bool MatchAndPredicate(const TAndPredicate& predicate, const TArgs& args, TLangVersion version) {
        for (const auto& child : predicate.Children) {
            if (!MatchPredicate(*child, args, version)) {
                return false;
            }
        }

        return true;
    }

    static bool MatchOrPredicate(const TOrPredicate& predicate, const TArgs& args, TLangVersion version) {
        for (const auto& child : predicate.Children) {
            if (MatchPredicate(*child, args, version)) {
                return true;
            }
        }

        return false;
    }

    static bool MatchPredicate(const TPredicate& predicate, const TArgs& args, TLangVersion version) {
        return std::visit(
            TOverloaded{
                [&](const TTypePredicate& p) { return MatchTypePredicate(p, args); },
                [&](const TKindPredicate& p) { return MatchKindPredicate(p, args); },
                [&](const TVerPredicate& p) { return MatchVerPredicate(p, version); },
                [&](const TAndPredicate& p) { return MatchAndPredicate(p, args, version); },
                [&](const TOrPredicate& p) { return MatchOrPredicate(p, args, version); }},
            predicate.Value);
    }
};

} // namespace

std::unique_ptr<IPolyArgs> ParsePolyArgs(const NYT::TNode& config) {
    return std::make_unique<TPolyArgs>(config);
}

} // namespace NYql
