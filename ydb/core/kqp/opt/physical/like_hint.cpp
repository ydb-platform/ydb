#include "like_hint.h"

#include <yql/essentials/core/yql_opt_utils.h>

#include <array>

namespace NKikimr::NKqp::NOpt {

namespace {

const NYql::TExprNode* UnwrapQuote(const NYql::TExprNode* node) {
    while (node && node->IsCallable("quote") && node->ChildrenSize() == 1) {
        node = node->Child(0);
    }
    return node;
}

const NYql::TExprNode* SkipJust(const NYql::TExprNode* node) {
    while (node && node->IsCallable("Just") && node->ChildrenSize() >= 1) {
        node = SkipJust(UnwrapQuote(node->Child(0)));
    }
    return node;
}

bool IsFalseBool(const NYql::TExprNode& node) {
    return node.IsCallable("Bool") && node.ChildrenSize() >= 1 && node.Head().Content() == "false";
}

bool IsRe2Udf(const NYql::TExprNode& node, const std::string_view name) {
    const auto& udf = NYql::SkipCallables(node, {"AssumeStrict"});
    if (!udf.IsCallable("Udf") || udf.ChildrenSize() < 1) {
        return false;
    }
    const auto content = udf.Head().Content();
    if (content == name) {
        return true;
    }
    // A type suffix is "(...)", not another UDF whose name merely starts with this one.
    return content.StartsWith(name) && content.size() > name.size() && content[name.size()] == '(';
}

std::optional<bool> ReadBool(const NYql::TExprNode* node) {
    node = SkipJust(UnwrapQuote(node));
    if (!node || !node->IsCallable("Bool") || node->ChildrenSize() < 1 || !node->Head().IsAtom()) {
        return std::nullopt;
    }
    if (node->Head().Content() == "true") {
        return true;
    }
    if (node->Head().Content() == "false") {
        return false;
    }
    return std::nullopt;
}

// SQL LIKE/ILIKE sets only CaseSensitive. Any other Re2 option can change what a match means
// (Literal=true matches the pattern text), so the LIKE implication no longer holds.
bool IsRe2OptionName(const std::string_view name) {
    return name == "CaseSensitive" || name == "DotNl" || name == "Literal" || name == "LogErrors" ||
        name == "LongestMatch" || name == "MaxMem" || name == "NeverCapture" || name == "NeverNl" ||
        name == "OneLine" || name == "PerlClasses" || name == "PosixSyntax" || name == "Utf8" ||
        name == "WordBoundary";
}

struct TOptionScan {
    std::optional<bool> CaseSensitive;
    bool Unsupported = false;
};

// Re2::Options argument order. CaseSensitive is the only one SQL LIKE/ILIKE sets.
constexpr std::array<std::string_view, 13> Re2OptionOrder = {
    "Utf8", "PosixSyntax", "LongestMatch", "LogErrors", "MaxMem", "Literal", "NeverNl", "DotNl",
    "NeverCapture", "CaseSensitive", "PerlClasses", "WordBoundary", "OneLine",
};

// A type node (DataType, CallableType, and the rest) is annotated as Type. Re2::Options type
// structs mention every option name; those mentions are not settings. No annotation is not a type.
bool IsTypeExpr(const NYql::TExprNode* node) {
    node = UnwrapQuote(node);
    if (!node || !node->GetTypeAnn()) {
        return false;
    }
    return node->GetTypeAnn()->GetKind() == NYql::ETypeAnnotationKind::Type;
}

// Nodes that may appear in a fully known Re2::Options value. If / Parameter / any other callable
// is an unknown branch: one side can be Literal=true while the other only shows CaseSensitive.
bool IsOptionsStructure(const NYql::TExprNode* node) {
    return node->IsCallable({
        "AsStruct", "AsTuple", "List", "Tuple", "Just", "Bool", "Uint64", "Int64", "Ui64", "Nothing", "Null", "Optional",
    }) || IsTypeExpr(node);
}

bool IsAbsentOption(const NYql::TExprNode* node) {
    node = SkipJust(UnwrapQuote(node));
    return !node || node->IsCallable({"Nothing", "Null"});
}

void AcceptCaseSensitive(const NYql::TExprNode* valueNode, TOptionScan& scan) {
    const auto value = ReadBool(valueNode);
    if (!value || (scan.CaseSensitive && *scan.CaseSensitive != *value)) {
        scan.Unsupported = true;
        return;
    }
    scan.CaseSensitive = *value;
}

void AcceptSetting(const std::string_view name, const NYql::TExprNode* valueNode, TOptionScan& scan) {
    if (name == "CaseSensitive") {
        AcceptCaseSensitive(valueNode, scan);
        return;
    }
    scan.Unsupported = true;
}

void ScanPositionalOptions(const NYql::TExprNode& positional, TOptionScan& scan) {
    ui32 index = 0;
    for (const auto& arg : positional.Children()) {
        if (index >= Re2OptionOrder.size()) {
            scan.Unsupported = true;
            return;
        }
        if (!IsAbsentOption(arg.Get())) {
            AcceptSetting(Re2OptionOrder[index], arg.Get(), scan);
            if (scan.Unsupported) {
                return;
            }
        }
        ++index;
    }
}

void ScanOptions(const NYql::TExprNode& node, TOptionScan& scan) {
    const auto* current = UnwrapQuote(&node);
    if (!current || scan.Unsupported) {
        return;
    }
    if (current->IsCallable("NamedApply") && current->ChildrenSize() >= 2 && IsRe2Udf(current->Head(), "Re2.Options")) {
        // The positional arguments must be a list or tuple node; any other node could hide a setting.
        const auto* positional = UnwrapQuote(current->Child(1));
        if (!positional || !(positional->IsList() || positional->IsCallable({"AsTuple", "Tuple"}))) {
            scan.Unsupported = true;
            return;
        }
        ScanPositionalOptions(*positional, scan);
        if (scan.Unsupported) {
            return;
        }
        if (current->ChildrenSize() >= 3) {
            ScanOptions(*current->Child(2), scan);
        }
        return;
    }
    if (current->IsCallable() && !IsOptionsStructure(current) &&
        !(current->IsCallable("Apply") && current->ChildrenSize() >= 1 && IsRe2Udf(current->Head(), "Re2.Options"))) {
        const auto* head = current->ChildrenSize() >= 1 ? UnwrapQuote(&current->Head()) : nullptr;
        const bool namedSetting = head && head->IsAtom() && IsRe2OptionName(head->Content());
        if (!namedSetting) {
            scan.Unsupported = true;
            return;
        }
    }
    if (current->IsCallable("Apply") && current->ChildrenSize() >= 1 && IsRe2Udf(current->Head(), "Re2.Options")) {
        for (ui32 index = 1; index < current->ChildrenSize(); ++index) {
            if (index - 1 >= Re2OptionOrder.size()) {
                scan.Unsupported = true;
                return;
            }
            if (!IsAbsentOption(current->Child(index))) {
                AcceptSetting(Re2OptionOrder[index - 1], current->Child(index), scan);
                if (scan.Unsupported) {
                    return;
                }
            }
        }
        return;
    }
    if (current->ChildrenSize() >= 2) {
        const auto* name = UnwrapQuote(current->Child(0));
        const auto* valueNode = UnwrapQuote(current->Child(1));
        // Type nodes mention every option name. Anything else bound to that name is a setting,
        // including a parameter: Re2::Options($x AS Literal) must not keep the hint.
        if (name && name->IsAtom() && valueNode && !IsTypeExpr(valueNode) && IsRe2OptionName(name->Content())) {
            AcceptSetting(name->Content(), valueNode, scan);
            return;
        }
    }
    for (const auto& child : current->Children()) {
        ScanOptions(*child, scan);
        if (scan.Unsupported) {
            return;
        }
    }
}

bool ExtractLiteral(const NYql::TExprNode& node, TString& value) {
    const auto* literal = SkipJust(UnwrapQuote(&node));
    if (!literal || !literal->IsCallable({"String", "Utf8"}) || literal->ChildrenSize() < 1 || !literal->Head().IsAtom()) {
        return false;
    }
    value = TString(literal->Head().Content());
    return true;
}

bool IsStringOrUtf8(const NYql::TTypeAnnotationNode* type) {
    if (!type) {
        return false;
    }
    if (type->GetKind() == NYql::ETypeAnnotationKind::Optional) {
        type = type->Cast<NYql::TOptionalExprType>()->GetItemType();
    }
    if (type->GetKind() != NYql::ETypeAnnotationKind::Data) {
        return false;
    }
    const auto slot = type->Cast<NYql::TDataExprType>()->GetSlot();
    return slot == NYql::EDataSlot::String || slot == NYql::EDataSlot::Utf8;
}

// An optional column is matched through Map/FlatMap (UDF AutoMap). The column is that lambda
// input, or Unwrap(Member) when the match is applied directly. SafeCast is accepted only from
// String and Utf8; a cast from another type must not receive a string bloom probe.
const NYql::TExprNode* MemberColumn(const NYql::TExprNode* node) {
    node = SkipJust(UnwrapQuote(node));
    while (node && node->IsCallable({"Unwrap", "SafeCast"})) {
        if (node->IsCallable("SafeCast") && !IsStringOrUtf8(node->Head().GetTypeAnn())) {
            return nullptr;
        }
        node = SkipJust(UnwrapQuote(node->Child(0)));
    }
    if (node && node->IsCallable("Member")) {
        return node;
    }
    return nullptr;
}

struct TLocatedMatch {
    const NYql::TExprNode* Apply = nullptr;
    const NYql::TExprNode* Column = nullptr;
};

// The arguments of a Re2.PatternFromLike call: the pattern and an optional escape. SQL LIKE produces
// Apply(udf, pattern [, escape]). A hand-written Udf(Re2::PatternFromLike)(...) produces
// NamedApply(udf, '(pattern [, escape]), AsStruct()). Named arguments are not a shape we know.
std::optional<std::vector<const NYql::TExprNode*>> PatternFromLikeArgs(const NYql::TExprNode* node) {
    node = UnwrapQuote(node);
    if (!node || node->ChildrenSize() < 2 || !IsRe2Udf(node->Head(), "Re2.PatternFromLike")) {
        return std::nullopt;
    }
    std::vector<const NYql::TExprNode*> args;
    if (node->IsCallable("Apply")) {
        for (ui32 index = 1; index < node->ChildrenSize(); ++index) {
            args.push_back(node->Child(index));
        }
    } else if (node->IsCallable("NamedApply")) {
        const auto* positional = UnwrapQuote(node->Child(1));
        if (!positional || !(positional->IsList() || positional->IsCallable({"AsTuple", "Tuple"}))) {
            return std::nullopt;
        }
        if (node->ChildrenSize() >= 3) {
            const auto* named = UnwrapQuote(node->Child(2));
            if (!named || !named->IsCallable("AsStruct") || named->ChildrenSize() != 0) {
                return std::nullopt;
            }
        }
        for (const auto& child : positional->Children()) {
            args.push_back(child.Get());
        }
    } else {
        return std::nullopt;
    }
    if (args.empty() || args.size() > 2) {
        return std::nullopt;
    }
    return args;
}

std::optional<TLocatedMatch> LocateMatch(const NYql::TExprNode* node, const NYql::TExprNode* boundArg, const NYql::TExprNode* boundColumn) {
    node = UnwrapQuote(node);
    if (!node) {
        return std::nullopt;
    }
    // Only the two-argument form: Coalesce(x, false) is true exactly when x is true. With more
    // arguments a true result may come from a later child while x is NULL.
    if (node->IsCallable("Coalesce") && node->ChildrenSize() == 2 && IsFalseBool(node->Tail())) {
        return LocateMatch(node->Child(0), boundArg, boundColumn);
    }
    if (node->IsCallable("Just")) {
        return LocateMatch(&node->Head(), boundArg, boundColumn);
    }
    if (node->IsCallable({"Map", "FlatMap"}) && node->ChildrenSize() >= 2 && node->Tail().IsLambda()) {
        const auto& lambda = node->Tail();
        if (lambda.Head().ChildrenSize() != 1) {
            return std::nullopt;
        }
        const auto* inputColumn = MemberColumn(node->Child(0));
        if (!inputColumn) {
            return std::nullopt;
        }
        return LocateMatch(&lambda.Tail(), &lambda.Head().Head(), inputColumn);
    }
    if (!node->IsCallable("Apply") || node->ChildrenSize() != 2 || !IsRe2Udf(node->Head(), "Re2.Match")) {
        return std::nullopt;
    }
    const NYql::TExprNode* column = (boundArg && &node->Tail() == boundArg) ? boundColumn : MemberColumn(&node->Tail());
    if (!column) {
        return std::nullopt;
    }
    return TLocatedMatch{node, column};
}

}   // namespace

std::optional<TLikeHint> TryParseLikeHint(const NYql::TExprNode& predicate) {
    const auto located = LocateMatch(&predicate, nullptr, nullptr);
    if (!located) {
        return std::nullopt;
    }

    const auto& udf = NYql::SkipCallables(located->Apply->Head(), {"AssumeStrict"});
    if (udf.ChildrenSize() < 2) {
        return std::nullopt;
    }
    const auto* runConfig = UnwrapQuote(udf.Child(1));
    if (!runConfig || runConfig->ChildrenSize() < 2) {
        return std::nullopt;
    }
    const auto patternArgs = PatternFromLikeArgs(runConfig->Child(0));
    if (!patternArgs) {
        return std::nullopt;
    }

    TLikeHint hint;
    hint.Column = located->Column;
    if (!ExtractLiteral(*patternArgs->front(), hint.Pattern)) {
        return std::nullopt;
    }
    if (patternArgs->size() == 2) {
        if (!ExtractLiteral(*patternArgs->back(), hint.Escape) || hint.Escape.size() != 1) {
            return std::nullopt;
        }
    }
    TOptionScan options;
    ScanOptions(*runConfig->Child(1), options);
    if (options.Unsupported || !options.CaseSensitive) {
        return std::nullopt;
    }
    hint.CaseSensitive = *options.CaseSensitive;
    return hint;
}

}   // namespace NKikimr::NKqp::NOpt
