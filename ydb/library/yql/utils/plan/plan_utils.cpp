#include "plan_utils.h"

#include <yql/essentials/ast/yql_ast_escaping.h>

#include <util/charset/utf8.h>
#include <util/string/vector.h>

#include <regex>

namespace NYql::NPlanUtils {

using namespace NNodes;

TPredicate ExtractPredicate(const TCoLambda& expr) {
    TPredicate pred;
    pred.Args.reserve(expr.Args().Ref().ChildrenSize());
    for (const auto& child : expr.Args().Ref().Children()) {
        pred.Args.push_back(PrettyExprStr(TExprBase(child)));
    }

    pred.Body = PrettyExprStr(expr.Body());
    return pred;
}

TString ToStr(const TCoDataCtor& data) {
    TStringStream out;
    EscapeArbitraryAtom(data.Literal().Value(), '"', &out);
    return out.Str();
}

TString ToStr(const TCoPgConst& data) {
    TStringStream out;
    EscapeArbitraryAtom(data.Value().Value(), '"', &out);
    return out.Str();
}


TString ToStr(const TCoLambda& lambda) {
    if (lambda.Raw()->ChildrenSize() == 2) {
        return PrettyExprStr(lambda.Body());
    } else {
        TVector<TString> bodies;
        for (size_t i = 1; i < lambda.Raw()->ChildrenSize(); i++) {
            if (auto str = PrettyExprStr(TExprBase(lambda.Raw()->ChildPtr(i)))) {
                bodies.push_back(std::move(str));
            }
        }
        return TStringBuilder() << "(" << JoinStrings(std::move(bodies), ",") << ")";
    }
}

TString ToStr(const TCoAsStruct& asStruct) {
    TVector<TString> args;
    for (const auto& kv : asStruct.Args()) {
        auto key = PrettyExprStr(TExprBase(kv->Child(0)));
        auto value = PrettyExprStr(TExprBase(kv->Child(1)));

        if (!key.empty() && !value.empty()) {
            if (key.StartsWith("_yql_agg_")) args.push_back(value);
            else args.push_back(TStringBuilder() << key << ": " << value);
        }
    }

    return TStringBuilder() << "{" << JoinStrings(std::move(args), ",") << "}";
}

TString ToStr(const TCoAsList& asList) {
    TVector<TString> args;
    for (const auto& arg : asList.Args()) {
        if (auto str = PrettyExprStr(TExprBase(arg))) {
            args.push_back(std::move(str));
        }
    }

    return TStringBuilder() << "[" << JoinStrings(std::move(args), ",") << "]";
}

TString ToStr(const TCoList& list) {
    TVector<TString> args;
    for (const auto& arg : list.Args()) {
        if (auto str = PrettyExprStr(TExprBase(arg))) {
            args.push_back(std::move(str));
        }
    }

    return TStringBuilder() << "[" << JoinStrings(std::move(args), ",") << "]";
}

TString ToStr(const TCoMember& member) {
    auto structName = PrettyExprStr(member.Struct());
    auto memberName = PrettyExprStr(member.Name());

    if (!structName.empty() && !memberName.empty()) {
        return TStringBuilder() << structName << "." << memberName;
    }

    return {};
}

TString ToStr(const TCoNth& nth) {
    return TStringBuilder() << '#' << PrettyExprStr(nth.Index());
}

TString ToStr(const TCoIfPresent& ifPresent) {
    /* expected IfPresent with 3 children:
        * 0-Optional, 1-PresentHandler, 2-MissingValue */
    if (ifPresent.Ref().ChildrenSize() == 3) {
        auto arg = PrettyExprStr(ifPresent.Optional());
        auto pred = ExtractPredicate(ifPresent.PresentHandler());

        Y_ENSURE(!pred.Args.empty());
        return std::regex_replace(pred.Body.c_str(),
                std::regex(pred.Args[0].c_str()), arg.c_str()).data();
    }

    return "...";
}

TString ToStr(const TCoExists& exist) {
    if (auto str = PrettyExprStr(exist.Optional())) {
        return TStringBuilder() << "Exist(" << str << ")";
    }

    return {};
}

TString AggrOpToStr(const TExprBase& aggr) {
    TVector<TString> args;
    for (const auto& child : aggr.Ref().Children()) {
        if (auto str = PrettyExprStr(TExprBase(child))) {
            args.push_back(std::move(str));
        }
    }

    return TStringBuilder() << aggr.Ref().Content() << "("
            << JoinStrings(std::move(args), ",") << ")";
}

/* if lhs has lower priority than rhs */
bool IsLowerPriority(const TString& lhs, const TString& rhs) {
    const static THashMap<TString, i64> OP_PRIORITY = {
        {">", 0},
        {">=", 0},
        {"<", 0},
        {"<=", 0},
        {"==", 0},
        {"+", 1},
        {"-", 1},
        {"%", 2},
        {"*", 3},
        {"/", 3}
    };

    auto lhsIt = OP_PRIORITY.find(lhs);
    auto rhsIt = OP_PRIORITY.find(rhs);

    if (lhsIt == OP_PRIORITY.end() || rhsIt == OP_PRIORITY.end()) {
        return true;
    }

    return lhsIt->second < rhsIt->second;
}

TString BinaryOpToStr(const TExprBase& op) {
    TString curBinaryOp = ToString(op.Ref().Content());

    TString left;
    auto leftChild = TExprBase(op.Ref().Child(0));
    if (leftChild.Maybe<TCoBinaryArithmetic>()) {
        TString leftChildOp = ToString(leftChild.Ref().Content());

        if (IsLowerPriority(leftChildOp, curBinaryOp)) {
            left = "("  +  BinaryOpToStr(leftChild) + ")";
        } else {
            left = BinaryOpToStr(leftChild);
        }
    } else {
        left = PrettyExprStr(leftChild);
    }

    TString right;
    auto rightChild = TExprBase(op.Ref().Child(1));
    if (rightChild.Maybe<TCoBinaryArithmetic>()) {
        TString rightChildOp = ToString(rightChild.Ref().Content());

        if (IsLowerPriority(rightChildOp, curBinaryOp)) {
            right = "(" + BinaryOpToStr(rightChild) + ")";
        } else {
            right = BinaryOpToStr(rightChild);
        }
    } else {
        right = PrettyExprStr(rightChild);
    }

    TStringBuilder str;

    str << left;

    if (left && right) {
        str << " " << curBinaryOp << " ";
    }

    str << right;

    return str;
}

TString LogicOpToStr(const TExprBase& op) {
    TVector<TString> args;
    for (const auto& child : op.Ref().Children()) {
        if (auto str = PrettyExprStr(TExprBase(child))) {
            args.push_back(std::move(str));
        }
    }

    return JoinStrings(std::move(args), TStringBuilder() << " " << ToUpperUTF8(op.Ref().Content()) << " ");
}

TString NotToStr(const TCoNot& notOp) {
    return TStringBuilder() << "NOT " << PrettyExprStr(notOp.Value());
}

TString PrettyExprStr(const TExprBase& expr) {
    static const THashMap<TString, TString> aggregations = {
        {"AggrMin", "MIN"},
        {"AggrMax", "MAX"},
        {"AggrCountUpdate", "COUNT"},
        {"AggrAdd", "SUM"}
    };

    if (expr.Maybe<TCoIntegralCtor>()) {
        return TString(expr.Ref().Child(0)->Content());
    } else if (auto data = expr.Maybe<TCoDataCtor>()) {
        return ToStr(data.Cast());
    } else if (auto pgConst = expr.Maybe<TCoPgConst>()) {
        return ToStr(pgConst.Cast());
    } else if (auto lambda = expr.Maybe<TCoLambda>()) {
        return ToStr(lambda.Cast());
    } else if (auto asStruct = expr.Maybe<TCoAsStruct>()) {
        return ToStr(asStruct.Cast());
    } else if (auto asList = expr.Maybe<TCoAsList>()) {
        return ToStr(asList.Cast());
    } else if (auto list = expr.Maybe<TCoList>()) {
        return ToStr(list.Cast());
    } else if (auto member = expr.Maybe<TCoMember>()) {
        return ToStr(member.Cast());
    } else if (auto ifPresent = expr.Maybe<TCoIfPresent>()) {
        return ToStr(ifPresent.Cast());
    } else if (auto exist = expr.Maybe<TCoExists>()) {
        return ToStr(exist.Cast());
    } else if (expr.Maybe<TCoMin>() || expr.Maybe<TCoMax>() || expr.Maybe<TCoInc>()) {
        return AggrOpToStr(expr);
    } else if (aggregations.contains(expr.Ref().Content())) {
        TVector<TString> children;
        for (auto i = 0; i <= 1; i++) {
            auto str = PrettyExprStr(TExprBase(expr.Ref().Child(i)));
            if (str && !str.StartsWith("state._yql_agg_")) {
                children.push_back(std::move(str));
            }
        }
        return TStringBuilder() << aggregations.at(expr.Ref().Content()) << "(" << JoinStrings(std::move(children), ",") << ")";
    } else if (expr.Maybe<TCoBinaryArithmetic>() || expr.Maybe<TCoCompare>()) {
        return BinaryOpToStr(expr);
    } else if (expr.Maybe<TCoAnd>() || expr.Maybe<TCoOr>() || expr.Maybe<TCoXor>()) {
        return LogicOpToStr(expr);
    } else if (auto notOp = expr.Maybe<TCoNot>()) {
        return NotToStr(notOp.Cast());
    } else if (expr.Maybe<TCoParameter>() || expr.Maybe<TCoJust>() || expr.Maybe<TCoSafeCast>()
            || expr.Maybe<TCoCoalesce>() || expr.Maybe<TCoConvert>()) {
        return PrettyExprStr(TExprBase(expr.Ref().Child(0)));
    } else if (auto nth = expr.Maybe<TCoNth>()) {
        // return ToStr(nth.Cast());
        return "";
    // } else if (auto arg = expr.Maybe<TCoArgument>()) {
    //     return ""; // not argument deduction yet, so just skip them
    } else if (expr.Raw()->IsList()) {
        TVector<TString> items;
        for (const auto& item : expr.Raw()->ChildrenList()) {
            if (auto str = PrettyExprStr(TExprBase(item))) {
                if (!str.StartsWith("_yql_agg_")) {
                    items.push_back(std::move(str));
                }
            }
        }

        return TStringBuilder() << "[" << JoinStrings(std::move(items), ",") << "]";
    } else {
        if (auto arg = expr.Maybe<TCoArgument>()) {
            TStringBuf yqlOlapApplyMember = "members_";
            TString argumentName = TString(arg.Cast().Name());
            if (argumentName.StartsWith(yqlOlapApplyMember)) {
                return argumentName.substr(yqlOlapApplyMember.length(), argumentName.length() - yqlOlapApplyMember.length());
            }
        }
        auto raw = TString(expr.Ref().Content());
        // return raw.StartsWith("_yql_agg_") ? "" : raw;
        return raw;
    }

    return {};
}

} // namespace NYql::NPlanUtils
