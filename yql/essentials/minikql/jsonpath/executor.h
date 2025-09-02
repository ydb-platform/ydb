#pragma once

#include <yql/essentials/minikql/jsonpath/parser/binary.h>
#include "value.h"

#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/utils/yql_panic.h>
#include <yql/essentials/public/udf/udf_value.h>
#include <yql/essentials/public/udf/udf_value_builder.h>
#include <yql/essentials/public/udf/udf_allocator.h>

#include <library/cpp/json/json_value.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>

#include <util/generic/ptr.h>
#include <util/generic/stack.h>
#include <util/generic/hash.h>
#include <util/generic/maybe.h>

#include <variant>

namespace NYql::NJsonPath {

using TJsonNodes = TSmallVec<TValue>;

class TResult {
public:
    TResult(TJsonNodes&& nodes);

    TResult(const TJsonNodes& nodes);

    TResult(TIssue&& issue);

    const TJsonNodes& GetNodes() const;

    TJsonNodes& GetNodes();

    const TIssue& GetError() const;

    bool IsError() const;

private:
    std::variant<TJsonNodes, TIssue> Result_;
};

class TArraySubscript {
public:
    TArraySubscript(i64 from, TPosition fromPos)
        : From_(from)
        , FromPos_(fromPos)
        , HasTo_(false)
    {
    }

    TArraySubscript(i64 from, TPosition fromPos, i64 to, TPosition toPos)
        : From_(from)
        , FromPos_(fromPos)
        , To_(to)
        , ToPos_(toPos)
        , HasTo_(true)
    {
    }

    i64 GetFrom() const {
        return From_;
    }

    TPosition GetFromPos() const {
        return FromPos_;
    }

    i64 GetTo() const {
        YQL_ENSURE(IsRange());
        return To_;
    }

    TPosition GetToPos() const {
        return ToPos_;
    }

    bool IsRange() const {
        return HasTo_;
    }

private:
    i64 From_ = 0;
    TPosition FromPos_;
    i64 To_ = 0;
    TPosition ToPos_;
    bool HasTo_;
};

using TVariablesMap = THashMap<TString, TValue>;

class TExecutor {
public:
    TExecutor(
        const TJsonPathPtr path,
        const TJsonNodes& input,
        const TVariablesMap& variables,
        const NUdf::IValueBuilder* valueBuilder);

    TResult Execute();

private:
    constexpr static double EPSILON = 1e-20;

    static bool IsZero(double value);

    static bool IsEqual(double a, double b);

    static bool IsLess(double a, double b);

    static bool IsGreater(double a, double b);

    bool IsStrict() const;

    bool IsLax() const;

    TResult Execute(const TJsonPathItem& item);

    TResult ContextObject();

    TResult Variable(const TJsonPathItem& item);

    TResult LastArrayIndex(const TJsonPathItem& item);

    TResult NumberLiteral(const TJsonPathItem& item);

    TResult MemberAccess(const TJsonPathItem& item);

    TResult WildcardMemberAccess(const TJsonPathItem& item);

    TMaybe<TIssue> EnsureSingleSubscript(TPosition pos, const TJsonNodes& index, i64& result);

    TMaybe<TIssue> EnsureArraySubscripts(const TJsonPathItem& item, TVector<TArraySubscript>& result);

    TResult ArrayAccess(const TJsonPathItem& item);

    TResult WildcardArrayAccess(const TJsonPathItem& item);

    TResult UnaryArithmeticOp(const TJsonPathItem& item);

    TMaybe<TIssue> EnsureBinaryArithmeticOpArgument(TPosition pos, const TJsonNodes& nodes, double& result);

    TResult BinaryArithmeticOp(const TJsonPathItem& item);

    TMaybe<TIssue> EnsureLogicalOpArgument(TPosition pos, const TJsonNodes& nodes, TMaybe<bool>& result);

    TResult BinaryLogicalOp(const TJsonPathItem& item);

    TResult UnaryLogicalOp(const TJsonPathItem& item);

    TResult BooleanLiteral(const TJsonPathItem& item);

    TResult NullLiteral();

    TResult StringLiteral(const TJsonPathItem& item);

    TMaybe<bool> CompareValues(const TValue& left, const TValue& right, EJsonPathItemType operation);

    TResult CompareOp(const TJsonPathItem& item);

    TResult FilterObject(const TJsonPathItem& item);

    TResult FilterPredicate(const TJsonPathItem& item);

    TResult NumericMethod(const TJsonPathItem& item);

    TResult DoubleMethod(const TJsonPathItem& item);

    TResult TypeMethod(const TJsonPathItem& item);

    TResult SizeMethod(const TJsonPathItem& item);

    TResult KeyValueMethod(const TJsonPathItem& item);

    TResult StartsWithPredicate(const TJsonPathItem& item);

    TResult IsUnknownPredicate(const TJsonPathItem& item);

    TResult ExistsPredicate(const TJsonPathItem& item);

    TResult LikeRegexPredicate(const TJsonPathItem& item);

    TJsonNodes OptionalUnwrapArrays(const TJsonNodes& input);

    TJsonNodes OptionalArrayWrapNodes(const TJsonNodes& input);

    TStack<TValue> ArraySubscriptSource_;
    TStack<TValue> CurrentFilterObject_;
    TJsonPathReader Reader_;
    TJsonNodes Input_;
    const TVariablesMap& Variables_;
    const NUdf::IValueBuilder* ValueBuilder_;
};

}
