#pragma once

#include "binary.h"
#include "value.h"

#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/public/udf/udf_value_builder.h>
#include <ydb/library/yql/public/udf/udf_allocator.h>

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
    std::variant<TJsonNodes, TIssue> Result;
};

class TArraySubscript {
public:
    TArraySubscript(i64 from, TPosition fromPos)
        : From(from)
        , FromPos(fromPos)
        , HasTo(false)
    {
    }

    TArraySubscript(i64 from, TPosition fromPos, i64 to, TPosition toPos)
        : From(from)
        , FromPos(fromPos)
        , To(to)
        , ToPos(toPos)
        , HasTo(true)
    {
    }

    i64 GetFrom() const {
        return From;
    }

    TPosition GetFromPos() const {
        return FromPos;
    }

    i64 GetTo() const {
        YQL_ENSURE(IsRange());
        return To;
    }

    TPosition GetToPos() const {
        return ToPos;
    }

    bool IsRange() const {
        return HasTo;
    }

private:
    i64 From = 0;
    TPosition FromPos;
    i64 To = 0;
    TPosition ToPos;
    bool HasTo;
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

    TStack<TValue> ArraySubscriptSource;
    TStack<TValue> CurrentFilterObject;
    TJsonPathReader Reader;
    TJsonNodes Input;
    const TVariablesMap& Variables;
    const NUdf::IValueBuilder* ValueBuilder;
};

}
