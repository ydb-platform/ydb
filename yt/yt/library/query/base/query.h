#pragma once

#include "public.h"
#include "query_common.h"

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/misc/guid.h>

#include <library/cpp/yt/memory/range.h>

#include <library/cpp/yt/misc/property.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

constexpr i64 MaxQueryLimit = std::numeric_limits<i64>::max() - 2;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EScanOrder,
    ((Unordered) (0))
    ((Ordered)   (1))
    ((Reversed)  (2))
);

DEFINE_ENUM(EExpressionKind,
    ((None)                    (0))
    ((Literal)                 (1))
    ((Reference)               (2))
    ((Function)                (3))
    ((UnaryOp)                 (4))
    ((BinaryOp)                (5))
    ((In)                      (6))
    ((Transform)               (7))
    ((Between)                 (8))
    ((Case)                    (9))
    ((Like)                    (10))
    ((CompositeMemberAccessor) (11))
    ((Subquery)                (12))
);

struct TExpression
    : public TRefCounted
{
    TLogicalTypePtr LogicalType;
    TLogicalTypePtr OriginalLogicalType;

    explicit TExpression(EValueType type);
    explicit TExpression(TLogicalTypePtr type);
    TExpression(TLogicalTypePtr type, TLogicalTypePtr originalType);

    EValueType GetWireType() const;

    template <class TDerived>
    const TDerived* As() const
    {
        return dynamic_cast<const TDerived*>(this);
    }

    template <class TDerived>
    TDerived* As()
    {
        return dynamic_cast<TDerived*>(this);
    }
};

DEFINE_REFCOUNTED_TYPE(TExpression)

struct TLiteralExpression
    : public TExpression
{
    TOwningValue Value;

    using TExpression::TExpression;

    TLiteralExpression(EValueType type, TOwningValue value);
};

struct TReferenceExpression
    : public TExpression
{
    std::string ColumnName;

    explicit TReferenceExpression(const TLogicalTypePtr& type);
    TReferenceExpression(const TLogicalTypePtr& type, const std::string& columnName);
};

struct TFunctionExpression
    : public TExpression
{
    std::string FunctionName;
    std::vector<TConstExpressionPtr> Arguments;

    using TExpression::TExpression;

    TFunctionExpression(
        EValueType type,
        const std::string& functionName,
        std::vector<TConstExpressionPtr> arguments);

    TFunctionExpression(
        TLogicalTypePtr type,
        const std::string& functionName,
        std::vector<TConstExpressionPtr> arguments);
};

DEFINE_REFCOUNTED_TYPE(TFunctionExpression)

struct TUnaryOpExpression
    : public TExpression
{
    EUnaryOp Opcode;
    TConstExpressionPtr Operand;

    using TExpression::TExpression;

    TUnaryOpExpression(
        EValueType type,
        EUnaryOp opcode,
        TConstExpressionPtr operand);
};

struct TBinaryOpExpression
    : public TExpression
{
    EBinaryOp Opcode;
    TConstExpressionPtr Lhs;
    TConstExpressionPtr Rhs;

    using TExpression::TExpression;

    TBinaryOpExpression(
        EValueType type,
        EBinaryOp opcode,
        TConstExpressionPtr lhs,
        TConstExpressionPtr rhs);
};

struct TInExpression
    : public TExpression
{
    std::vector<TConstExpressionPtr> Arguments;
    TSharedRange<TRow> Values;

    explicit TInExpression(EValueType type);

    TInExpression(
        std::vector<TConstExpressionPtr> arguments,
        TSharedRange<TRow> values);
};

struct TBetweenExpression
    : public TExpression
{
    std::vector<TConstExpressionPtr> Arguments;
    TSharedRange<TRowRange> Ranges;

    explicit TBetweenExpression(EValueType type);

    TBetweenExpression(
        std::vector<TConstExpressionPtr> arguments,
        TSharedRange<TRowRange> ranges);
};

struct TTransformExpression
    : public TExpression
{
    std::vector<TConstExpressionPtr> Arguments;
    TSharedRange<TRow> Values;
    TConstExpressionPtr DefaultExpression;

    explicit TTransformExpression(EValueType type);

    TTransformExpression(
        EValueType type,
        std::vector<TConstExpressionPtr> arguments,
        TSharedRange<TRow> values,
        TConstExpressionPtr defaultExpression);
};

struct TWhenThenExpression
    : public TRefCounted
{
    TConstExpressionPtr Condition;
    TConstExpressionPtr Result;

    TWhenThenExpression() = default;

    TWhenThenExpression(TConstExpressionPtr condition, TConstExpressionPtr result);
};

DEFINE_REFCOUNTED_TYPE(TWhenThenExpression)

struct TCaseExpression
    : public TExpression
{
    TConstExpressionPtr OptionalOperand;
    std::vector<TWhenThenExpressionPtr> WhenThenExpressions;
    TConstExpressionPtr DefaultExpression;

    explicit TCaseExpression(EValueType type);

    TCaseExpression(
        EValueType type,
        TConstExpressionPtr optionalOperand,
        std::vector<TWhenThenExpressionPtr> whenThenExpressions,
        TConstExpressionPtr defaultExpression);
};

struct TLikeExpression
    : public TExpression
{
    TConstExpressionPtr Text;
    EStringMatchOp Opcode;
    TConstExpressionPtr Pattern;
    TConstExpressionPtr EscapeCharacter;

    explicit TLikeExpression(EValueType type);

    TLikeExpression(
        TConstExpressionPtr text,
        EStringMatchOp opcode,
        TConstExpressionPtr pattern,
        TConstExpressionPtr escapeCharacter);
};

struct TCompositeMemberAccessorPath
{
    std::vector<NTableClient::ELogicalMetatype> NestedTypes;
    std::vector<TStructMemberAccessor> NamedStructMembers;
    std::vector<int> PositionalStructMembers;
    std::vector<TTupleItemIndexAccessor> TupleItemIndices;

    void AppendStructMember(TStructMemberAccessor name, int position);
    void AppendTupleItem(TTupleItemIndexAccessor index);
    void Reserve(int length);

    bool operator==(const TCompositeMemberAccessorPath& other) const = default;
};

struct TCompositeMemberAccessorExpression
    : public TExpression
{
    using TDictOrListItemAccessorExpression = TConstExpressionPtr;

    TConstExpressionPtr CompositeExpression;
    TCompositeMemberAccessorPath NestedStructOrTupleItemAccessor;
    TDictOrListItemAccessorExpression DictOrListItemAccessor;

    explicit TCompositeMemberAccessorExpression(TLogicalTypePtr type);

    TCompositeMemberAccessorExpression(
        TLogicalTypePtr type,
        TConstExpressionPtr compositeExpression,
        TCompositeMemberAccessorPath nestedStructOrTupleItemAccess,
        TDictOrListItemAccessorExpression dictOrListItemAccess);
};

////////////////////////////////////////////////////////////////////////////////

struct TNamedItem
{
    TConstExpressionPtr Expression;
    std::string Name;

    TNamedItem() = default;
    TNamedItem(TConstExpressionPtr expression, const std::string& name);
};

using TNamedItemList = std::vector<TNamedItem>;

////////////////////////////////////////////////////////////////////////////////

struct TSubqueryExpression
    : public TExpression
{
    TNamedItemList FromExpressions;

    std::vector<TConstJoinClausePtr> JoinClauses;

    TConstExpressionPtr WhereClause;
    TConstGroupClausePtr GroupClause;
    TConstProjectClausePtr ProjectClause;
    TConstOrderClausePtr OrderClause;

    i64 Offset = 0;
    std::optional<i64> Limit;

    using TExpression::TExpression;
};

DEFINE_REFCOUNTED_TYPE(TSubqueryExpression)

////////////////////////////////////////////////////////////////////////////////

struct TAggregateItem
{
    std::vector<TConstExpressionPtr> Arguments;
    std::string Name;
    std::string AggregateFunction;
    TLogicalTypePtr StateType;
    TLogicalTypePtr ResultType;

    TAggregateItem() = default;

    TAggregateItem(
        std::vector<TConstExpressionPtr> arguments,
        const std::string& aggregateFunction,
        const std::string& name,
        TLogicalTypePtr stateType,
        TLogicalTypePtr resultType);
};

using TAggregateItemList = std::vector<TAggregateItem>;

struct TGroupClause
    : public TRefCounted
{
    TNamedItemList GroupItems;
    TAggregateItemList AggregateItems;
    ETotalsMode TotalsMode = ETotalsMode::None;
    size_t CommonPrefixWithPrimaryKey = 0;

    void AddGroupItem(TNamedItem namedItem);

    void AddGroupItem(TConstExpressionPtr expression, const std::string& name);

    TTableSchemaPtr GetTableSchema(bool isFinal) const;

    bool AllAggregatesAreFirst() const;

    TGroupClause() = default;
    TGroupClause(const TGroupClause& other);
};

DEFINE_REFCOUNTED_TYPE(TGroupClause)

////////////////////////////////////////////////////////////////////////////////

struct TColumnDescriptor
{
    // Renamed column.
    // TODO(sabdenovch): Do not keep name but restore name from table alias and column name from original schema.
    std::string Name;
    // Index in schema.
    int Index;
};

struct TMappedSchema
{
    TTableSchemaPtr Original;
    std::vector<TColumnDescriptor> Mapping;

    std::vector<TColumnDescriptor> GetOrderedSchemaMapping() const;

    TKeyColumns GetKeyColumns() const;

    TTableSchemaPtr GetRenamedSchema() const;
};

struct TJoinClause
    : public TRefCounted
{
    TMappedSchema Schema;
    THashSet<std::string> SelfJoinedColumns;
    THashSet<std::string> ForeignJoinedColumns;

    TConstExpressionPtr Predicate;

    std::vector<TConstExpressionPtr> ForeignEquations;
    std::vector<TConstExpressionPtr> SelfEquations;

    size_t CommonKeyPrefix = 0;
    size_t ForeignKeyPrefix = 0;

    // TODO(sabdenovch): introduce TArrayJoinClause and TTableJoinClause.
    // Currently non-empty ArrayExpressions renders fields *KeyPrefix, Foreign*Id and *Equations meaningless.
    std::vector<TConstExpressionPtr> ArrayExpressions;

    bool IsLeft = false;

    std::optional<std::pair<int, int>> PrefetchedBlockRange;

    //! See #TDataSource::ObjectId.
    NObjectClient::TObjectId ForeignObjectId;
    //! See #TDataSource::CellId.
    NObjectClient::TCellId ForeignCellId;

    TConstGroupClausePtr GroupClause;

    bool RequireSyncReplica = true;

    TJoinClause() = default;

    TJoinClause(const TJoinClause& other);

    TTableSchemaPtr GetRenamedSchema() const;

    TKeyColumns GetKeyColumns() const;

    TTableSchemaPtr GetTableSchema(const TTableSchema& source) const;

    std::vector<size_t> GetForeignColumnIndices() const;

    TQueryPtr GetJoinSubquery() const;
};

DEFINE_REFCOUNTED_TYPE(TJoinClause)

struct THierarchicalJoinClause
    : public TRefCounted
{
    TMappedSchema ForeignTableSchema;

    std::vector<TConstExpressionPtr> ForeignEquations;
    std::vector<TConstExpressionPtr> SelfEquations;

    bool IsLeft = false;

    //! See #TDataSource::ObjectId.
    NObjectClient::TObjectId ForeignObjectId;
    //! See #TDataSource::CellId.
    NObjectClient::TCellId ForeignCellId;

    THashSet<std::string> ForeignJoinedColumns;

    std::string ResultColumnName;

    TConstSubqueryExpressionPtr SelfSideJoinKeys;
    TConstSubqueryExpressionPtr JoiningSubquery;

    THierarchicalJoinClause() = default;
    THierarchicalJoinClause(const THierarchicalJoinClause& other);

    TTableSchemaPtr GetForeignTableRenamedSchema() const;

    TKeyColumns GetForeignTableKeyColumns() const;

    TTableSchemaPtr GetTableSchema(const TTableSchema& source) const;

    std::vector<size_t> GetForeignColumnIndices() const;

    TQueryPtr GetJoinSubquery() const;
};

DEFINE_REFCOUNTED_TYPE(THierarchicalJoinClause)

std::vector<size_t> GetJoinGroups(
    const std::vector<TConstJoinClausePtr>& joinClauses,
    TTableSchemaPtr schema);

////////////////////////////////////////////////////////////////////////////////

struct TOrderItem
{
    TConstExpressionPtr Expression;
    bool Descending;
};

struct TOrderClause
    : public TRefCounted
{
    std::vector<TOrderItem> OrderItems;
};

DEFINE_REFCOUNTED_TYPE(TOrderClause)

////////////////////////////////////////////////////////////////////////////////

struct TProjectClause
    : public TRefCounted
{
    TNamedItemList Projections;

    void AddProjection(TNamedItem namedItem);

    void AddProjection(TConstExpressionPtr expression, const std::string& name);

    TTableSchemaPtr GetTableSchema(bool castToQLType = true) const;
};

DEFINE_REFCOUNTED_TYPE(TProjectClause)

////////////////////////////////////////////////////////////////////////////////

// Front Query is not Coordinatable
// IsMerge is always true for front Query and false for Bottom Query

struct TBaseQuery
    : public TRefCounted
{
    TGuid Id;

    bool IsFinal = true;
    bool HasExclusiveGroupKeyView = false;
    bool EnableCombineGroupOpWithOrderOp = true;

    TConstGroupClausePtr GroupClause;
    TConstExpressionPtr HavingClause;
    TConstOrderClausePtr OrderClause;

    TConstProjectClausePtr ProjectClause;

    i64 Offset = 0;

    std::optional<i64> Limit;

    EScanOrder ScanOrder = EScanOrder::Ordered;

    // True if the grouping key uses each column of primary key.
    // In this case, some additional optimizations can be applied.
    bool UseDisjointGroupBy = false;

    bool InferRanges = true;

    bool ForceLightRangeInference = false;

    bool Prefetching = false;

    explicit TBaseQuery(TGuid id = TGuid::Create());

    TBaseQuery(const TBaseQuery& other);

    virtual TTableSchemaPtr GetReadSchema() const = 0;
    virtual TTableSchemaPtr GetTableSchema(bool castToQLType = true) const = 0;
};

DEFINE_REFCOUNTED_TYPE(TBaseQuery)

EScanOrder DeriveLegacyScanOrder(const TConstBaseQueryPtr& query, bool allowUnorderedGroupByWithLimit);

struct TQuery
    : public TBaseQuery
{
    TMappedSchema Schema;

    // Bottom
    std::vector<TConstJoinClausePtr> JoinClauses;
    std::vector<TConstHierarchicalJoinClausePtr> HierarchicalJoinsInWhereClause;
    std::vector<TConstHierarchicalJoinClausePtr> HierarchicalJoinsBeforeGroupBy;
    std::vector<TConstHierarchicalJoinClausePtr> HierarchicalJoinsAfterGroupBy;
    TConstExpressionPtr WhereClause;

    explicit TQuery(TGuid id = TGuid::Create());

    TQuery(const TQuery& other) = default;

    TKeyColumns GetKeyColumns() const;

    TTableSchemaPtr GetReadSchema() const override;

    TTableSchemaPtr GetRenamedSchema() const;

    TTableSchemaPtr GetTableSchema(bool castToQLType = true) const override;
};

DEFINE_REFCOUNTED_TYPE(TQuery)

struct TFrontQuery
    : public TBaseQuery
{
    explicit TFrontQuery(TGuid id = TGuid::Create());

    TFrontQuery(const TFrontQuery& other) = default;

    TTableSchemaPtr Schema;

    std::vector<TConstHierarchicalJoinClausePtr> HierarchicalJoinsAfterGroupBy;

    TTableSchemaPtr GetReadSchema() const override;

    TTableSchemaPtr GetRenamedSchema() const;

    TTableSchemaPtr GetTableSchema(bool castToQLType = true) const override;
};

DEFINE_REFCOUNTED_TYPE(TFrontQuery)

////////////////////////////////////////////////////////////////////////////////

struct TUnionAllQuery
    : public TRefCounted
{
    std::vector<TPlanFragmentPtr> SubqueryFragments;
    TTableSchemaPtr Schema;
};

DEFINE_REFCOUNTED_TYPE(TUnionAllQuery)

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TQuery* serialized, const TConstQueryPtr& original);
void FromProto(TConstQueryPtr* original, const NProto::TQuery& serialized);

////////////////////////////////////////////////////////////////////////////////

struct TInferNameOptions
{
    bool OmitValues = false;
    bool OmitAliases = false;
    bool OmitOffsetAndLimit = false;
    TConstGroupClausePtr GroupClause;
};

std::string InferName(TConstExpressionPtr expr, const TInferNameOptions& options = {});
std::string InferName(TConstBaseQueryPtr query, const TInferNameOptions& options = {});

////////////////////////////////////////////////////////////////////////////////

class TSchemaAwareReferenceComparer
{
public:
    TSchemaAwareReferenceComparer(const TTableSchema& lhsSchema, const TTableSchema& rhsSchema, int maxIndex);

    bool operator()(const std::string& lhs, const std::string& rhs);

private:
    const TTableSchema& LhsSchema_;
    const TTableSchema& RhsSchema_;
    const int MaxIndex_;
};

struct TReferenceComparer
{
    bool operator()(const std::string& lhs, const std::string& rhs);
};

bool Compare(
    TConstExpressionPtr lhs,
    TConstExpressionPtr rhs,
    std::function<bool(const std::string&, const std::string&)> comparer = TReferenceComparer());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
