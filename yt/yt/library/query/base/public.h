#pragma once

#include <yt/yt/client/query_client/public.h>

#include <yt/yt/client/transaction_client/public.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/ypath/public.h>

namespace NYT::NQueryClient {

using NTransactionClient::TTimestamp;

using NTableClient::TRowRange;

using TReadSessionId = TGuid;

struct TDataSplit;

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TColumnDescriptor;
class TExpression;
class TGroupClause;
class TProjectClause;
class TWhenThenExpression;
class TJoinClause;
class TQuery;
class TQueryOptions;
class TDataSource;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TExpression)
using TConstExpressionPtr = TIntrusivePtr<const TExpression>;

DECLARE_REFCOUNTED_STRUCT(TFunctionExpression)
using TConstFunctionExpressionPtr = TIntrusivePtr<const TFunctionExpression>;

DECLARE_REFCOUNTED_STRUCT(TAggregateFunctionExpression)
using TConstAggregateFunctionExpressionPtr = TIntrusivePtr<const TAggregateFunctionExpression>;

DECLARE_REFCOUNTED_STRUCT(TArrayJoinClause)
using TConstArrayJoinClausePtr = TIntrusivePtr<const TArrayJoinClause>;

DECLARE_REFCOUNTED_STRUCT(TJoinClause)
using TConstJoinClausePtr = TIntrusivePtr<const TJoinClause>;

DECLARE_REFCOUNTED_STRUCT(TGroupClause)
using TConstGroupClausePtr = TIntrusivePtr<const TGroupClause>;

DECLARE_REFCOUNTED_STRUCT(TOrderClause)
using TConstOrderClausePtr = TIntrusivePtr<const TOrderClause>;

DECLARE_REFCOUNTED_STRUCT(TProjectClause)
using TConstProjectClausePtr = TIntrusivePtr<const TProjectClause>;

DECLARE_REFCOUNTED_STRUCT(TWhenThenExpression)
using TConstWhenThenExpressionPtr = TIntrusivePtr<const TWhenThenExpression>;

DECLARE_REFCOUNTED_STRUCT(TBaseQuery)
using TConstBaseQueryPtr = TIntrusivePtr<const TBaseQuery>;

DECLARE_REFCOUNTED_STRUCT(TFrontQuery)
using TConstFrontQueryPtr = TIntrusivePtr<const TFrontQuery>;

DECLARE_REFCOUNTED_STRUCT(TQuery)
using TConstQueryPtr = TIntrusivePtr<const TQuery>;

struct IPrepareCallbacks;

struct TQueryStatistics;

struct TQueryOptions;

DECLARE_REFCOUNTED_STRUCT(IAggregateFunctionDescriptor)

DECLARE_REFCOUNTED_STRUCT(ICallingConvention)

DECLARE_REFCOUNTED_STRUCT(IExecutor)

DECLARE_REFCOUNTED_STRUCT(IEvaluator)

DECLARE_REFCOUNTED_CLASS(TExecutorConfig)

DECLARE_REFCOUNTED_STRUCT(IColumnEvaluatorCache)
DECLARE_REFCOUNTED_CLASS(TColumnEvaluator)
DECLARE_REFCOUNTED_CLASS(TColumnEvaluatorCacheConfig)
DECLARE_REFCOUNTED_CLASS(TColumnEvaluatorCacheDynamicConfig)

DECLARE_REFCOUNTED_STRUCT(IExpressionEvaluatorCache)
DECLARE_REFCOUNTED_CLASS(TExpressionEvaluator)
DECLARE_REFCOUNTED_CLASS(TExpressionEvaluatorCacheConfig)

DECLARE_REFCOUNTED_STRUCT(TExternalCGInfo)
using TConstExternalCGInfoPtr = TIntrusivePtr<const TExternalCGInfo>;

DECLARE_REFCOUNTED_STRUCT(TTypeInferrerMap)
using TConstTypeInferrerMapPtr = TIntrusivePtr<const TTypeInferrerMap>;

const TConstTypeInferrerMapPtr GetBuiltinTypeInferrers();

DECLARE_REFCOUNTED_STRUCT(IFunctionRegistry)
DECLARE_REFCOUNTED_STRUCT(ITypeInferrer)

DECLARE_REFCOUNTED_CLASS(TFunctionImplCache)

using NTableClient::ISchemafulUnversionedReader;
using NTableClient::ISchemafulUnversionedReaderPtr;
using NTableClient::ISchemalessUnversionedReader;
using NTableClient::ISchemalessUnversionedReaderPtr;
using NTableClient::IUnversionedRowsetWriter;
using NTableClient::IUnversionedRowsetWriterPtr;
using NTableClient::EValueType;
using NTableClient::TTableSchema;
using NTableClient::TTableSchemaPtr;
using NTableClient::TColumnSchema;
using NTableClient::TKeyColumns;
using NTableClient::TColumnFilter;
using NTableClient::TRowRange;

using NTransactionClient::TTimestamp;
using NTransactionClient::NullTimestamp;

using NTableClient::TRowBuffer;
using NTableClient::TRowBufferPtr;

using TSchemaColumns = std::vector<NTableClient::TColumnSchema>;

using TRow = NTableClient::TUnversionedRow;
using TMutableRow = NTableClient::TMutableUnversionedRow;
using TRowHeader = NTableClient::TUnversionedRowHeader;
using TRowBuilder = NTableClient::TUnversionedRowBuilder;
using TOwningRow = NTableClient::TUnversionedOwningRow;
using TOwningRowBuilder = NTableClient::TUnversionedOwningRowBuilder;
using TValue = NTableClient::TUnversionedValue;
using TValueData = NTableClient::TUnversionedValueData;
using TOwningValue = NTableClient::TUnversionedOwningValue;
using TLegacyOwningKey = NTableClient::TLegacyOwningKey;

using TKeyRange = std::pair<TLegacyOwningKey, TLegacyOwningKey>;
using TMutableRowRange = std::pair<TMutableRow, TMutableRow>;
using TRowRanges = std::vector<TRowRange>;
using TMutableRowRanges = std::vector<TMutableRowRange>;

using TColumnSet = THashSet<TString>;

////////////////////////////////////////////////////////////////////////////////

extern const NYPath::TYPath QueryPoolsPath;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

