#pragma once

#include "yql_ydb_provider.h"

#include <ydb/public/api/protos/ydb_value.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/providers/common/transform/yql_visit.h>

namespace NYql::NYdb {

struct TCluster {
    TString Endpoint;
    TString Database;
    bool UseTls = false;
};

struct TTable {
    const TStructExprType* RowType = nullptr;
    TVector<TString> ColumnOrder;
    THashMap<TString, Ydb::Type> ColumnTypes;
};

inline constexpr ui64 MaxMetadataTables = 64;
inline constexpr ui64 MaxMetadataColumns = 1024;
inline constexpr ui64 MaxMetadataSchemaBytes = 64 * 1024;

struct TMetadataSchema {
    TVector<std::pair<TString, Ydb::Type>> Columns;
};

bool ExtractMetadataSchema(const Ydb::Table::DescribeTableResult& description,
                           TMetadataSchema& schema, TString& error);

struct TState : public TThrRefBase {
    using TPtr = TIntrusivePtr<TState>;
    using TTableKey = std::pair<TString, TString>;

    TState(TTypeAnnotationContext* types, TYdbMetadataClientCacheFactory metadataClientCacheFactory,
           IStructuredTokenCredentialsFactory::TPtr credentialsFactory,
           TInstant metadataDeadline = TInstant::Max())
        : Types(types)
        , CredentialsFactory(std::move(credentialsFactory))
        , MetadataDeadline(metadataDeadline == TInstant::Max() ? TInstant::Now() + TDuration::Seconds(60) : metadataDeadline)
        , MetadataClientCacheFactory(std::move(metadataClientCacheFactory))
    {
    }

    TTypeAnnotationContext* const Types;
    const IStructuredTokenCredentialsFactory::TPtr CredentialsFactory;
    // One local budget for the whole metadata batch, including all tables.
    const TInstant MetadataDeadline;
    const TYdbMetadataClientCacheFactory MetadataClientCacheFactory;
    THashMap<TString, TCluster> Clusters;
    THashMap<TString, TString> Tokens;
    THashSet<TString> ValidClusters;
    THashMap<TTableKey, TTable> Tables;
};

void AddCluster(TState& state, const TString& name, const THashMap<TString, TString>& properties);
const TTypeAnnotationNode* ParseColumnType(const Ydb::Type& type, TExprContext& ctx);
THolder<IGraphTransformer> CreateLoadMetadataTransformer(TState::TPtr state);
THolder<TVisitorTransformerBase> CreateTypeAnnotationTransformer(TState::TPtr state);
THolder<IGraphTransformer> CreatePhysicalOptimizer();
THolder<IGraphTransformer> CreateLogicalOptimizer(TState::TPtr state);
THolder<IDqIntegration> CreateDqIntegration(TState::TPtr state);

} // namespace NYql::NYdb
