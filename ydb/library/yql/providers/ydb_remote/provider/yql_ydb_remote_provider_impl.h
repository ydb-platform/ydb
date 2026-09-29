#pragma once

#include "yql_ydb_remote_provider.h"

#include <ydb/public/api/protos/ydb_value.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/providers/common/transform/yql_visit.h>

namespace NYql::NYdbRemote {

struct TCluster {
    TString Endpoint;
    TString Database;
    bool UseTls = false;
};

struct TTable {
    // Retained for as long as the compile-time schema remains cached.
    std::shared_ptr<void> MetadataLease;
    const TStructExprType* RowType = nullptr;
    TVector<TString> ColumnOrder;
    THashMap<TString, Ydb::Type> ColumnTypes;
};

inline constexpr ui64 MaxMetadataTables = 64;
inline constexpr ui64 MaxMetadataColumns = 1024;
inline constexpr ui64 MaxMetadataSchemaBytes = 64 * 1024;
// Covers the compact primitive schema, its map/order copies and type annotations.
// The bounded SDK decoder and its temporary objects use a separate reservation.
inline constexpr ui64 MetadataSchemaReservation = 1024 * 1024;
inline constexpr ui64 MetadataResponseReservation = 64 * 1024 * 1024;

struct TMetadataSchema {
    std::shared_ptr<void> MemoryLease;
    TVector<std::pair<TString, Ydb::Type>> Columns;
};

bool ExtractMetadataSchema(const Ydb::Table::DescribeTableResult& description,
                           TMetadataSchema& schema, TString& error);

struct TState : public TThrRefBase {
    using TPtr = TIntrusivePtr<TState>;
    using TTableKey = std::pair<TString, TString>;

    TState(TTypeAnnotationContext* types, const NYdb::TDriver& driver,
           IStructuredTokenCredentialsFactory::TPtr credentialsFactory,
           TInstant metadataDeadline = TInstant::Max(),
           std::shared_ptr<NNative::IAsyncMemoryQuota> metadataQuota = {})
        : Types(types)
        , Driver(driver)
        , CredentialsFactory(std::move(credentialsFactory))
        , MetadataDeadline(metadataDeadline == TInstant::Max() ? TInstant::Now() + TDuration::Seconds(60) : metadataDeadline)
        , MetadataQuota(std::move(metadataQuota))
    {
    }

    TTypeAnnotationContext* const Types;
    const NYdb::TDriver Driver;
    const IStructuredTokenCredentialsFactory::TPtr CredentialsFactory;
    const TInstant MetadataDeadline;
    const std::shared_ptr<NNative::IAsyncMemoryQuota> MetadataQuota;
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
THolder<IDqIntegration> CreateDqIntegration(TState::TPtr state);

} // namespace NYql::NYdbRemote
