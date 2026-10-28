#pragma once
#include "yql_qyt_provider.h"

#include <ydb/library/yql/providers/abstract/message_stream/message_stream_client.h>

#include <yql/essentials/providers/common/dq/yql_dq_integration_impl.h>
#include <yql/essentials/providers/common/transform/yql_visit.h>
#include <yt/yt/client/table_client/schema.h>

namespace NYql {
struct TQytState {
    struct TCluster {
        TString Endpoint;
        TString Token;
    };

    IStructuredTokenCredentialsFactory::TPtr Credentials;
    THashMap<TString, TCluster> Clusters;
    THashMap<TString, TString> Tokens;
    THashSet<TString> Names;
    THashMap<std::pair<TString, TString>, ui64> Partitions;
    THashMap<std::pair<TString, TString>, NYT::NTableClient::TTableSchema> Schemas;
};

struct TQytReadSettings {
    TString Path;
    TString Consumer;
};

TQytReadSettings ParseQytReadSettings(const TExprNode& read);
THolder<TVisitorTransformerBase> CreateQytTypeAnnotation(std::shared_ptr<TQytState> state);
THolder<IDqIntegration> CreateQytDqIntegration(std::shared_ptr<TQytState> state);
THolder<IGraphTransformer> CreateQytLoadMetadata(std::shared_ptr<TQytState> state);
}   // namespace NYql
