#pragma once
#include "yql_yt_message_stream.h"
#include <yql/essentials/providers/common/transform/yql_visit.h>
#include <yql/essentials/providers/common/dq/yql_dq_integration_impl.h>
#include <ydb/library/yql/providers/abstract/message_stream/message_stream_client.h>
namespace NYql {
struct TYtMessageStreamState {
    struct TCluster {
        TString Endpoint;
        TString Token;
    };
    IStructuredTokenCredentialsFactory::TPtr Credentials;
    THashMap<TString, TCluster> Clusters;
    THashMap<TString, TString> Tokens;
    THashSet<TString> Names;
    THashMap<std::pair<TString, TString>, ui64> Partitions;
};


struct TYtMessageStreamReadSettings { TString Path; TString Consumer; };
TYtMessageStreamReadSettings ParseYtMessageStreamReadSettings(const TExprNode& read);
THolder<TVisitorTransformerBase> CreateYtMessageStreamTypeAnnotation();
THolder<IDqIntegration> CreateYtMessageStreamDqIntegration(std::shared_ptr<TYtMessageStreamState> state);
THolder<IGraphTransformer> CreateYtMessageStreamLoadMetadata(std::shared_ptr<TYtMessageStreamState> state);
std::shared_ptr<IYtMessageStreamIntegration> CreateYtMessageStreamIntegrationImpl(std::shared_ptr<TYtMessageStreamState> state);
}
