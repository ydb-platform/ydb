#pragma once

#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_node_serialization.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>
#include <ydb/library/yql/providers/common/mkql/yql_provider_mkql.h>

namespace NKikimr {

namespace Tests {

class TYqlServer : public TServer {
public:
    TYqlServer(const TServerSettings& settings)
        : TServer(settings, false)
    {
        Initialize();
    }

    TYqlServer(TServerSettings::TConstPtr settings)
        : TServer(settings, false)
    {
        Initialize();
    }

    TYqlServer& operator=(TYqlServer&& server) = default;

    void ResumeYqlExecutionActor();

protected:
    void Initialize();

    TAutoPtr<IThreadPool> YqlQueue;
    NThreading::TPromise<void> ResumeYqlExecutionPromise;
};

void MakeGatewaysConfig(const THashMap<TString, TString>& clusterMapping, NYql::TGatewaysConfig& gatewaysConfig);

}
}
