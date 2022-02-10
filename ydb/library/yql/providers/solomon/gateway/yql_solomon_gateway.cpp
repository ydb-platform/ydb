#include "yql_solomon_gateway.h"

#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>

namespace NYql {

using namespace NThreading;

class TSolomonGateway : public ISolomonGateway {
public:
    TSolomonGateway(const TSolomonGatewayConfig& config)
    {
        for (const auto& item: config.GetClusterMapping()) {
            Clusters_[item.GetName()] = item;
        }
    }

    NThreading::TFuture<TGetMetaResult> GetMeta(const TGetMetaRequest& request) const override {
        Y_UNUSED(request);
        try {
            TGetMetaResult result;
            auto meta = MakeIntrusive<TSolomonMetadata>();
            result.Meta = meta;

            // TODO: actual meta
            meta->DoesExist = true;

            result.SetSuccess();
            return MakeFuture(result);

        } catch (const yexception& e) {
            return MakeFuture(NCommon::ResultFromException<TGetMetaResult>(e));

        } catch (...) {
            return MakeFuture(NCommon::ResultFromError<TGetMetaResult>(CurrentExceptionMessage()));
        }
    }

    TMaybe<TSolomonClusterConfig> GetClusterConfig(const TStringBuf cluster) const override {
        if (Clusters_.contains(cluster)) {
            return Clusters_.find(cluster)->second;
        }
        return Nothing();
    }

    bool HasCluster(const TStringBuf cluster) const override {
        return Clusters_.contains(cluster);
    }

private:
    THashMap<TString, TSolomonClusterConfig> Clusters_;
};

ISolomonGateway::TPtr CreateSolomonGateway(const TSolomonGatewayConfig& config) {
    return new TSolomonGateway(config);
}

} // namespace NYql
