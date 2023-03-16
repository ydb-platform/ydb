#include "system_clusters.h"

#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <util/generic/hash.h>

namespace NFq {

using namespace NYql;

void AddSystemClusters(TGatewaysConfig& gatewaysConfig, THashMap<TString, TString>& clusters, const TString& authToken) {
    {
        const auto clusterCfg = gatewaysConfig.MutablePq()->AddClusterMapping();
        clusterCfg->SetName("logbroker");
        clusterCfg->SetEndpoint("logbroker.yandex.net:2135");
        clusterCfg->SetConfigManagerEndpoint("cm.logbroker.yandex.net:1111");
        clusterCfg->SetClusterType(TPqClusterConfig::CT_PERS_QUEUE);
        clusterCfg->SetToken(authToken);
        clusters.emplace(clusterCfg->GetName(), PqProviderName);
    }
    {
        const auto clusterCfg = gatewaysConfig.MutablePq()->AddClusterMapping();
        clusterCfg->SetName("logbroker_iam_no_sa");
        clusterCfg->SetEndpoint("logbroker.yandex.net:2135");
        clusterCfg->SetConfigManagerEndpoint("cm.logbroker.yandex.net:1111");
        clusterCfg->SetAddBearerToToken(true);
        clusterCfg->SetClusterType(TPqClusterConfig::CT_PERS_QUEUE);
        clusterCfg->SetToken(authToken);
        clusters.emplace(clusterCfg->GetName(), PqProviderName);
    }
    {
        const auto clusterCfg = gatewaysConfig.MutablePq()->AddClusterMapping();
        clusterCfg->SetName("lbkx");
        clusterCfg->SetEndpoint("lbkx.logbroker.yandex.net:2135");
        clusterCfg->SetConfigManagerEndpoint("cm.lbkx.logbroker.yandex.net:1111");
        clusterCfg->SetClusterType(TPqClusterConfig::CT_PERS_QUEUE);
        clusterCfg->SetToken(authToken);
        clusters.emplace(clusterCfg->GetName(), PqProviderName);
    }
    {
        const auto clusterCfg = gatewaysConfig.MutablePq()->AddClusterMapping();
        clusterCfg->SetName("logbroker_prestable");
        clusterCfg->SetEndpoint("logbroker-prestable.yandex.net:2135");
        clusterCfg->SetConfigManagerEndpoint("cm.logbroker-prestable.yandex.net:1111");
        clusterCfg->SetClusterType(TPqClusterConfig::CT_PERS_QUEUE);
        clusterCfg->SetToken(authToken);
        clusters.emplace(clusterCfg->GetName(), PqProviderName);
    }
    {
        const auto clusterCfg = gatewaysConfig.MutablePq()->AddClusterMapping();
        clusterCfg->SetName("logbroker_prestable_iam");
        clusterCfg->SetEndpoint("logbroker-prestable.yandex.net:2135");
        clusterCfg->SetConfigManagerEndpoint("cm.logbroker-prestable.yandex.net:1111");
        clusterCfg->SetServiceAccountId("f6ooj9og003v61fjb821");
        clusterCfg->SetServiceAccountIdSignature("26YmpsY8SkqCNirlRduj8bqt4JQ=");
        clusterCfg->SetAddBearerToToken(true);
        clusterCfg->SetClusterType(TPqClusterConfig::CT_PERS_QUEUE);
        clusters.emplace(clusterCfg->GetName(), PqProviderName);
    }
    {
        const auto clusterCfg = gatewaysConfig.MutablePq()->AddClusterMapping();
        clusterCfg->SetName("logbroker_prestable_iam_no_sa");
        clusterCfg->SetEndpoint("logbroker-prestable.yandex.net:2135");
        clusterCfg->SetConfigManagerEndpoint("cm.logbroker-prestable.yandex.net:1111");
        clusterCfg->SetAddBearerToToken(true);
        clusterCfg->SetClusterType(TPqClusterConfig::CT_PERS_QUEUE);
        clusterCfg->SetToken(authToken);
        clusters.emplace(clusterCfg->GetName(), PqProviderName);
    }
    {
        const auto clusterCfg = gatewaysConfig.MutablePq()->AddClusterMapping();
        clusterCfg->SetName("lbkxt");
        clusterCfg->SetEndpoint("lbkxt.logbroker.yandex.net:2135");
        clusterCfg->SetConfigManagerEndpoint("cm.lbkxt.logbroker.yandex.net:1111");
        clusterCfg->SetClusterType(TPqClusterConfig::CT_PERS_QUEUE);
        clusterCfg->SetToken(authToken);
        clusters.emplace(clusterCfg->GetName(), PqProviderName);
    }
    {
        const auto clusterCfg = gatewaysConfig.MutablePq()->AddClusterMapping();
        clusterCfg->SetName("datastreams_preprod");
        clusterCfg->SetEndpoint("lb.cc802lcdcldbsh7b5fd1.ydb.mdb.cloud-preprod.yandex.net:2135");
        clusterCfg->SetClusterType(TPqClusterConfig::CT_DATA_STREAMS);
        clusterCfg->SetToken(authToken);
        clusters.emplace(clusterCfg->GetName(), PqProviderName);
    }
    {
        const auto clusterCfg = gatewaysConfig.MutablePq()->AddClusterMapping();
        clusterCfg->SetName("yc-logbroker-preprod");
        clusterCfg->SetEndpoint("lb.cc8035oc71oh9um52mv3.ydb.mdb.cloud-preprod.yandex.net:2135");
        clusterCfg->SetConfigManagerEndpoint("cm.global.logbroker.cloud-preprod.yandex.net:1111");
        clusterCfg->SetDatabase("/pre-prod_global/aoeb66ftj1tbt1b2eimn/cc8035oc71oh9um52mv3");
        clusterCfg->SetUseSsl(true);
        clusterCfg->SetServiceAccountId("bfbf4nc8c849ej11k3aq"); // audit-trails-reader-sa
        clusterCfg->SetServiceAccountIdSignature("SyFk1SpKdQa6L8IJxnwvgC57yMQ=");
        clusterCfg->SetClusterType(TPqClusterConfig::CT_PERS_QUEUE);
        clusters.emplace(clusterCfg->GetName(), PqProviderName);
    }
    {
        const auto clusterCfg = gatewaysConfig.MutablePq()->AddClusterMapping();
        clusterCfg->SetName("yc-logbroker");
        clusterCfg->SetEndpoint("lb.etn03iai600jur7pipla.ydb.mdb.yandexcloud.net:2135");
        clusterCfg->SetConfigManagerEndpoint("cm.global.logbroker.cloud.yandex.net:1111");
        clusterCfg->SetDatabase("/global/b1gvcqr959dbmi1jltep/etn03iai600jur7pipla");
        clusterCfg->SetUseSsl(true);
        clusterCfg->SetServiceAccountId("ajeg8dpl3e4ckjfj1qoq"); // yc-logbroker-reader
        clusterCfg->SetServiceAccountIdSignature("kPzxGKY7r4i7FRaqpY64WZP6pfM=");
        clusterCfg->SetClusterType(TPqClusterConfig::CT_PERS_QUEUE);
        clusters.emplace(clusterCfg->GetName(), PqProviderName);
    }
    {
        const auto clusterCfg = gatewaysConfig.MutableSolomon()->AddClusterMapping();
        clusterCfg->SetName("solomon_prod");
        clusterCfg->SetCluster("solomon.yandex.net");
        clusterCfg->SetClusterType(TSolomonClusterConfig::SCT_SOLOMON);
        clusterCfg->SetUseSsl(true);
        clusterCfg->SetToken(authToken);
        clusters.emplace(clusterCfg->GetName(), SolomonProviderName);
    }
}

} //NFq
