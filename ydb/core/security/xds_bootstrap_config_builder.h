#pragma once

#include <util/generic/string.h>
#include <ydb/core/protos/auth.pb.h>

namespace NJson {

class TJsonValue;

} // NJson

namespace NKikimr {

class TXdsBootstrapConfigBuilder {
private:
    NKikimrProto::TXdsBootstrap Config;
    TString DataCenterId;
    TString NodeId;

public:
    TXdsBootstrapConfigBuilder(const NKikimrProto::TXdsBootstrap& config, const TString& dataCenterId, const TString& nodeId);

    TString Build() const;

private:
    void BuildFieldNode(NJson::TJsonValue* json) const;
    void BuildFieldXdsServers(NJson::TJsonValue* json) const;
};

} // NKikimr
