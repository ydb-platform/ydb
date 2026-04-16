#pragma once

#include <memory>

#include <util/generic/string.h>
#include <ydb/core/base/storage_pools.h>
#include <ydb/core/base/subdomain.h>
#include <ydb/public/api/protos/ydb_table.pb.h>

namespace grpc {
class Channel;
}

namespace NKikimr {
namespace Tests {
class TClient;
}

Ydb::Table::ExecuteQueryResult ExecYql(
    std::shared_ptr<grpc::Channel> channel,
    const TString& sessionId,
    const TString& yql,
    bool withStat = false);

TString CreateSession(std::shared_ptr<grpc::Channel> channel);

TStoragePools CreatePoolsForTenant(
    Tests::TClient& client,
    const TDomainsInfo::TDomain::TStoragePoolKinds& poolTypes,
    const TString& tenant);

NKikimrSubDomains::TSubDomainSettings GetSubDomainDeclarationSetting(const TString& name);

NKikimrSubDomains::TSubDomainSettings GetSubDomainDefaultSetting(
    const TString& name,
    const TStoragePools& pools = {});

} // namespace NKikimr
