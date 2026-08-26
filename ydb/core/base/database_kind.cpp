#include "database_kind.h"

#include <ydb/core/base/subdomain.h>

namespace NKikimr {

namespace {

// Local path id of the root path of any schemeshard
constexpr ui64 RootLocalPathId = 1;

} // namespace

EDatabaseKind GetDatabaseKind(const NKikimrSchemeOp::TPathDescription& pathDescription) {
    if (!pathDescription.HasSelf()) {
        return EDatabaseKind::NotDatabase;
    }

    const auto& self = pathDescription.GetSelf();
    const TSubDomainKey selfKey(self.GetSchemeshardId(), self.GetPathId());

    // DomainDescription describes the database the path belongs to (not the path itself).
    const auto& domain = pathDescription.GetDomainDescription();

    // DomainKey is the key of that database as it is known to the domain (global) schemeshard:
    // when a database is described by its own (tenant) schemeshard, DomainKey is the key of the
    // database path on the domain schemeshard and thus differs from the key of the described path.
    const TSubDomainKey domainKey(domain.GetDomainKey());

    // ResourcesDomainKey is the key of the database that owns the storage resources:
    // it is the database itself unless the database is serverless.
    const TSubDomainKey resourcesDomainKey(domain.GetResourcesDomainKey());

    const bool isServerless = domainKey && resourcesDomainKey && resourcesDomainKey != domainKey;

    switch (self.GetPathType()) {
        case NKikimrSchemeOp::EPathTypeDir:
            // The only directory that is a database is the root path of the domain schemeshard.
            if (domainKey && domainKey == selfKey && self.GetPathId() == RootLocalPathId) {
                return EDatabaseKind::Root;
            }
            return EDatabaseKind::NotDatabase;

        case NKikimrSchemeOp::EPathTypeSubDomain:
            if (domainKey && domainKey != selfKey) {
                // The path is the root path of a tenant schemeshard, i.e. a full-fledged database
                // described by the schemeshard that serves it.
                return isServerless ? EDatabaseKind::Serverless : EDatabaseKind::Dedicated;
            }
            if (domain.GetProcessingParams().HasSchemeShard() &&
                domain.GetProcessingParams().GetSchemeShard() != self.GetSchemeshardId()
            ) {
                // An old-style subdomain that has got a schemeshard of its own is a full-fledged database too
                // (that is how databases were created before ext subdomains).
                return isServerless ? EDatabaseKind::Serverless : EDatabaseKind::Dedicated;
            }
            return EDatabaseKind::SubDomain;

        case NKikimrSchemeOp::EPathTypeExtSubDomain:
            return isServerless ? EDatabaseKind::Serverless : EDatabaseKind::Dedicated;

        default:
            return EDatabaseKind::NotDatabase;
    }
}

EDatabaseKind GetDatabaseKind(const NKikimrScheme::TEvDescribeSchemeResult& describeResult) {
    return GetDatabaseKind(describeResult.GetPathDescription());
}

bool IsDatabase(const NKikimrSchemeOp::TPathDescription& pathDescription) {
    return GetDatabaseKind(pathDescription) != EDatabaseKind::NotDatabase;
}

bool IsDatabase(const NKikimrScheme::TEvDescribeSchemeResult& describeResult) {
    return GetDatabaseKind(describeResult) != EDatabaseKind::NotDatabase;
}

} // namespace NKikimr
