#include "database_kind.h"

#include <ydb/core/protos/subdomains.pb.h>
#include <ydb/core/scheme/scheme_pathid.h>

namespace NKikimr {

namespace {

// Local path id of the root path of any schemeshard
constexpr TLocalPathId RootLocalPathId = 1;

} // namespace

EDatabaseKind GetDatabaseKind(const NKikimrSchemeOp::TPathDescription& pathDescription) {
    if (!pathDescription.HasSelf()) {
        return EDatabaseKind::NotDatabase;
    }

    const auto& self = pathDescription.GetSelf();
    const TPathId pathId(self.GetSchemeshardId(), self.GetPathId());

    // DomainDescription describes the subdomain the path belongs to (not the path itself).
    const auto& subdomain = pathDescription.GetDomainDescription();

    // Subdomain root key is a path-id under which subdomain is known to the root schemeshard.
    // Subdomain local key is a path-id under which subdomain is known to itself.
    // So subdomain's schema root path (unlike any other path in the system) has not a single unique
    // path-id but two path-ids:
    // - global or root's -- from the point of view of root schemeshard (where all subdomains are registered),
    // - and local -- from the point of view of tenant schemeshard (where belongs all subdomain paths).
    const TPathId subdomainRootKey = subdomain.HasDomainKey()
        ? TPathId::FromDomainKey(subdomain.GetDomainKey())
        : TPathId();

    // Subdomain resources (root) key is the path-id of the subdomain that provides compute and storage
    // resources for this subdomain in question.
    // Subdomain resources (root) key is equal to the subdomainRootKey unless the database is serverless.
    const TPathId resourcesSubdomainRootKey = subdomain.HasResourcesDomainKey()
        ? TPathId::FromDomainKey(subdomain.GetResourcesDomainKey())
        : TPathId();

    const bool isServerless = subdomainRootKey && resourcesSubdomainRootKey &&
        resourcesSubdomainRootKey != subdomainRootKey;

    switch (self.GetPathType()) {
        case NKikimrSchemeOp::EPathTypeDir:
            // Root database path can have a directory type.
            if (subdomainRootKey && subdomainRootKey == pathId) {
                if (pathId.LocalPathId == RootLocalPathId) {
                    return EDatabaseKind::Root;
                }
                // A directory that is a subdomain root but not the root path of a schemeshard
                // (i.e. pathId.LocalPathId != RootLocalPathId) means a broken configuration:
                // should the enum ever get an Invalid kind, that is the place to report it.
            }

            return EDatabaseKind::NotDatabase;

        case NKikimrSchemeOp::EPathTypeSubDomain:
            if (subdomainRootKey && subdomainRootKey != pathId && pathId.LocalPathId == RootLocalPathId) {
                // The path is the root path of a subdomain that is served by its own (tenant) schemeshard.
                return isServerless ? EDatabaseKind::Serverless : EDatabaseKind::Dedicated;
            }
            if (subdomain.GetProcessingParams().HasSchemeShard() &&
                subdomain.GetProcessingParams().GetSchemeShard() != self.GetSchemeshardId()
            ) {
                // An old-style subdomain that has got a tenant schemeshard of its own is a full-fledged
                // database too (that is how databases were created before ext subdomains).
                return isServerless ? EDatabaseKind::Serverless : EDatabaseKind::Dedicated;
            }
            // A subdomain that has no tenant schemeshard of its own, so it is served by the very
            // schemeshard that has described it.
            //
            // The same answer is given when there is no DomainDescription in the result at all:
            // then both checks above fail (their data is missing) and there is nothing left to
            // distinguish the kinds by, while the path type alone already means it is a database.
            return EDatabaseKind::SubDomain;

        case NKikimrSchemeOp::EPathTypeExtSubDomain:
            return isServerless ? EDatabaseKind::Serverless : EDatabaseKind::Dedicated;

        default:
            return EDatabaseKind::NotDatabase;
    }
}

EDatabaseKind GetDatabaseKind(const NKikimrScheme::TEvDescribeSchemeResult& describeResult) {
    // Status is intentionally not checked here: on failure PathDescription.Self is not filled in,
    // so an unsuccessful result yields NotDatabase anyway.
    return GetDatabaseKind(describeResult.GetPathDescription());
}

bool IsDatabase(const NKikimrSchemeOp::TPathDescription& pathDescription) {
    return GetDatabaseKind(pathDescription) != EDatabaseKind::NotDatabase;
}

bool IsDatabase(const NKikimrScheme::TEvDescribeSchemeResult& describeResult) {
    return GetDatabaseKind(describeResult) != EDatabaseKind::NotDatabase;
}

} // namespace NKikimr
