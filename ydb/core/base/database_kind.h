#pragma once

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>

namespace NKikimr {

enum class EDatabaseKind {
    NotDatabase /* "NotDatabase" */,
    // The root database of the whole cluster (the root path of the root schemeshard).
    Root /* "Root" */,
    // An old-style lightweight database that has no tenant schemeshard of its own
    // and is served by the root schemeshard.
    SubDomain /* "SubDomain" */,
    // A database with a tenant schemeshard of its own and its own compute and storage resources.
    Dedicated /* "Dedicated" */,
    // A database with a tenant schemeshard of its own that uses compute and storage resources of a shared database.
    Serverless /* "Serverless" */,

    // Note that there is no separate kind for a shared database: from the scheme point of view
    // a shared database is indistinguishable from a dedicated one, so it is reported as Dedicated.
};

EDatabaseKind GetDatabaseKind(const NKikimrSchemeOp::TPathDescription& pathDescription);
EDatabaseKind GetDatabaseKind(const NKikimrScheme::TEvDescribeSchemeResult& describeResult);

bool IsDatabase(const NKikimrSchemeOp::TPathDescription& pathDescription);
bool IsDatabase(const NKikimrScheme::TEvDescribeSchemeResult& describeResult);

} // namespace NKikimr
