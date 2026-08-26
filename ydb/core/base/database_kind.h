#pragma once

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>

namespace NKikimr {

enum class EDatabaseKind {
    NotDatabase /* "NotDatabase" */,
    // The root database of the whole cluster (the root path of the domain schemeshard).
    Root /* "Root" */,
    // An old-style lightweight database that lives inside another database's schemeshard.
    SubDomain /* "SubDomain" */,
    // A database with its own schemeshard and its own storage resources.
    Dedicated /* "Dedicated" */,
    // A database with its own schemeshard that uses resources of a shared database.
    Serverless /* "Serverless" */,

    // Note that there is no separate kind for a shared database: from the scheme point of view
    // a shared database is indistinguishable from a dedicated one, so it is reported as Dedicated.
};

EDatabaseKind GetDatabaseKind(const NKikimrSchemeOp::TPathDescription& pathDescription);
EDatabaseKind GetDatabaseKind(const NKikimrScheme::TEvDescribeSchemeResult& describeResult);

bool IsDatabase(const NKikimrSchemeOp::TPathDescription& pathDescription);
bool IsDatabase(const NKikimrScheme::TEvDescribeSchemeResult& describeResult);

} // namespace NKikimr
