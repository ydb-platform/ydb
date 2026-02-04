#pragma once

#include <util/generic/string.h>

namespace NYdb::inline Dev::NReplication {
    class TReplicationDescription;
    class TTransferDescription;
}

namespace NYql {
    class TIssues;
}

namespace NYdb::NDump {

TString BuildCreateReplicationQuery(
    const TString& db,
    const TString& backupRoot,
    const TString& name,
    const NReplication::TReplicationDescription& desc);

constexpr TStringBuf TRANSFER_LAMBDA_DEFAULT_NAME = "$__ydb_transfer_lambda";

TString BuildCreateTransferQuery(
    const TString& db,
    const TString& backupRoot,
    const TString& name,
    const NReplication::TTransferDescription& desc);

bool RewriteCreateAsyncReplicationQueryNoSecrets(
    TString& query,
    const TString& dbRestoreRoot,
    const TString& dbPath,
    NYql::TIssues& issues);

bool RewriteCreateTransferQueryNoSecrets(
    TString& query,
    const TString& dbRestoreRoot,
    const TString& dbPath,
    NYql::TIssues& issues);

bool RewriteCreateAsyncReplicationQuery(
    TString& query,
    const TString& dbRestoreRoot,
    const TString& dbPath,
    NYql::TIssues& issues);

bool RewriteCreateTransferQuery(
    TString& query,
    const TString& dbRestoreRoot,
    const TString& dbPath,
    NYql::TIssues& issues);

} // namespace NYdb::NDump
