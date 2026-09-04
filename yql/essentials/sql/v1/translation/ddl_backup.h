#pragma once

#include "node.h"

namespace NSQLTranslationV1 {

struct TCreateBackupCollectionParameters {
    std::map<TString, TDeferredAtom> Settings;

    bool Database;
    TVector<TDeferredAtom> Tables;

    bool ExistingOk;
};

struct TAlterBackupCollectionParameters {
    enum class EDatabase {
        Unchanged,
        Add,
        Drop,
    };

    std::map<TString, TDeferredAtom> Settings;
    std::set<TString> SettingsToReset;

    EDatabase Database = EDatabase::Unchanged;
    TVector<TDeferredAtom> TablesToAdd;
    TVector<TDeferredAtom> TablesToDrop;

    bool MissingOk;
};

struct TDropBackupCollectionParameters {
    bool MissingOk;
};

struct TBackupParameters {
    bool Incremental = false;
};

struct TRestoreParameters {
    TString At;
};

TNodePtr BuildCreateBackupCollection(
    TPosition pos,
    const TString& prefix,
    const TString& id,
    const TCreateBackupCollectionParameters& params,
    const TObjectOperatorContext& context);
TNodePtr BuildAlterBackupCollection(
    TPosition pos,
    const TString& prefix,
    const TString& id,
    const TAlterBackupCollectionParameters& params,
    const TObjectOperatorContext& context);
TNodePtr BuildDropBackupCollection(
    TPosition pos,
    const TString& prefix,
    const TString& id,
    const TDropBackupCollectionParameters& params,
    const TObjectOperatorContext& context);

TNodePtr BuildBackup(
    TPosition pos,
    const TString& prefix,
    const TString& id,
    const TBackupParameters& params,
    const TObjectOperatorContext& context);
TNodePtr BuildRestore(
    TPosition pos,
    const TString& prefix,
    const TString& id,
    const TRestoreParameters& params,
    const TObjectOperatorContext& context);

} // namespace NSQLTranslationV1
