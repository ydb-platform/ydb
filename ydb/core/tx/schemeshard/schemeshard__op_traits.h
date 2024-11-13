#pragma once

#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <util/generic/string.h>
#include <util/generic/map.h>

#include <optional>

namespace NKikimr::NSchemeShard {

using TTxTransaction = NKikimrSchemeOp::TModifyScheme;

struct TSchemeTxTraitsFallback {
    static std::optional<TString> GetTargetName(const TTxTransaction& tx) {
        Y_UNUSED(tx);
        return std::nullopt;
    }

    static bool SetName(TTxTransaction& tx, const TString& name) {
        Y_UNUSED(tx, name);
        return false;
    }

    static THashMap<TString, THashSet<TString>> GetRequiredPaths(const TTxTransaction& tx) {
        Y_UNUSED(tx);
        return {};
    }

    constexpr inline static bool CreateDirsFromName = false;

    constexpr inline static bool CreateAdditionalDirs = false;
};

template <NKikimrSchemeOp::EOperationType opType>
struct TSchemeTxTraits : public TSchemeTxTraitsFallback {};

template <>
struct TSchemeTxTraits<NKikimrSchemeOp::EOperationType::ESchemeOpMkDir> : public TSchemeTxTraitsFallback {
    static std::optional<TString> GetTargetName(const TTxTransaction& tx) {
        return tx.GetMkDir().GetName();
    }

    static bool SetName(TTxTransaction& tx, const TString& name) {
        tx.MutableMkDir()->SetName(name);
        return true;
    }

    constexpr inline static bool CreateDirsFromName = true;
};

template <>
struct TSchemeTxTraits<NKikimrSchemeOp::EOperationType::ESchemeOpCreateTable> : public TSchemeTxTraitsFallback {
    static std::optional<TString> GetTargetName(const TTxTransaction& tx) {
        if (tx.GetCreateTable().HasCopyFromTable()) {
            return std::nullopt;
        }
        return tx.GetCreateTable().GetName();
    }

    static bool SetName(TTxTransaction& tx, const TString& name) {
        tx.MutableCreateTable()->SetName(name);
        return true;
    }

    constexpr inline static bool CreateDirsFromName = true;
};

template <>
struct TSchemeTxTraits<NKikimrSchemeOp::EOperationType::ESchemeOpCreatePersQueueGroup> : public TSchemeTxTraitsFallback {
    static std::optional<TString> GetTargetName(const TTxTransaction& tx) {
        return tx.GetCreatePersQueueGroup().GetName();
    }

    static bool SetName(TTxTransaction& tx, const TString& name) {
        tx.MutableCreatePersQueueGroup()->SetName(name);
        return true;
    }

    constexpr inline static bool CreateDirsFromName = true;
};

template <>
struct TSchemeTxTraits<NKikimrSchemeOp::EOperationType::ESchemeOpCreateSubDomain> : public TSchemeTxTraitsFallback {
    static std::optional<TString> GetTargetName(const TTxTransaction& tx) {
        return tx.GetSubDomain().GetName();
    }

    static bool SetName(TTxTransaction& tx, const TString& name) {
        tx.MutableSubDomain()->SetName(name);
        return true;
    }

    constexpr inline static bool CreateDirsFromName = true;
};

template <>
struct TSchemeTxTraits<NKikimrSchemeOp::EOperationType::ESchemeOpCreateExtSubDomain> : public TSchemeTxTraitsFallback {
    static std::optional<TString> GetTargetName(const TTxTransaction& tx) {
        return tx.GetSubDomain().GetName();
    }

    static bool SetName(TTxTransaction& tx, const TString& name) {
        tx.MutableSubDomain()->SetName(name);
        return true;
    }

    constexpr inline static bool CreateDirsFromName = true;
};

template <>
struct TSchemeTxTraits<NKikimrSchemeOp::EOperationType::ESchemeOpCreateRtmrVolume> : public TSchemeTxTraitsFallback {
    static std::optional<TString> GetTargetName(const TTxTransaction& tx) {
        return tx.GetCreateRtmrVolume().GetName();
    }

    static bool SetName(TTxTransaction& tx, const TString& name) {
        tx.MutableCreateRtmrVolume()->SetName(name);
        return true;
    }

    constexpr inline static bool CreateDirsFromName = true;
};

template <>
struct TSchemeTxTraits<NKikimrSchemeOp::EOperationType::ESchemeOpCreateBlockStoreVolume> : public TSchemeTxTraitsFallback {
    static std::optional<TString> GetTargetName(const TTxTransaction& tx) {
        return tx.GetCreateBlockStoreVolume().GetName();
    }

    static bool SetName(TTxTransaction& tx, const TString& name) {
        tx.MutableCreateBlockStoreVolume()->SetName(name);
        return true;
    }

    constexpr inline static bool CreateDirsFromName = true;
};

template <>
struct TSchemeTxTraits<NKikimrSchemeOp::EOperationType::ESchemeOpCreateFileStore> : public TSchemeTxTraitsFallback {
    static std::optional<TString> GetTargetName(const TTxTransaction& tx) {
        return tx.GetCreateFileStore().GetName();
    }

    static bool SetName(TTxTransaction& tx, const TString& name) {
        tx.MutableCreateFileStore()->SetName(name);
        return true;
    }

    constexpr inline static bool CreateDirsFromName = true;
};

template <>
struct TSchemeTxTraits<NKikimrSchemeOp::EOperationType::ESchemeOpCreateKesus> : public TSchemeTxTraitsFallback {
    static std::optional<TString> GetTargetName(const TTxTransaction& tx) {
        return tx.GetKesus().GetName();
    }

    static bool SetName(TTxTransaction& tx, const TString& name) {
        tx.MutableKesus()->SetName(name);
        return true;
    }

    constexpr inline static bool CreateDirsFromName = true;
};

template <>
struct TSchemeTxTraits<NKikimrSchemeOp::EOperationType::ESchemeOpCreateSolomonVolume> : public TSchemeTxTraitsFallback {
    static std::optional<TString> GetTargetName(const TTxTransaction& tx) {
        return tx.GetCreateSolomonVolume().GetName();
    }

    static bool SetName(TTxTransaction& tx, const TString& name) {
        tx.MutableCreateSolomonVolume()->SetName(name);
        return true;
    }

    constexpr inline static bool CreateDirsFromName = true;
};

template <>
struct TSchemeTxTraits<NKikimrSchemeOp::EOperationType::ESchemeOpCreateIndexedTable> : public TSchemeTxTraitsFallback {
    static std::optional<TString> GetTargetName(const TTxTransaction& tx) {
        return tx.GetCreateIndexedTable().GetTableDescription().GetName();
    }

    static bool SetName(TTxTransaction& tx, const TString& name) {
        tx.MutableCreateIndexedTable()->MutableTableDescription()->SetName(name);
        return true;
    }

    constexpr inline static bool CreateDirsFromName = true;
};

template <>
struct TSchemeTxTraits<NKikimrSchemeOp::EOperationType::ESchemeOpCreateColumnStore> : public TSchemeTxTraitsFallback {
    static std::optional<TString> GetTargetName(const TTxTransaction& tx) {
        return tx.GetCreateColumnStore().GetName();
    }

    static bool SetName(TTxTransaction& tx, const TString& name) {
        tx.MutableCreateColumnStore()->SetName(name);
        return true;
    }

    constexpr inline static bool CreateDirsFromName = true;
};

template <>
struct TSchemeTxTraits<NKikimrSchemeOp::EOperationType::ESchemeOpCreateColumnTable> : public TSchemeTxTraitsFallback {
    static std::optional<TString> GetTargetName(const TTxTransaction& tx) {
        return tx.GetCreateColumnTable().GetName();
    }

    static bool SetName(TTxTransaction& tx, const TString& name) {
        tx.MutableCreateColumnTable()->SetName(name);
        return true;
    }

    constexpr inline static bool CreateDirsFromName = true;
};

template <>
struct TSchemeTxTraits<NKikimrSchemeOp::EOperationType::ESchemeOpCreateExternalTable> : public TSchemeTxTraitsFallback {
    static std::optional<TString> GetTargetName(const TTxTransaction& tx) {
        return tx.GetCreateExternalTable().GetName();
    }

    static bool SetName(TTxTransaction& tx, const TString& name) {
        tx.MutableCreateExternalTable()->SetName(name);
        return true;
    }

    constexpr inline static bool CreateDirsFromName = true;
};

template <>
struct TSchemeTxTraits<NKikimrSchemeOp::EOperationType::ESchemeOpCreateExternalDataSource> : public TSchemeTxTraitsFallback {
    static std::optional<TString> GetTargetName(const TTxTransaction& tx) {
        return tx.GetCreateExternalDataSource().GetName();
    }

    static bool SetName(TTxTransaction& tx, const TString& name) {
        tx.MutableCreateExternalDataSource()->SetName(name);
        return true;
    }

    constexpr inline static bool CreateDirsFromName = true;
};

template <>
struct TSchemeTxTraits<NKikimrSchemeOp::EOperationType::ESchemeOpCreateView> : public TSchemeTxTraitsFallback {
    static std::optional<TString> GetTargetName(const TTxTransaction& tx) {
        return tx.GetCreateView().GetName();
    }

    static bool SetName(TTxTransaction& tx, const TString& name) {
        tx.MutableCreateView()->SetName(name);
        return true;
    }

    constexpr inline static bool CreateDirsFromName = true;
};

template <>
struct TSchemeTxTraits<NKikimrSchemeOp::EOperationType::ESchemeOpCreateResourcePool> : public TSchemeTxTraitsFallback {
    static std::optional<TString> GetTargetName(const TTxTransaction& tx) {
        return tx.GetCreateResourcePool().GetName();
    }

    static bool SetName(TTxTransaction& tx, const TString& name) {
        tx.MutableCreateResourcePool()->SetName(name);
        return true;
    }

    constexpr inline static bool CreateDirsFromName = true;
};

template <>
struct TSchemeTxTraits<NKikimrSchemeOp::EOperationType::ESchemeOpCreateBackupCollection> : public TSchemeTxTraitsFallback {
    static std::optional<TString> GetTargetName(const TTxTransaction& tx) {
        return tx.GetCreateBackupCollection().GetName();
    }

    static bool SetName(TTxTransaction& tx, const TString& name) {
        tx.MutableCreateBackupCollection()->SetName(name);
        return true;
    }

    constexpr inline static bool CreateDirsFromName = true;
};

} // namespace NKikimr::NSchemeShard
