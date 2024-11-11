#pragma once

#include <util/generic/string.h>

#include <optional>

#include <ydb/core/protos/flat_scheme_op.pb.h>

namespace NKikimr::NSchemeShard::NGenerated {

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

    constexpr inline static bool CreateDirsFromName = false;
};

template <NKikimrSchemeOp::EOperationType opType>
struct TSchemeTxTraits : public TSchemeTxTraitsFallback {};

template <>
struct TSchemeTxTraits<NKikimrSchemeOp::EOperationType::ESchemeOpMkDir> {
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
struct TSchemeTxTraits<NKikimrSchemeOp::EOperationType::ESchemeOpCreateTable> {
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
struct TSchemeTxTraits<NKikimrSchemeOp::EOperationType::ESchemeOpCreatePersQueueGroup> {
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
struct TSchemeTxTraits<NKikimrSchemeOp::EOperationType::ESchemeOpCreateSubDomain> {
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
struct TSchemeTxTraits<NKikimrSchemeOp::EOperationType::ESchemeOpCreateExtSubDomain> {
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
struct TSchemeTxTraits<NKikimrSchemeOp::EOperationType::ESchemeOpCreateRtmrVolume> {
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
struct TSchemeTxTraits<NKikimrSchemeOp::EOperationType::ESchemeOpCreateBlockStoreVolume> {
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
struct TSchemeTxTraits<NKikimrSchemeOp::EOperationType::ESchemeOpCreateFileStore> {
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
struct TSchemeTxTraits<NKikimrSchemeOp::EOperationType::ESchemeOpCreateKesus> {
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
struct TSchemeTxTraits<NKikimrSchemeOp::EOperationType::ESchemeOpCreateSolomonVolume> {
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
struct TSchemeTxTraits<NKikimrSchemeOp::EOperationType::ESchemeOpCreateIndexedTable> {
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
struct TSchemeTxTraits<NKikimrSchemeOp::EOperationType::ESchemeOpCreateColumnStore> {
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
struct TSchemeTxTraits<NKikimrSchemeOp::EOperationType::ESchemeOpCreateColumnTable> {
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
struct TSchemeTxTraits<NKikimrSchemeOp::EOperationType::ESchemeOpCreateExternalTable> {
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
struct TSchemeTxTraits<NKikimrSchemeOp::EOperationType::ESchemeOpCreateExternalDataSource> {
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
struct TSchemeTxTraits<NKikimrSchemeOp::EOperationType::ESchemeOpCreateView> {
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
struct TSchemeTxTraits<NKikimrSchemeOp::EOperationType::ESchemeOpCreateResourcePool> {
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
struct TSchemeTxTraits<NKikimrSchemeOp::EOperationType::ESchemeOpCreateBackupCollection> {
    static std::optional<TString> GetTargetName(const TTxTransaction& tx) {
        return tx.GetCreateBackupCollection().GetName();
    }

    static bool SetName(TTxTransaction& tx, const TString& name) {
        tx.MutableCreateBackupCollection()->SetName(name);
        return true;
    }

    constexpr inline static bool CreateDirsFromName = true;
};

} // namespace NKikimr::NSchemeShard::NGenerated
