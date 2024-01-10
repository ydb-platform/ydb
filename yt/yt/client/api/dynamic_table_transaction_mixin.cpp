#include "dynamic_table_transaction_mixin.h"

#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/versioned_row.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

namespace NYT::NApi {

using namespace NTableClient;
using namespace NTabletClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

void TDynamicTableTransactionMixin::WriteRows(
    const NYPath::TYPath& path,
    TNameTablePtr nameTable,
    TSharedRange<TUnversionedRow> rows,
    const TModifyRowsOptions& options,
    ELockType lockType)
{
    THROW_ERROR_EXCEPTION_UNLESS(IsWriteLock(lockType), "Inappropriate lock type %Qlv given for write modification",
        lockType);

    std::vector<TRowModification> modifications;
    modifications.reserve(rows.Size());

    switch (lockType) {
        case ELockType::Exclusive: {
            for (auto row : rows) {
                modifications.push_back({ERowModificationType::Write, row.ToTypeErasedRow(), TLockMask()});
            }

            break;
        }

        case ELockType::SharedWrite: {
            // NB: This mount revision could differ from the one will be sent to tablet node.
            // However locks correctness will be checked in native transaction.
            const auto& tableMountCache = GetClient()->GetTableMountCache();
            auto tableInfo = WaitFor(tableMountCache->GetTableInfo(path))
                .ValueOrThrow();

            std::vector<int> columnIndexToLockIndex;
            GetLocksMapping(
                *tableInfo->Schemas[ETableSchemaKind::Write],
                GetAtomicity() == NTransactionClient::EAtomicity::Full,
                &columnIndexToLockIndex);

            for (auto row : rows) {
                TLockMask lockMask;
                for (const auto& value : row) {
                    auto lockIndex = columnIndexToLockIndex[value.Id];
                    if (lockIndex != -1) {
                        lockMask.Set(lockIndex, lockType);
                    }
                }

                modifications.push_back({ERowModificationType::WriteAndLock, row.ToTypeErasedRow(), lockMask});
            }

            break;
        }

        default:
            YT_ABORT();
    }

    ModifyRows(
        path,
        std::move(nameTable),
        MakeSharedRange(std::move(modifications), std::move(rows.ReleaseHolder())),
        options);
}

void TDynamicTableTransactionMixin::WriteRows(
    const NYPath::TYPath& path,
    TNameTablePtr nameTable,
    TSharedRange<TVersionedRow> rows,
    const TModifyRowsOptions& options)
{
    std::vector<TRowModification> modifications;
    modifications.reserve(rows.Size());

    for (auto row : rows) {
        modifications.push_back({ERowModificationType::VersionedWrite, row.ToTypeErasedRow(), TLockMask()});
    }

    ModifyRows(
        path,
        std::move(nameTable),
        MakeSharedRange(std::move(modifications), std::move(rows.ReleaseHolder())),
        options);
}

void TDynamicTableTransactionMixin::DeleteRows(
    const NYPath::TYPath& path,
    TNameTablePtr nameTable,
    TSharedRange<TLegacyKey> keys,
    const TModifyRowsOptions& options)
{
    std::vector<TRowModification> modifications;
    modifications.reserve(keys.Size());
    for (auto key : keys) {
        modifications.push_back({ERowModificationType::Delete, key.ToTypeErasedRow(), TLockMask()});
    }

    ModifyRows(
        path,
        std::move(nameTable),
        MakeSharedRange(std::move(modifications), std::move(keys.ReleaseHolder())),
        options);
}

void TDynamicTableTransactionMixin::LockRows(
    const NYPath::TYPath& path,
    TNameTablePtr nameTable,
    TSharedRange<TLegacyKey> keys,
    TLockMask lockMask)
{
    std::vector<TRowModification> modifications;
    modifications.reserve(keys.Size());

    for (auto key : keys) {
        TRowModification modification;
        modification.Type = ERowModificationType::WriteAndLock;
        modification.Row = key.ToTypeErasedRow();
        modification.Locks = lockMask;
        modifications.push_back(modification);
    }

    ModifyRows(
        path,
        std::move(nameTable),
        MakeSharedRange(std::move(modifications), std::move(keys)),
        TModifyRowsOptions());
}

void TDynamicTableTransactionMixin::LockRows(
    const NYPath::TYPath& path,
    TNameTablePtr nameTable,
    TSharedRange<TLegacyKey> keys,
    ELockType lockType)
{
    TLockMask lockMask;
    lockMask.Set(PrimaryLockIndex, lockType);
    LockRows(path, nameTable, keys, lockMask);
}

void TDynamicTableTransactionMixin::LockRows(
    const NYPath::TYPath& path,
    TNameTablePtr nameTable,
    TSharedRange<TLegacyKey> keys,
    const std::vector<TString>& locks,
    ELockType lockType)
{
    const auto& tableMountCache = GetClient()->GetTableMountCache();
    auto tableInfo = WaitFor(tableMountCache->GetTableInfo(path))
        .ValueOrThrow();

    auto lockMask = GetLockMask(
        *tableInfo->Schemas[ETableSchemaKind::Write],
        GetAtomicity() == NTransactionClient::EAtomicity::Full,
        locks,
        lockType);

    LockRows(path, nameTable, keys, lockMask);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
