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

    std::vector<NFuture::TRowModification> modifications;
    modifications.reserve(rows.Size());

    switch (lockType) {
        case ELockType::Exclusive: {
            for (auto row : rows) {
                modifications.push_back(NFuture::NRowModifications::TWriteRow(row));
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

                modifications.push_back(NFuture::NRowModifications::TWriteAndLockRow(row, lockMask));
            }

            break;
        }

        default:
            YT_ABORT();
    }

    FutureModifyRows(
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
    std::vector<NFuture::TRowModification> modifications;
    modifications.reserve(rows.Size());

    for (auto row : rows) {
        modifications.push_back(NFuture::NRowModifications::TVersionedWriteRow(row.ToTypeErasedRow()));
    }

    FutureModifyRows(
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
    std::vector<NFuture::TRowModification> modifications;
    modifications.reserve(keys.Size());
    for (auto key : keys) {
        modifications.push_back(NFuture::NRowModifications::TDeleteRow(key));
    }

    FutureModifyRows(
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
    std::vector<NFuture::TRowModification> modifications;
    modifications.reserve(keys.Size());

    for (auto key : keys) {
        modifications.push_back(NFuture::NRowModifications::TWriteAndLockRow(key, lockMask));
    }

    FutureModifyRows(
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
    const std::vector<std::string>& locks,
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

void TDynamicTableTransactionMixin::ModifyRows(
    const NYPath::TYPath& path,
    TNameTablePtr nameTable,
    TSharedRange<TRowModification> modifications,
    const TModifyRowsOptions& options)
{
    std::vector<NFuture::TRowModification> newModifications;
    newModifications.reserve(modifications.Size());

    for (const auto& modification : modifications) {
        THROW_ERROR_EXCEPTION_IF(modification.Type != ERowModificationType::WriteAndLock && !modification.Locks.IsNone(),
            "Cannot perform lock by %Qlv modification type, use %Qlv",
            modification.Type,
            ERowModificationType::WriteAndLock);

        switch (modification.Type) {
            case ERowModificationType::Write:
                newModifications.push_back(NFuture::NRowModifications::TWriteRow(TUnversionedRow(modification.Row)));
                break;

            case ERowModificationType::Delete:
                newModifications.push_back(NFuture::NRowModifications::TDeleteRow(TLegacyKey(modification.Row)));
                break;

            case ERowModificationType::VersionedWrite:
                newModifications.push_back(NFuture::NRowModifications::TVersionedWriteRow(modification.Row));
                break;

            case ERowModificationType::WriteAndLock:
                newModifications.push_back(NFuture::NRowModifications::TWriteAndLockRow(
                    TUnversionedRow(modification.Row),
                    modification.Locks));
                break;
        }
    }

    FutureModifyRows(
        path,
        std::move(nameTable),
        MakeSharedRange(std::move(newModifications), std::move(modifications.ReleaseHolder())),
        options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
