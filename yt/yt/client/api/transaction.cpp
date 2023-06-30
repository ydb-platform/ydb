#include "transaction.h"

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/client/queue_client/consumer_client.h>

#include <yt/yt/client/ypath/rich.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

using namespace NTableClient;
using namespace NTabletClient;
using namespace NQueueClient;
using namespace NConcurrency;

/////////////////////////////////////////////////////////////////////////////

void ITransaction::WriteRows(
    const NYPath::TYPath& path,
    TNameTablePtr nameTable,
    TSharedRange<TUnversionedRow> rows,
    const TModifyRowsOptions& options)
{
    std::vector<TRowModification> modifications;
    modifications.reserve(rows.Size());

    for (auto row : rows) {
        modifications.push_back({ERowModificationType::Write, row.ToTypeErasedRow(), TLockMask()});
    }

    ModifyRows(
        path,
        std::move(nameTable),
        MakeSharedRange(std::move(modifications), std::move(rows.ReleaseHolder())),
        options);
}

void ITransaction::WriteRows(
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

void ITransaction::DeleteRows(
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

////////////////////////////////////////////////////////////////////////////////

void ITransaction::LockRows(
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

void ITransaction::LockRows(
    const NYPath::TYPath& path,
    TNameTablePtr nameTable,
    TSharedRange<TLegacyKey> keys,
    ELockType lockType)
{
    TLockMask lockMask;
    lockMask.Set(PrimaryLockIndex, lockType);
    LockRows(path, nameTable, keys, lockMask);
}

void ITransaction::LockRows(
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

void ITransaction::AdvanceConsumer(
    const NYPath::TYPath& path,
    int partitionIndex,
    std::optional<i64> oldOffset,
    i64 newOffset)
{
    THROW_ERROR_EXCEPTION_IF(newOffset < 0, "Queue consumer offset %v cannot be negative", newOffset);

    auto consumerClient = CreateBigRTConsumerClient(GetClient(), path);
    consumerClient->Advance(MakeStrong(this), partitionIndex, oldOffset, newOffset);
}

void ITransaction::AdvanceConsumer(
    const NYPath::TRichYPath& consumerPath,
    const NYPath::TRichYPath& queuePath,
    int partitionIndex,
    std::optional<i64> oldOffset,
    i64 newOffset)
{
    THROW_ERROR_EXCEPTION_IF(newOffset < 0, "Queue consumer offset %v cannot be negative", newOffset);

    // TODO(achulkov2): Support consumers from any cluster.
    auto subConsumerClient = CreateSubConsumerClient(GetClient(), consumerPath.GetPath(), queuePath);
    return subConsumerClient->Advance(MakeStrong(this), partitionIndex, oldOffset, newOffset);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
