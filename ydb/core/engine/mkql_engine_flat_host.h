#pragma once
#include <ydb/library/yql/minikql/defs.h>
#include <ydb/core/engine/mkql_keys.h>
#include <ydb/core/tablet_flat/flat_database.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>

namespace NKikimr {
namespace NMiniKQL {

class IEngineFlatHost {
public:
    struct TUpdateCommand {
        ui32 Column;
        TKeyDesc::EColumnOperation Operation; // Read is not allowed here
        EInplaceUpdateMode InplaceUpdateMode;
        TCell Value;
    };

    using TPeriodicCallback = std::function<void()>;

    virtual ~IEngineFlatHost() {}

    // Returns shard id of the host.
    virtual ui64 GetShardId() const = 0;

    // Returns table info
    virtual const NTable::TScheme::TTableInfo* GetTableInfo(const TTableId& tableId) const = 0;

    // Returns whether shard is read only.
    virtual bool IsReadonly() const = 0;

    // Validate key and fill status into it.
    virtual bool IsValidKey(TKeyDesc& key) const = 0;

    // Calculate the whole size of data that needs to be read into memory
    virtual ui64 CalculateReadSize(const TVector<const TKeyDesc*>& keys) const = 0;

    // Exstimate size of the merged result of reading the data
    virtual ui64 CalculateResultSize(const TKeyDesc& key) const = 0;

    // At Tx execution make sure that all pages are loaded
    virtual void PinPages(const TVector<THolder<TKeyDesc>>& keys, ui64 pageFaultCount = 0) = 0;

    // Returns empty optional with type 'returnType' or the filled one.
    virtual NUdf::TUnboxedValue SelectRow(const TTableId& tableId, const TArrayRef<const TCell>& row,
        TStructLiteral* columnIds, TOptionalType* returnType, const TReadTarget& readTarget,
        const THolderFactory& holderFactory) = 0;

    // Returns struct with type 'returnType' - that should have fields 'List' and 'Truncated'.
    virtual NUdf::TUnboxedValue SelectRange(const TTableId& tableId, const TTableRange& range,
        TStructLiteral* columnIds, TListLiteral* skipNullKeys, TStructType* returnType,
        const TReadTarget& readTarget, ui64 itemsLimit, ui64 bytesLimit, bool reverse,
        std::pair<const TListLiteral*, const TListLiteral*> forbidNullArgs, const THolderFactory& holderFactory) = 0;

    // Updates the single row. Column in commands must be unique.
    virtual void UpdateRow(const TTableId& tableId, const TArrayRef<const TCell>& row,
        const TArrayRef<const TUpdateCommand>& commands) = 0;

    // Erases the single row.
    virtual void EraseRow(const TTableId& tableId, const TArrayRef<const TCell>& row) = 0;

    // Check that table is erased
    virtual bool IsPathErased(const TTableId& tableId) const = 0;

    // Returns whether row belong this shard.
    virtual bool IsMyKey(const TTableId& tableId, const TArrayRef<const TCell>& row) const = 0;

    // Returns schema version for given tableId
    virtual ui64 GetTableSchemaVersion(const TTableId& tableId) const = 0;

    // Set callback for periodic execution
    virtual void SetPeriodicCallback(TPeriodicCallback&& callback) = 0;
};

}
}
