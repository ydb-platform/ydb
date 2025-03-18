#pragma once
#include "constructor_meta.h"
#include "portion_info.h"

#include <ydb/core/tx/columnshard/engines/scheme/versions/abstract_scheme.h>

#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NOlap {

class TColumnChunkLoadContextV1;
class TIndexChunkLoadContext;
class TPortionAccessorConstructor;

class TPortionInfoConstructor {
private:
    bool Constructed = false;
    YDB_ACCESSOR(NColumnShard::TInternalPathId, PathId, NColumnShard::TInternalPathId{});
    std::optional<ui64> PortionId;

    TPortionMetaConstructor MetaConstructor;

    std::optional<TSnapshot> MinSnapshotDeprecated;
    std::optional<TSnapshot> RemoveSnapshot;
    std::optional<ui64> SchemaVersion;
    std::optional<ui64> ShardingVersion;

    std::optional<TSnapshot> CommitSnapshot;
    std::optional<TInsertWriteId> InsertWriteId;

    TPortionInfoConstructor(const TPortionInfoConstructor&) = default;
    TPortionInfoConstructor& operator=(const TPortionInfoConstructor&) = default;

    TPortionInfoConstructor(TPortionInfo&& portion)
        : PathId(portion.GetPathId())
        , PortionId(portion.GetPortionId())
        , MinSnapshotDeprecated(portion.GetMinSnapshotDeprecated())
        , RemoveSnapshot(portion.GetRemoveSnapshotOptional())
        , SchemaVersion(portion.GetSchemaVersionOptional())
        , ShardingVersion(portion.GetShardingVersionOptional()) {
        MetaConstructor = TPortionMetaConstructor(std::move(portion.Meta), true);
    }
    friend class TPortionAccessorConstructor;

public:
    TPortionInfoConstructor(TPortionInfoConstructor&&) noexcept = default;
    TPortionInfoConstructor& operator=(TPortionInfoConstructor&&) noexcept = default;

    TPortionInfoConstructor(const TPortionInfo& portion, const bool withMetadata, const bool withMetadataBlobs)
        : PathId(portion.GetPathId())
        , PortionId(portion.GetPortionId())
        , MinSnapshotDeprecated(portion.GetMinSnapshotDeprecated())
        , RemoveSnapshot(portion.GetRemoveSnapshotOptional())
        , SchemaVersion(portion.GetSchemaVersionOptional())
        , ShardingVersion(portion.GetShardingVersionOptional())
        , CommitSnapshot(portion.GetCommitSnapshotOptional())
        , InsertWriteId(portion.GetInsertWriteIdOptional()) {
        if (withMetadata) {
            MetaConstructor = TPortionMetaConstructor(portion.Meta, withMetadataBlobs);
        } else {
            AFL_VERIFY(!withMetadataBlobs);
        }
    }

    bool HaveBlobsData() {
        return MetaConstructor.GetBlobIdsCount();
    }

    void SetPortionId(const ui64 value) {
        AFL_VERIFY(value);
        PortionId = value;
    }

    void AddMetadata(const ISnapshotSchema& snapshotSchema, const std::shared_ptr<arrow::RecordBatch>& batch);

    void AddMetadata(const ISnapshotSchema& snapshotSchema, const ui32 deletionsCount, const NArrow::TFirstLastSpecialKeys& firstLastRecords,
        const std::optional<NArrow::TMinMaxSpecialKeys>& minMaxSpecial) {
        MetaConstructor.FillMetaInfo(firstLastRecords, deletionsCount, minMaxSpecial, snapshotSchema.GetIndexInfo());
    }

    ui64 GetPortionIdVerified() const {
        AFL_VERIFY(PortionId);
        AFL_VERIFY(*PortionId);
        return *PortionId;
    }

    TPortionMetaConstructor& MutableMeta() {
        return MetaConstructor;
    }

    const TPortionMetaConstructor& GetMeta() const {
        return MetaConstructor;
    }

    TInsertWriteId GetInsertWriteIdVerified() const {
        AFL_VERIFY(InsertWriteId);
        return *InsertWriteId;
    }

    TPortionAddress GetAddress() const {
        return TPortionAddress(PathId, GetPortionIdVerified());
    }

    bool HasRemoveSnapshot() const {
        return !!RemoveSnapshot;
    }

    TPortionInfoConstructor(const NColumnShard::TInternalPathId pathId, const ui64 portionId)
        : PathId(pathId)
        , PortionId(portionId) {
        AFL_VERIFY(PathId);
        AFL_VERIFY(PortionId);
    }

    TPortionInfoConstructor(const NColumnShard::TInternalPathId pathId)
        : PathId(pathId) {
        AFL_VERIFY(PathId);
    }

    const TSnapshot& GetMinSnapshotDeprecatedVerified() const {
        AFL_VERIFY(!!MinSnapshotDeprecated);
        return *MinSnapshotDeprecated;
    }

    std::shared_ptr<ISnapshotSchema> GetSchema(const TVersionedIndex& index) const;

    void SetCommitSnapshot(const TSnapshot& snap) {
        AFL_VERIFY(!!InsertWriteId);
        AFL_VERIFY(!CommitSnapshot);
        AFL_VERIFY(snap.Valid());
        CommitSnapshot = snap;
    }

    void SetInsertWriteId(const TInsertWriteId value) {
        AFL_VERIFY(!InsertWriteId);
        AFL_VERIFY((ui64)value);
        InsertWriteId = value;
    }

    void SetMinSnapshotDeprecated(const TSnapshot& snap) {
        Y_ABORT_UNLESS(snap.Valid());
        MinSnapshotDeprecated = snap;
    }

    void SetSchemaVersion(const ui64 version) {
        //        AFL_VERIFY(version);
        SchemaVersion = version;
    }

    void SetShardingVersion(const ui64 version) {
        //        AFL_VERIFY(version);
        ShardingVersion = version;
    }

    void SetRemoveSnapshot(const TSnapshot& snap) {
        AFL_VERIFY(!RemoveSnapshot);
        if (snap.Valid()) {
            RemoveSnapshot = snap;
        }
    }

    void SetRemoveSnapshot(const ui64 planStep, const ui64 txId) {
        SetRemoveSnapshot(TSnapshot(planStep, txId));
    }

    TString DebugString() const {
        TStringBuilder sb;
        sb << (PortionId ? *PortionId : 0) << ";";
        return sb;
    }

    std::shared_ptr<TPortionInfo> Build();
};

}   // namespace NKikimr::NOlap
