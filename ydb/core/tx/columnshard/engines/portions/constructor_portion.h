#pragma once
#include "compacted.h"
#include "constructor_meta.h"
#include "written.h"

#include <ydb/core/tx/columnshard/common/path_id.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/abstract_scheme.h>

#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NOlap {

class TColumnChunkLoadContextV1;
class TIndexChunkLoadContext;
class TPortionAccessorConstructor;

class TPortionInfoConstructor {
protected:
    bool Constructed = false;
    YDB_ACCESSOR_DEF(TInternalPathId, PathId);
    std::optional<ui64> PortionId;

    TPortionMetaConstructor MetaConstructor;

    std::optional<TSnapshot> RemoveSnapshot;
    std::optional<ui64> SchemaVersion;
    std::optional<ui64> ShardingVersion;

    TPortionInfoConstructor(const TPortionInfoConstructor&) = default;
    TPortionInfoConstructor& operator=(const TPortionInfoConstructor&) = default;

    TPortionInfoConstructor(TPortionInfo&& portion)
        : PathId(portion.GetPathId())
        , PortionId(portion.GetPortionId())
        , RemoveSnapshot(portion.GetRemoveSnapshotOptional())
        , SchemaVersion(portion.GetSchemaVersionVerified())
        , ShardingVersion(portion.GetShardingVersionOptional()) {
        MetaConstructor = TPortionMetaConstructor(std::move(portion.Meta));
    }

    virtual std::shared_ptr<TPortionInfo> BuildPortionImpl(TPortionMeta&& meta) = 0;

    friend class TPortionAccessorConstructor;

public:
    TPortionInfoConstructor(TPortionInfoConstructor&&) noexcept = default;
    TPortionInfoConstructor& operator=(TPortionInfoConstructor&&) noexcept = default;

    virtual ~TPortionInfoConstructor() = default;

    virtual EPortionType GetType() const = 0;

    TPortionInfoConstructor(const TPortionInfo& portion, const bool withMetadata)
        : PathId(portion.GetPathId())
        , PortionId(portion.GetPortionId())
        , RemoveSnapshot(portion.GetRemoveSnapshotOptional())
        , SchemaVersion(portion.GetSchemaVersionVerified())
        , ShardingVersion(portion.GetShardingVersionOptional()) {
        if (withMetadata) {
            MetaConstructor = TPortionMetaConstructor(portion.Meta);
        }
    }

    void SetPortionId(const ui64 value) {
        AFL_VERIFY(value);
        PortionId = value;
    }

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

    TPortionAddress GetAddress() const {
        return TPortionAddress(PathId, GetPortionIdVerified());
    }

    bool HasRemoveSnapshot() const {
        return !!RemoveSnapshot;
    }

    TPortionInfoConstructor(const TInternalPathId pathId, const ui64 portionId)
        : PathId(pathId)
        , PortionId(portionId) {
        AFL_VERIFY(PathId);
        AFL_VERIFY(PortionId);
    }

    TPortionInfoConstructor(const TInternalPathId pathId)
        : PathId(pathId) {
        AFL_VERIFY(PathId);
    }

    std::shared_ptr<ISnapshotSchema> GetSchema(const TVersionedIndex& index) const;

    void SetSchemaVersion(const ui64 version) {
        AFL_VERIFY(version);
        SchemaVersion = version;
    }

    ui64 GetSchemaVersionVerified() const {
        AFL_VERIFY(SchemaVersion);
        return *SchemaVersion;
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

class TCompactedPortionInfoConstructor: public TPortionInfoConstructor {
private:
    using TBase = TPortionInfoConstructor;
    std::optional<TSnapshot> AppearanceSnapshot;

public:
    using TBase::TBase;

    void SetAppearanceSnapshot(const TSnapshot snapshot) {
        AFL_VERIFY(!AppearanceSnapshot);
        AppearanceSnapshot = snapshot;
    }

    TCompactedPortionInfoConstructor(const TCompactedPortionInfo& portion, const bool withMetadata)
        : TBase(portion, withMetadata)
        , AppearanceSnapshot(portion.AppearanceSnapshot) {
    }

    virtual EPortionType GetType() const override {
        return EPortionType::Compacted;
    }

    virtual std::shared_ptr<TPortionInfo> BuildPortionImpl(TPortionMeta&& meta) override;
};

class TWrittenPortionInfoConstructor: public TPortionInfoConstructor {
private:
    using TBase = TPortionInfoConstructor;
    std::optional<TSnapshot> CommitSnapshot;
    std::optional<TInsertWriteId> InsertWriteId;

    virtual std::shared_ptr<TPortionInfo> BuildPortionImpl(TPortionMeta&& meta) override;

public:
    using TBase::TBase;

    virtual EPortionType GetType() const override {
        return EPortionType::Written;
    }

    TWrittenPortionInfoConstructor(const TWrittenPortionInfo& portion, const bool withMetadata)
        : TBase(portion, withMetadata)
        , CommitSnapshot(portion.GetCommitSnapshotOptional())
        , InsertWriteId(portion.GetInsertWriteId()) {
    }

    TInsertWriteId GetInsertWriteIdVerified() const {
        AFL_VERIFY(InsertWriteId);
        return *InsertWriteId;
    }

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
};

}   // namespace NKikimr::NOlap
