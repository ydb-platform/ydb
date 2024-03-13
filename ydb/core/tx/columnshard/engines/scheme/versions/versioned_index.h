#pragma once
#include "abstract_scheme.h"

namespace NKikimr::NOlap {

class TVersionedIndex {
    std::map<TSnapshot, ISnapshotSchema::TPtr> Snapshots;
    std::shared_ptr<arrow::Schema> PrimaryKey;
    std::map<ui64, ISnapshotSchema::TPtr> SnapshotByVersion;
    ui64 LastSchemaVersion = 0;
public:
    ISnapshotSchema::TPtr GetLastCriticalSchema() const {
        return nullptr;
    }

    ISnapshotSchema::TPtr GetLastCriticalSchemaDef(const ISnapshotSchema::TPtr defaultSchema) const {
        auto result = GetLastCriticalSchema();
        return result ? result : defaultSchema;
    }

    TString DebugString() const {
        TStringBuilder sb;
        for (auto&& i : Snapshots) {
            sb << i.first << ":" << i.second->DebugString() << ";";
        }
        return sb;
    }

    ISnapshotSchema::TPtr GetSchema(const ui64 version) const {
        auto it = SnapshotByVersion.find(version);
        return it == SnapshotByVersion.end() ? nullptr : it->second;
    }

    ISnapshotSchema::TPtr GetSchemaVerified(const ui64 version) const {
        auto it = SnapshotByVersion.find(version);
        Y_ABORT_UNLESS(it != SnapshotByVersion.end(), "no schema for version %lu", version);
        return it->second;
    }

    ISnapshotSchema::TPtr GetSchema(const TSnapshot& version) const {
        for (auto it = Snapshots.rbegin(); it != Snapshots.rend(); ++it) {
            if (it->first <= version) {
                return it->second;
            }
        }
        Y_ABORT_UNLESS(!Snapshots.empty());
        Y_ABORT_UNLESS(version.IsZero());
        return Snapshots.begin()->second; // For old compaction logic compatibility
    }

    ISnapshotSchema::TPtr GetLastSchema() const {
        Y_ABORT_UNLESS(!Snapshots.empty());
        return Snapshots.rbegin()->second;
    }

    bool IsEmpty() const {
        return Snapshots.empty();
    }

    const std::shared_ptr<arrow::Schema>& GetPrimaryKey() const noexcept {
        return PrimaryKey;
    }

    void AddIndex(const TSnapshot& snapshot, TIndexInfo&& indexInfo);
};
}
