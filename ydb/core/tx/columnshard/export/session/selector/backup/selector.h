#pragma once
#include <ydb/core/tx/columnshard/export/session/selector/abstract/selector.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

namespace NKikimr::NOlap::NExport {

class TBackupSelector: public ISelector {
public:
    static TString GetClassNameStatic() {
        return "BACKUP";
    }
private:
    TSnapshot Snapshot = TSnapshot::Zero();
    TString TableName;
    ui64 TablePathId;
    static inline const TFactory::TRegistrator<TBackupSelector> Registrator = TFactory::TRegistrator<TBackupSelector>(GetClassNameStatic());

    TConclusionStatus Validate() const {
        if (!Snapshot.Valid()) {
            return TConclusionStatus::Fail("invalid snapshot");
        }
        if (!TablePathId) {
            return TConclusionStatus::Fail("invalid path id");
        }
        if (!TableName) {
            return TConclusionStatus::Fail("invalid table name");
        }
        return TConclusionStatus::Success();
    }
protected:
    virtual std::unique_ptr<TEvDataShard::TEvKqpScan> DoBuildRequestInitiator(const TCursor& cursor) const override;

    virtual TConclusionStatus DoDeserializeFromProto(const NKikimrColumnShardExportProto::TSelectorContainer& proto) override {
        auto result = Snapshot.DeserializeFromProto(proto.GetBackup().GetSnapshot());
        if (!result) {
            return result;
        }
        TableName = proto.GetBackup().GetTableName();
        TablePathId = proto.GetBackup().GetTablePathId();
        return Validate();
    }

    virtual void DoSerializeToProto(NKikimrColumnShardExportProto::TSelectorContainer& proto) const override {
        *proto.MutableBackup()->MutableSnapshot() = Snapshot.SerializeToProto();
        proto.MutableBackup()->SetTablePathId(TablePathId);
        proto.MutableBackup()->SetTableName(TableName);
    }

    TConclusionStatus DeserializeFromProto(const NKikimrSchemeOp::TBackupTask& proto) {
        Snapshot = TSnapshot(proto.GetSnapshotStep(), proto.GetSnapshotTxId());
        TableName = proto.GetTableName();
        TablePathId = proto.GetTableId();
        return Validate();
    }
public:
    TBackupSelector() = default;
    TBackupSelector(const TSnapshot& snapshot)
        : Snapshot(snapshot) {

    }

    virtual ui64 GetPathId() const override {
        return TablePathId;
    }

    static TConclusion<TBackupSelector> BuildFromProto(const NKikimrSchemeOp::TBackupTask& proto) {
        TBackupSelector result;
        auto parsed = result.DeserializeFromProto(proto);
        if (!parsed) {
            return parsed;
        }
        return result;
    }

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};
}