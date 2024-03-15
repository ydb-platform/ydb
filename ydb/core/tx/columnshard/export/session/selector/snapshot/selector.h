#pragma once
#include <ydb/core/tx/columnshard/export/session/selector/abstract/selector.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>

namespace NKikimr::NOlap::NExport {

class TSnapshotSelector: public ISelector {
public:
    static TString GetClassNameStatic() {
        return "SNAPSHOT";
    }
private:
    TSnapshot Snapshot = TSnapshot::Zero();
    static inline const TFactory::TRegistrator<TSnapshotSelector> Registrator = TFactory::TRegistrator<TSnapshotSelector>(GetClassNameStatic());
public:
    TSnapshotSelector() = default;
    TSnapshotSelector(const TSnapshot& snapshot)
        : Snapshot(snapshot)
    {

    }

    virtual TConclusionStatus DoDeserializeFromProto(const NKikimrColumnShardExportProto::TSelectorContainer& proto) override {
        auto result = Snapshot.DeserializeFromProto(proto.GetSnapshot().GetSnapshot());
        if (!result) {
            return result;
        }
        if (!Snapshot.Valid()) {
            return TConclusionStatus::Fail("invalid snapshot");
        }
        return TConclusionStatus::Success();
    }

    virtual void DoSerializeToProto(NKikimrColumnShardExportProto::TSelectorContainer& proto) const override {
        *proto.MutableSnapshot()->MutableSnapshot() = Snapshot.SerializeToProto();
    }

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};
}