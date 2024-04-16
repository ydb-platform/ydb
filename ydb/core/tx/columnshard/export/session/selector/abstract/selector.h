#pragma once
#include <ydb/library/conclusion/status.h>
#include <ydb/library/conclusion/result.h>
#include <ydb/core/tx/columnshard/export/protos/selector.pb.h>
#include <ydb/services/bg_tasks/abstract/interface.h>
#include <ydb/core/tx/columnshard/export/session/cursor.h>
#include <ydb/core/tx/datashard/datashard.h>

namespace NKikimrTxColumnShard {
class TBackupTxBody;
}

namespace NKikimr::NOlap::NExport {

class ISelector {
protected:
    virtual TConclusionStatus DoDeserializeFromProto(const NKikimrColumnShardExportProto::TSelectorContainer& proto) = 0;
    virtual void DoSerializeToProto(NKikimrColumnShardExportProto::TSelectorContainer& proto) const = 0;
    virtual std::unique_ptr<TEvDataShard::TEvKqpScan> DoBuildRequestInitiator(const TCursor& cursor) const = 0;

public:
    using TProto = NKikimrColumnShardExportProto::TSelectorContainer;
    using TFactory = NObjectFactory::TObjectFactory<ISelector, TString>;

    virtual ~ISelector() = default;
    TConclusionStatus DeserializeFromProto(const NKikimrColumnShardExportProto::TSelectorContainer& proto) {
        return DoDeserializeFromProto(proto);
    }

    std::unique_ptr<TEvDataShard::TEvKqpScan> BuildRequestInitiator(const TCursor& cursor) const {
        return DoBuildRequestInitiator(cursor);
    }

    void SerializeToProto(NKikimrColumnShardExportProto::TSelectorContainer& proto) const {
        DoSerializeToProto(proto);
    }

    virtual ui64 GetPathId() const = 0;
    virtual TString GetClassName() const = 0;
};

class TSelectorContainer: public NBackgroundTasks::TInterfaceProtoContainer<ISelector> {
private:
    using TBase = NBackgroundTasks::TInterfaceProtoContainer<ISelector>;
public:
    using TBase::TBase;

    static TConclusion<TSelectorContainer> BuildFromProto(const NKikimrColumnShardExportProto::TSelectorContainer& proto) {
        TSelectorContainer result;
        if (!result.DeserializeFromProto(proto)) {
            return TConclusionStatus::Fail("cannot parse proto as TSelectorContainer");
        }
        return result;
    }

    static TConclusion<TSelectorContainer> BuildFromProto(const NKikimrTxColumnShard::TBackupTxBody& proto);

    TString DebugString() const {
        return TBase::SerializeToProto().DebugString();
    }
};
}