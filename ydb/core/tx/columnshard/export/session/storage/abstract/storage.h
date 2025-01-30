#pragma once
#include <ydb/library/conclusion/status.h>
#include <ydb/library/conclusion/result.h>
#include <ydb/core/tx/columnshard/export/protos/storage.pb.h>
#include <ydb/services/bg_tasks/abstract/interface.h>

namespace NKikimr::NOlap {
class IStoragesManager;
class IBlobsStorageOperator;
}

namespace NKikimrTxColumnShard {
class TBackupTxBody;
}

namespace NKikimr::NOlap::NExport {

class IStorageInitializer {
protected:
    virtual TConclusionStatus DoDeserializeFromProto(const NKikimrColumnShardExportProto::TStorageInitializerContainer& proto) = 0;
    virtual void DoSerializeToProto(NKikimrColumnShardExportProto::TStorageInitializerContainer& proto) const = 0;
    virtual TConclusion<std::shared_ptr<IBlobsStorageOperator>> DoInitializeOperator(const std::shared_ptr<IStoragesManager>& storages) const = 0;
public:
    using TProto = NKikimrColumnShardExportProto::TStorageInitializerContainer;
    using TFactory = NObjectFactory::TObjectFactory<IStorageInitializer, TString>;

    virtual ~IStorageInitializer() = default;
    TConclusionStatus DeserializeFromProto(const NKikimrColumnShardExportProto::TStorageInitializerContainer& proto) {
        return DoDeserializeFromProto(proto);
    }

    TConclusion<std::shared_ptr<IBlobsStorageOperator>> InitializeOperator(const std::shared_ptr<IStoragesManager>& storages) const {
        return DoInitializeOperator(storages);
    }

    void SerializeToProto(NKikimrColumnShardExportProto::TStorageInitializerContainer& proto) const {
        DoSerializeToProto(proto);
    }

    virtual TString GetClassName() const = 0;
};

class TStorageInitializerContainer: public NBackgroundTasks::TInterfaceProtoContainer<IStorageInitializer> {
private:
    using TBase = NBackgroundTasks::TInterfaceProtoContainer<IStorageInitializer>;
public:
    using TBase::TBase;

    static TConclusion<TStorageInitializerContainer> BuildFromProto(const NKikimrColumnShardExportProto::TStorageInitializerContainer& proto) {
        TStorageInitializerContainer result;
        if (!result.DeserializeFromProto(proto)) {
            return TConclusionStatus::Fail("cannot parse proto as TSelectorContainer");
        }
        return result;
    }

    static TConclusion<TStorageInitializerContainer> BuildFromProto(const NKikimrTxColumnShard::TBackupTxBody& proto);

    TString DebugString() const {
        return TBase::SerializeToProto().DebugString();
    }
};
}