#pragma once
#include "common.h"
#include <ydb/core/tx/schemeshard/olap/bg_tasks/protos/data.pb.h>
#include <ydb/core/tx/columnshard/bg_tasks/protos/data.pb.h>
#include <ydb/core/tx/columnshard/bg_tasks/abstract/task.h>
#include <ydb/core/tx/columnshard/bg_tasks/abstract/session.h>
#include <ydb/services/bg_tasks/abstract/interface.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NSchemeShard::NOlap::NBackground {

class TTxChainTask: public NBackgroundTasks::TInterfaceProtoAdapter<NKikimrSchemeShardTxBackgroundProto::TTxChainTask, NKikimr::NOlap::NBackground::ITaskDescription> {
private:
    TTxChainData TxData;
    using TBase = NBackgroundTasks::TInterfaceProtoAdapter<TProtoStorage, NKikimr::NOlap::NBackground::ITaskDescription>;
public:
    static TString GetStaticClassName() {
        return "SS::BG::TX_CHAIN";
    }
private:
    virtual TConclusionStatus DoDeserializeFromProto(const TProtoStorage& proto) override {
        return TxData.DeserializeFromProto(proto.GetCommonData());
    }
    virtual TProtoStorage DoSerializeToProto() const override {
        TProtoStorage result;
        *result.MutableCommonData() = TxData.SerializeToProto();
        return result;
    }
    virtual std::shared_ptr<NKikimr::NOlap::NBackground::ISessionLogic> DoBuildSession() const override;
    static const inline TFactory::TRegistrator<TTxChainTask> Registrator = TFactory::TRegistrator<TTxChainTask>(GetStaticClassName());
public:
    TTxChainTask() = default;

    TTxChainTask(const TTxChainData& data)
        : TxData(data)
    {

    }
    virtual TString GetClassName() const override {
        return GetStaticClassName();
    }
};

}