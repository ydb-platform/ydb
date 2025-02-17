#pragma once
#include "status_channel.h"

#include <ydb/core/tx/columnshard/common/tablet_id.h>
#include <ydb/services/bg_tasks/abstract/interface.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/conclusion/status.h>
#include <ydb/library/conclusion/result.h>

namespace Ydb::Operations {
class Operation;
}

namespace NKikimr::NOlap::NBackground {

class TSession;
class ITabletAdapter;

class TStartContext {
private:
    YDB_READONLY_DEF(std::shared_ptr<TSession>, SessionSelfPtr);
    YDB_READONLY_DEF(std::shared_ptr<ITabletAdapter>, Adapter);
public:
    TStartContext(const std::shared_ptr<TSession>& sessionSelfPtr,
        const std::shared_ptr<ITabletAdapter>& adapter)
        : SessionSelfPtr(sessionSelfPtr)
        , Adapter(adapter)
    {
        AFL_VERIFY(!!SessionSelfPtr);
        AFL_VERIFY(!!Adapter);
    }
};

class ISessionLogic {
private:
    mutable bool ActorConstructed = false;
    virtual TConclusionStatus DoDeserializeProgressFromString(const TString& data) = 0;
    virtual TConclusionStatus DoDeserializeStateFromString(const TString& data) = 0;
    virtual TConclusionStatus DoDeserializeFromString(const TString& data) = 0;
    virtual TString DoSerializeProgressToString() const = 0;
    virtual TString DoSerializeStateToString() const = 0;
    virtual TString DoSerializeToString() const = 0;
    virtual TConclusion<std::unique_ptr<NActors::IActor>> DoCreateActor(const TStartContext& context) const = 0;
public:
    using TFactory = NObjectFactory::TObjectFactory<ISessionLogic, TString>;

    virtual ~ISessionLogic() = default;

    virtual TString GetClassName() const = 0;

    void CheckStatusCorrect() const {
    }

    TConclusionStatus DeserializeProgressFromString(const TString& data) {
        return DoDeserializeProgressFromString(data);
    }
    TString SerializeProgressToString() const {
        CheckStatusCorrect();
        return DoSerializeProgressToString();
    }
    TConclusionStatus DeserializeFromString(const TString& data) {
        return DoDeserializeFromString(data);
    }
    TString SerializeToString() const {
        CheckStatusCorrect();
        return DoSerializeToString();
    }
    TConclusionStatus DeserializeStateFromString(const TString& data) {
        return DoDeserializeStateFromString(data);
    }
    TString SerializeStateToString() const {
        CheckStatusCorrect();
        return DoSerializeStateToString();
    }

    std::unique_ptr<NActors::IActor> CreateActor(const TStartContext& context) const {
        AFL_VERIFY(IsReadyForStart());
        AFL_VERIFY(!IsFinished());
        AFL_VERIFY(!ActorConstructed);
        ActorConstructed = true;
        std::unique_ptr<NActors::IActor> result = DoCreateActor(context).DetachResult();
        AFL_VERIFY(!!result);
        return result;
    }

    virtual bool IsReadyForStart() const = 0;
    virtual bool IsFinished() const = 0;
    virtual bool IsReadyForRemoveOnFinished() const = 0;
};

template <class TProtoLogicExt, class TProtoProgressExt, class TProtoStateExt>
class TSessionProtoAdapter: public NBackgroundTasks::TInterfaceProtoAdapter<TProtoLogicExt, ISessionLogic> {
protected:
    using TProtoProgress = TProtoProgressExt;
    using TProtoState = TProtoStateExt;
    using TProtoLogic = TProtoLogicExt;
private:
    virtual TConclusionStatus DoDeserializeProgressFromProto(const TProtoProgress& proto) = 0;
    virtual TProtoProgress DoSerializeProgressToProto() const = 0;
    virtual TConclusionStatus DoDeserializeStateFromProto(const TProtoState& proto) = 0;
    virtual TProtoState DoSerializeStateToProto() const = 0;
protected:
    virtual TConclusionStatus DoDeserializeProgressFromString(const TString& data) override final {
        TProtoProgress proto;
        if (!proto.ParseFromArray(data.data(), data.size())) {
            return TConclusionStatus::Fail("cannot parse proto string as " + TypeName<TProtoProgress>());
        }
        return DoDeserializeProgressFromProto(proto);
    }
    virtual TString DoSerializeProgressToString() const override final {
        TProtoProgress proto = DoSerializeProgressToProto();
        return proto.SerializeAsString();
    }
    virtual TConclusionStatus DoDeserializeStateFromString(const TString& data) override final {
        TProtoState proto;
        if (!proto.ParseFromArray(data.data(), data.size())) {
            return TConclusionStatus::Fail("cannot parse proto string as " + TypeName<TProtoState>());
        }
        return DoDeserializeStateFromProto(proto);
    }
    virtual TString DoSerializeStateToString() const override final {
        TProtoState proto = DoSerializeStateToProto();
        return proto.SerializeAsString();
    }
};

class TSessionLogicContainer: public NBackgroundTasks::TInterfaceStringContainer<ISessionLogic> {
private:
    using TBase = NBackgroundTasks::TInterfaceStringContainer<ISessionLogic>;
public:
    using TBase::TBase;
};

class TSessionRecord {
private:
    YDB_ACCESSOR_DEF(TString, Identifier);
    YDB_ACCESSOR_DEF(TString, ClassName);
    YDB_ACCESSOR_DEF(TString, LogicDescription);
    YDB_ACCESSOR_DEF(TString, StatusChannel);
    YDB_ACCESSOR_DEF(TString, Progress);
    YDB_ACCESSOR_DEF(TString, State);
public:
};

class TSessionInfoReport {
private:
    YDB_READONLY_DEF(TString, Identifier);
    YDB_READONLY_DEF(TString, ClassName);
    YDB_READONLY(bool, IsFinished, false);
public:
    Ydb::Operations::Operation SerializeToProto() const;

    TSessionInfoReport(const TString& id, const TString& className, const bool isFinished)
        : Identifier(id)
        , ClassName(className)
        , IsFinished(isFinished) {

    }
};

}