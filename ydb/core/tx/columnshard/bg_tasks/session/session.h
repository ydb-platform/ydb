#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/tx/columnshard/bg_tasks/abstract/status_channel.h>
#include <ydb/core/tx/columnshard/bg_tasks/abstract/session.h>

namespace NKikimr::NOlap::NBackground {

class TSession {
private:
    YDB_READONLY_DEF(TString, Identifier);
    YDB_READONLY_DEF(std::optional<NActors::TActorId>, ActorId);
    YDB_READONLY_DEF(TStatusChannelContainer, ChannelContainer);
    YDB_READONLY_DEF(TSessionLogicContainer, LogicContainer);
public:
    TSessionInfoReport GetSessionInfoForReport() const {
        return TSessionInfoReport(Identifier, LogicContainer.GetClassName(), LogicContainer->IsFinished());
    }

    TSessionRecord SerializeToLocalDatabaseRecord() const {
        TSessionRecord result;
        result.SetIdentifier(Identifier);
        result.SetClassName(LogicContainer.GetClassName());
        result.SetLogicDescription(LogicContainer.SerializeToString());
        result.SetProgress(LogicContainer->SerializeProgressToString());
        result.SetState(LogicContainer->SerializeStateToString());
        result.SetStatusChannel(ChannelContainer.SerializeToString());
        return result;
    }

    template <class T>
    std::shared_ptr<T> GetLogicAsVerifiedPtr() const {
        return LogicContainer.GetObjectPtrVerifiedAs<T>();
    }

    [[nodiscard]] TConclusionStatus DeserializeFromLocalDatabase(TSessionRecord&& record) {
        Identifier = record.GetIdentifier();
        ChannelContainer.DeserializeFromString(record.GetStatusChannel());
        if (!LogicContainer.DeserializeFromString(record.GetLogicDescription())) {
            return TConclusionStatus::Fail("cannot parse logic description");
        }
        if (!LogicContainer->DeserializeProgressFromString(record.GetProgress())) {
            return TConclusionStatus::Fail("cannot parse progress");
        }
        if (!LogicContainer->DeserializeStateFromString(record.GetState())) {
            return TConclusionStatus::Fail("cannot parse state");
        }
        return TConclusionStatus::Success();
    }

    TString GetLogicClassName() const {
        return LogicContainer.GetClassName();
    }

    bool IsRunning() const {
        return !!ActorId;
    }

    const NActors::TActorId& GetActorIdVerified() const {
        AFL_VERIFY(!!ActorId);
        return *ActorId;
    }

    void StartActor(const TStartContext& startContext) {
        AFL_VERIFY(!ActorId);
        std::unique_ptr<NActors::IActor> actor = LogicContainer->CreateActor(startContext);
        ActorId = NActors::TActivationContext::AsActorContext().Register(actor.release());
    }

    void FinishActor() {
        AFL_VERIFY(!!ActorId);
        NActors::TActivationContext::AsActorContext().Send(*ActorId, new NActors::TEvents::TEvPoisonPill);
        ActorId.reset();
    }

    TSession() = default;

    TSession(const TString& identifier, const TStatusChannelContainer& channel, const TSessionLogicContainer& logic)
        : Identifier(identifier)
        , ChannelContainer(channel)
        , LogicContainer(logic)
    {
        AFL_VERIFY(!!Identifier);
        AFL_VERIFY(!!ChannelContainer);
        AFL_VERIFY(!!LogicContainer);
    }
};

}