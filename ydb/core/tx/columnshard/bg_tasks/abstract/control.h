#pragma once
#include "session.h"
#include "status_channel.h"
#include <ydb/services/bg_tasks/abstract/interface.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimrTxBackgroundProto {
class TSessionControlContainer;
class TSessionLogicControlContainer;
}

namespace NKikimr::NOlap::NBackground {

class ISessionLogicControl {
private:
    YDB_READONLY_DEF(TString, SessionClassName);
    YDB_READONLY_DEF(TString, SessionIdentifier);
    virtual TConclusionStatus DoApply(const std::shared_ptr<ISessionLogic>& session) const = 0;

    virtual TConclusionStatus DoDeserializeFromString(const TString& data) = 0;
    virtual TString DoSerializeToString() const = 0;
protected:
    TConclusionStatus DeserializeFromString(const TString& data) {
        return DoDeserializeFromString(data);
    }
    TString SerializeToString() const {
        return DoSerializeToString();
    }
public:
    using TProto = NKikimrTxBackgroundProto::TSessionLogicControlContainer;
    using TFactory = NObjectFactory::TObjectFactory<ISessionLogicControl, TString>;

    virtual ~ISessionLogicControl() = default;
    ISessionLogicControl() = default;
    ISessionLogicControl(const TString& sessionClassName, const TString& sessionIdentifier)
        : SessionClassName(sessionClassName)
        , SessionIdentifier(sessionIdentifier)
    {

    }

    TConclusionStatus DeserializeFromProto(const TProto& data);
    void SerializeToProto(TProto& proto) const;

    TConclusionStatus Apply(const std::shared_ptr<ISessionLogic>& session) const {
        session->CheckStatusCorrect();
        auto result = DoApply(session);
        session->CheckStatusCorrect();
        return result;
    }

    virtual TString GetClassName() const = 0;
};

class TSessionLogicControlContainer: public NBackgroundTasks::TInterfaceProtoContainer<ISessionLogicControl> {
private:
    using TBase = NBackgroundTasks::TInterfaceProtoContainer<ISessionLogicControl>;
public:
    using TBase::TBase;
};

class TSessionControlContainer {
private:
    YDB_READONLY_DEF(TStatusChannelContainer, ChannelContainer);
    YDB_READONLY_DEF(TSessionLogicControlContainer, LogicControlContainer);
public:
    NKikimrTxBackgroundProto::TSessionControlContainer SerializeToProto() const;
    TConclusionStatus DeserializeFromProto(const NKikimrTxBackgroundProto::TSessionControlContainer& proto);

    TSessionControlContainer() = default;

    TSessionControlContainer(const TStatusChannelContainer& channel, const TSessionLogicControlContainer& logic)
        : ChannelContainer(channel)
        , LogicControlContainer(logic) {
        AFL_VERIFY(!!ChannelContainer);
        AFL_VERIFY(!!LogicControlContainer);
    }
};

}