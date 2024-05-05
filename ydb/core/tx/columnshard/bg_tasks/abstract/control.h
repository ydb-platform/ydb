#pragma once
#include "session.h"
#include "status_channel.h"
#include <ydb/core/tx/columnshard/bg_tasks/protos/data.pb.h>
#include <ydb/services/bg_tasks/abstract/interface.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NOlap::NBackground {

class ISessionLogicControl {
private:
    YDB_READONLY_DEF(TString, Identifier);
    virtual TConclusionStatus DoApply(const std::shared_ptr<ISessionLogic>& session) const = 0;
    virtual TConclusionStatus DoDeserializeFromString(const TString& data) = 0;
    virtual TString DoSerializeToString() const = 0;
public:
    using TFactory = NObjectFactory::TObjectFactory<ISessionLogicControl, TString>;

    virtual ~ISessionLogicControl() = default;

    TConclusionStatus DeserializeFromString(const TString& data) {
        return DoDeserializeFromString(data);
    }

    TString SerializeToString() const {
        return DoSerializeToString();
    }

    TConclusionStatus Apply(const std::shared_ptr<ISessionLogic>& session) const {
        session->CheckStatusCorrect();
        auto result = DoApply(session);
        session->CheckStatusCorrect();
        return result;
    }

    virtual TString GetClassName() const = 0;
};

class TSessionLogicControlContainer: public NBackgroundTasks::TInterfaceStringContainer<ISessionLogicControl> {
private:
    using TBase = NBackgroundTasks::TInterfaceStringContainer<ISessionLogicControl>;
public:
    using TBase::TBase;
};

class TSessionControlContainer {
private:
    YDB_READONLY_DEF(TString, SessionClassName);
    YDB_READONLY_DEF(TString, SessionIdentifier);
    YDB_READONLY_DEF(TStatusChannelContainer, ChannelContainer);
    YDB_READONLY_DEF(TSessionLogicControlContainer, LogicControlContainer);
public:
    NKikimrTxBackgroundProto::TSessionControlContainer SerializeToProto() const {
        NKikimrTxBackgroundProto::TSessionControlContainer result;
        result.SetSessionClassName(SessionClassName);
        result.SetSessionIdentifier(SessionIdentifier);
        result.SetStatusChannelContainer(ChannelContainer.SerializeToString());
        result.SetLogicControlContainer(LogicControlContainer.SerializeToString());
        return result;
    }
    TConclusionStatus DeserializeFromProto(const NKikimrTxBackgroundProto::TSessionControlContainer& proto) {
        SessionClassName = proto.GetSessionClassName();
        SessionIdentifier = proto.GetSessionIdentifier();
        if (!SessionClassName) {
            return TConclusionStatus::Fail("incorrect session class name for bg_task");
        }
        if (!SessionIdentifier) {
            return TConclusionStatus::Fail("incorrect session id for bg_task");
        }
        if (!ChannelContainer.DeserializeFromString(proto.GetStatusChannelContainer())) {
            return TConclusionStatus::Fail("cannot parse channel from proto");
        }
        if (!LogicControlContainer.DeserializeFromString(proto.GetLogicControlContainer())) {
            return TConclusionStatus::Fail("cannot parse logic from proto");
        }
        return TConclusionStatus::Success();
    }

    TSessionControlContainer() = default;

    TSessionControlContainer(const TStatusChannelContainer& channel, const TSessionLogicControlContainer& logic)
        : ChannelContainer(channel)
        , LogicControlContainer(logic) {
        AFL_VERIFY(!!ChannelContainer);
        AFL_VERIFY(!!LogicControlContainer);
    }
};

}