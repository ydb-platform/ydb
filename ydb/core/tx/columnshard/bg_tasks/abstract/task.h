#pragma once
#include "session.h"
#include <ydb/core/tx/columnshard/bg_tasks/protos/data.pb.h>
#include <ydb/services/bg_tasks/abstract/interface.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NOlap::NBackground {

class ITaskDescription {
private:
    virtual TConclusionStatus DoDeserializeFromString(const TString& data) = 0;
    virtual TString DoSerializeToString() const = 0;
    virtual std::shared_ptr<ISessionLogic> DoBuildSession() const = 0;
public:
    using TFactory = NObjectFactory::TObjectFactory<ITaskDescription, TString>;

    virtual ~ITaskDescription() = default;

    virtual TString GetClassName() const = 0;

    TConclusionStatus DeserializeFromString(const TString& data) {
        return DoDeserializeFromString(data);
    }

    TString SerializeToString() const {
        return DoSerializeToString();
    }

    TConclusion<std::shared_ptr<ISessionLogic>> BuildSessionLogic() const {
        return DoBuildSession();
    }
};

class TTaskDescriptionContainer: public NBackgroundTasks::TInterfaceStringContainer<ITaskDescription> {
private:
    using TBase = NBackgroundTasks::TInterfaceStringContainer<ITaskDescription>;
public:
    using TBase::TBase;
};

class TTask {
private:
    YDB_READONLY_DEF(TString, Identifier);
    YDB_READONLY_DEF(TStatusChannelContainer, ChannelContainer);
    YDB_READONLY_DEF(TTaskDescriptionContainer, DescriptionContainer);
public:
    TTask() = default;
    TTask(const TString& identifier, const TStatusChannelContainer& channelContainer, const TTaskDescriptionContainer& descriptionContainer)
        : Identifier(identifier)
        , ChannelContainer(channelContainer)
        , DescriptionContainer(descriptionContainer)
    {
        AFL_VERIFY(!!Identifier);
        AFL_VERIFY(!!ChannelContainer);
        AFL_VERIFY(!!DescriptionContainer);
    }
    NKikimrTxBackgroundProto::TTaskContainer SerializeToProto() const {
        NKikimrTxBackgroundProto::TTaskContainer result;
        result.SetIdentifier(Identifier);
        result.SetStatusChannelContainer(ChannelContainer.SerializeToString());
        result.SetTaskDescriptionContainer(DescriptionContainer.SerializeToString());
        return result;
    }
    TConclusionStatus DeserializeFromProto(const NKikimrTxBackgroundProto::TTaskContainer& proto) {
        Identifier = proto.GetIdentifier();
        if (!Identifier) {
            return TConclusionStatus::Fail("empty identifier is not correct for bg_task");
        }
        if (!ChannelContainer.DeserializeFromString(proto.GetStatusChannelContainer())) {
            return TConclusionStatus::Fail("cannot parse status channel from proto");
        }
        if (!DescriptionContainer.DeserializeFromString(proto.GetTaskDescriptionContainer())) {
            return TConclusionStatus::Fail("cannot parse task description from proto");
        }
        return TConclusionStatus::Success();
    }
};

}