#pragma once
#include <ydb/services/bg_tasks/abstract/interface.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/conclusion/status.h>

namespace NKikimr::NOlap::NBackground {

class IStatusChannel {
private:
    virtual void DoOnFail(const TString& errorMessage) const = 0;
    virtual void DoOnAdded() const = 0;
    virtual void DoOnFinished() const = 0;
    virtual TString DoSerializeToString() const = 0;
    virtual TConclusionStatus DoDeserializeFromString(const TString& data) = 0;
public:
    virtual ~IStatusChannel() = default;

    using TFactory = NObjectFactory::TObjectFactory<IStatusChannel, TString>;

    virtual TString GetClassName() const = 0;

    TString SerializeToString() const {
        return DoSerializeToString();
    }
    TConclusionStatus DeserializeFromString(const TString& data) {
        return DoDeserializeFromString(data);
    }

    void OnFail(const TString& errorMessage) const {
        AFL_ERROR(NKikimrServices::TX_BACKGROUND)("problem", "fail_on_background_task")("reason", errorMessage);
        DoOnFail(errorMessage);
    }
    void OnAdded() const {
        AFL_INFO(NKikimrServices::TX_BACKGROUND)("info", "background task added");
        DoOnAdded();
    }
    void OnFinished() const {
        AFL_INFO(NKikimrServices::TX_BACKGROUND)("info", "background task finished");
        DoOnFinished();
    }
};

class TFakeStatusChannel: public IStatusChannel {
public:
    static TString GetClassNameStatic() {
        return "FAKE";
    }
private:
    static const inline TFactory::TRegistrator<TFakeStatusChannel> Registrator = TFactory::TRegistrator<TFakeStatusChannel>(GetClassNameStatic());
    virtual void DoOnFail(const TString& /*errorMessage*/) const override {

    }
    virtual void DoOnAdded() const override {

    }
    virtual void DoOnFinished() const override {

    }
    virtual TString DoSerializeToString() const override {
        return "";
    }
    virtual TConclusionStatus DoDeserializeFromString(const TString& /*data*/) override {
        return TConclusionStatus::Success();
    }
public:
    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

class TStatusChannelContainer: public NBackgroundTasks::TInterfaceStringContainer<IStatusChannel> {
private:
    using TBase = NBackgroundTasks::TInterfaceStringContainer<IStatusChannel>;
public:
    using TBase::TBase;
    bool DeserializeFromString(const TString& data) {
        if (!TBase::DeserializeFromString(data)) {
            Initialize(TFakeStatusChannel::GetClassNameStatic());
            return false;
        }
        return true;
    }
};

}