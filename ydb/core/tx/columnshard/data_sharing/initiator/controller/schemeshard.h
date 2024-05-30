#pragma once
#include "abstract.h"

namespace NKikimr::NOlap::NDataSharing {

class TSSInitiatorController: public IInitiatorController {
public:
    static TString GetClassNameStatic() {
        return "SS";
    }
private:
//    ui64 TabletId = 0;
//    ui64 ReplyCookie = 0;
    static inline TFactory::TRegistrator<TSSInitiatorController> Registrator = TFactory::TRegistrator<TSSInitiatorController>(GetClassNameStatic());
protected:
    virtual TConclusionStatus DoDeserializeFromProto(const NKikimrColumnShardDataSharingProto::TInitiator::TController& /*proto*/) override {
//        if (!proto.HasSS()) {
//            return TConclusionStatus::Fail("no data about SS initiator");
//        }
//        if (!proto.GetSS().GetTabletId()) {
//            return TConclusionStatus::Fail("incorrect tabletId for SS initiator");
//        }
//        TabletId = proto.GetSS().GetTabletId();
//        ReplyCookie = proto.GetSS().GetReplyCookie();
        return TConclusionStatus::Success();
    }
    virtual void DoSerializeToProto(NKikimrColumnShardDataSharingProto::TInitiator::TController& /*proto*/) const override {
//        AFL_VERIFY(TabletId);
//        proto.MutableSS()->SetTabletId(TabletId);
//        proto.MutableSS()->SetReplyCookie(ReplyCookie);
    }

    virtual void DoStatus(const TStatusContainer& /*status*/) const override {
        
    }
    virtual void DoProposeError(const TString& sessionId, const TString& message) const override;
    virtual void DoProposeSuccess(const TString& /*sessionId*/) const override {
    }
    virtual void DoConfirmSuccess(const TString& /*sessionId*/) const override {
    }
    virtual void DoFinished(const TString& /*sessionId*/) const override {
//        auto ev = std::make_unique<TBlobsTransferFinished>(sessionId);
//        NActors::TActivationContext::AsActorContext().Send(MakePipePerNodeCacheID(false),
//            new TEvPipeCache::TEvForward(ev.release(), (ui64)TabletId, true), IEventHandle::FlagTrackDelivery, ReplyId);
    }
    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
public:
    TSSInitiatorController() = default;
    TSSInitiatorController(const ui64 /*tabletId*/, const ui64 /*replyCookie*/)
//        : TabletId(tabletId)
//        , ReplyCookie(replyCookie)
    {

    }
};

}