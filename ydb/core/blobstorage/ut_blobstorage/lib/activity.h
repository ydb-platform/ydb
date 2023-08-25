#pragma once

#include "defs.h"

enum {
    EvFinished = EventSpaceBegin(TEvents::ES_PRIVATE),
};

class TActivityActor : public TActorBootstrapped<TActivityActor> {
    NUnitTest::TTestBase *Test = NUnitTest::NPrivate::GetCurrentTest();
    const ui64 TabletId;
    const ui32 GroupId;
    const TActorId Edge;
    const TString Prefix;
    ui32 Generation = 1;
    ui32 Step = 1;
    std::map<TLogoBlobID, TString> Committed;
    std::map<TLogoBlobID, TString> Inflight;
    ui32 ReadsInFlight = 0;
    ui32 NumWritesRemaining = 0;
    ui32 MaxWritesInFlight;
    TReplQuoter Quoter;

public:
    TActivityActor(ui64 tabletId, ui32 groupId, const TActorId& edge)
        : TabletId(tabletId)
        , GroupId(groupId)
        , Edge(edge)
        , Prefix(TStringBuilder() << "[" << TabletId << "@" << GroupId << "] ")
        , Quoter(10)
    {}

    void SendToProxy(TDuration timeout, IEventBase *ev) {
        TActivationContext::Schedule(timeout, CreateEventForBSProxy(SelfId(), MakeBlobStorageProxyID(GroupId), ev, 0));
    }

    void Bootstrap() {
        IssueInitialCheck();
        Become(&TThis::StateFunc);
    }

    void IssueInitialCheck() {
        LOG_NOTICE_S(*TlsActivationContext, NActorsServices::TEST, Prefix << "starting new cycle Generation# " << Generation);
        NumWritesRemaining = 15 + RandomNumber<size_t>(10);
        MaxWritesInFlight = 1 + RandomNumber<size_t>(5);
        SendToProxy(TDuration::MilliSeconds(100 + RandomNumber(100u)), new TEvBlobStorage::TEvStatus(TInstant::Max()));
    }

    void Handle(TEvBlobStorage::TEvStatusResult::TPtr ev) {
        LOG_DEBUG_S(*TlsActivationContext, NActorsServices::TEST, Prefix << "got TEvStatusResult# " << ev->Get()->ToString());
        if (ev->Get()->Status != NKikimrProto::OK) {
            IssueInitialCheck();
        } else {
            IssueBlock();
        }
    }

    void IssueBlock() {
        if (Generation == 1) {
            IssueDiscover();
        } else {
            SendToProxy(TDuration::MilliSeconds(100), new TEvBlobStorage::TEvBlock(TabletId, Generation - 1, TInstant::Max()));
        }
    }

    void Handle(TEvBlobStorage::TEvBlockResult::TPtr ev) {
        LOG_DEBUG_S(*TlsActivationContext, NActorsServices::TEST, Prefix << "got TEvBlockResult# " << ev->Get()->ToString());
        UNIT_ASSERT_C(ev->Get()->Status == NKikimrProto::OK || ev->Get()->Status == NKikimrProto::ALREADY ||
            ev->Get()->Status == NKikimrProto::RACE, ev->Get()->Print(false));
        if (ev->Get()->Status == NKikimrProto::OK || ev->Get()->Status == NKikimrProto::ALREADY) {
            IssueDiscover();
        } else {
            IssueBlock();
        }
    }

    void IssueDiscover() {
        SendToProxy(TDuration::MilliSeconds(100), new TEvBlobStorage::TEvDiscover(TabletId, 0, false, false, TInstant::Max(), 0, true));
    }

    void Handle(TEvBlobStorage::TEvDiscoverResult::TPtr ev) {
        LOG_DEBUG_S(*TlsActivationContext, NActorsServices::TEST, Prefix << "got TEvDiscoverResult# " << ev->Get()->ToString());
        if (Generation == 1) {
            UNIT_ASSERT_VALUES_EQUAL_C(ev->Get()->Status, NKikimrProto::NODATA, ev->Get()->Print(false));
        } else {
            UNIT_ASSERT_VALUES_EQUAL_C(ev->Get()->Status, NKikimrProto::OK, ev->Get()->Print(false));
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Id, Committed.rbegin()->first);
        }
        IssueRangeRead();
    }

    void IssueRangeRead() {
        const TLogoBlobID from(TabletId, Generation - 1, 0, 0, 0, 0);
        const TLogoBlobID to(TabletId, Generation - 1, Max<ui32>(), 0, TLogoBlobID::MaxBlobSize, TLogoBlobID::MaxCookie);
        auto msg = std::make_unique<TEvBlobStorage::TEvRange>(TabletId, from, to, true, TInstant::Max(), true);
        LOG_DEBUG_S(*TlsActivationContext, NActorsServices::TEST, Prefix << "sending TEvRange# " << msg->ToString());
        SendToProxy(TDuration::MilliSeconds(100), msg.release());
    }

    void Handle(TEvBlobStorage::TEvRangeResult::TPtr ev) {
        LOG_DEBUG_S(*TlsActivationContext, NActorsServices::TEST, Prefix << "got TEvRangeResult# " << ev->Get()->ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(ev->Get()->Status, NKikimrProto::OK, ev->Get()->Print(false));
        TDuration timeout = TDuration::MilliSeconds(100);
        for (const auto& item : ev->Get()->Responses) {
            SendToProxy(timeout, new TEvBlobStorage::TEvGet(item.Id, 0, 0, TInstant::Max(),
                NKikimrBlobStorage::EGetHandleClass::FastRead, true));
            timeout += TDuration::MilliSeconds(10);
            ++ReadsInFlight;
        }
        if (!ReadsInFlight) {
            UNIT_ASSERT(Committed.empty());
            IssueCollectGarbage();
        }
    }

    void Handle(TEvBlobStorage::TEvGetResult::TPtr ev) {
        LOG_DEBUG_S(*TlsActivationContext, NActorsServices::TEST, Prefix << "got TEvGetResult# " << ev->Get()->ToString());
        auto *msg = ev->Get();
        UNIT_ASSERT_VALUES_EQUAL_C(ev->Get()->Status, NKikimrProto::OK, ev->Get()->Print(false));
        UNIT_ASSERT_VALUES_EQUAL(msg->ResponseSz, 1);
        auto& response = msg->Responses[0];
        UNIT_ASSERT_VALUES_EQUAL(response.Status, NKikimrProto::OK);
        UNIT_ASSERT_EQUAL(response.Status, NKikimrProto::OK);
        if (Committed.count(response.Id)) {
            UNIT_ASSERT_VALUES_EQUAL(Committed.at(response.Id), response.Buffer.ConvertToString());
            Committed.erase(response.Id);
        }
        if (!--ReadsInFlight) {
            std::vector<TLogoBlobID> ids;
            for (const auto& [blobId, _] : Committed) {
                ids.push_back(blobId);
            }
            UNIT_ASSERT_C(Committed.empty(), FormatList(ids));
            IssueCollectGarbage();
        }
    }

    void IssueCollectGarbage() {
        if (Generation == 1) {
            IssueWrites();
        } else {
            auto msg = std::make_unique<TEvBlobStorage::TEvCollectGarbage>(TabletId, Generation, 1, 0, true,
                Generation - 1, Max<ui32>(), nullptr, nullptr, TInstant::Max(), false, false);
            LOG_DEBUG_S(*TlsActivationContext, NActorsServices::TEST, Prefix << "sending TEvCollectGarbage# "
                << msg->ToString());
            SendToProxy(TDuration::MilliSeconds(100), msg.release());
        }
    }

    void Handle(TEvBlobStorage::TEvCollectGarbageResult::TPtr ev) {
        LOG_DEBUG_S(*TlsActivationContext, NActorsServices::TEST, Prefix << "got TEvCollectGarbageResult# " << ev->Get()->ToString());
        UNIT_ASSERT_C(ev->Get()->Status == NKikimrProto::OK || ev->Get()->Status == NKikimrProto::ALREADY, ev->Get()->Print(false));
        IssueWrites();
    }

    TString GenerateBuffer() {
        size_t len = 10 + RandomNumber<size_t>(32);
        TString buffer = TString::Uninitialized(len);
        char *data = buffer.Detach();
        std::generate(data, data + len, TReallyFastRng32(TabletId ^ Step));
        return buffer;
    }

    void IssueWrites() {
        for (; Inflight.size() < MaxWritesInFlight && NumWritesRemaining; --NumWritesRemaining) {
            TString buffer = GenerateBuffer();
            const TLogoBlobID id(TabletId, Generation, Step++, 0, buffer.size(), 0);
            LOG_DEBUG_S(*TlsActivationContext, NActorsServices::TEST, Prefix << "sending TEvPut Id# " << id);
            SendToProxy(Quoter.Take(TActivationContext::Now(), 1), new TEvBlobStorage::TEvPut(id, buffer, TInstant::Max()));
            Inflight.emplace(id, std::move(buffer));
        }
        if (!NumWritesRemaining && Inflight.empty()) {
            ++Generation;
            IssueInitialCheck();
        }
    }

    void Handle(TEvBlobStorage::TEvPutResult::TPtr ev) {
        LOG_DEBUG_S(*TlsActivationContext, NActorsServices::TEST, Prefix << "got TEvPutResult# " << ev->Get()->ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(ev->Get()->Status, NKikimrProto::OK, ev->Get()->Print(false));
        auto it = Inflight.find(ev->Get()->Id);
        UNIT_ASSERT(it != Inflight.end());
        Committed.emplace(*it);
        Inflight.erase(it);
        IssueWrites();
    }

    void Handle(TEvents::TEvPoison::TPtr ev) {
        TActivationContext::Send(new IEventHandle(EvFinished, 0, ev->Sender, SelfId(), {}, 0));
        PassAway();
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvBlobStorage::TEvStatusResult, Handle);
        hFunc(TEvBlobStorage::TEvBlockResult, Handle);
        hFunc(TEvBlobStorage::TEvDiscoverResult, Handle);
        hFunc(TEvBlobStorage::TEvRangeResult, Handle);
        hFunc(TEvBlobStorage::TEvGetResult, Handle);
        hFunc(TEvBlobStorage::TEvPutResult, Handle);
        hFunc(TEvBlobStorage::TEvCollectGarbageResult, Handle);
        hFunc(TEvents::TEvPoison, Handle);
    )
};
