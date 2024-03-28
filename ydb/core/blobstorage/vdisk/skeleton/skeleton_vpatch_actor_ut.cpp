#include "skeleton_vpatch_actor.h"

#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>

#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/basics/appdata.h>


#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

    enum {
        Begin = EventSpaceBegin(TEvents::ES_USERSPACE),
        EvRequestEnd
    };

    struct TEvRequestEnd : TEventLocal<TEvRequestEnd, EvRequestEnd> {
        TEvRequestEnd() = default;
    };

    struct TVPatchDecoratorArgs {
        TActorId EdgeActor;
        bool IsCheckingEvents;
        TVector<ui64> SequenceOfReceivingEvents;
        TVector<ui64> SequenceOfSendingEvents;
    };

    struct TVPatchDecorator : public TTestDecorator {

        static constexpr bool CheckDyingState = false;
        bool InStateFunc = false;

        TActorId EdgeActor;

        TVector<ui64> SequenceOfReceivingEvents;
        TVector<ui64> SequenceOfSendingEvents;
        ui64 ReceivingIdx = 0;
        ui64 SendingIdx = 0;
        bool IsCheckingEvents = false;

        TVPatchDecorator(THolder<IActor> &&actor, TVPatchDecoratorArgs &args)
            : TTestDecorator(std::move(actor))
            , EdgeActor(args.EdgeActor)
            , SequenceOfReceivingEvents(std::move(args.SequenceOfReceivingEvents))
            , SequenceOfSendingEvents(std::move(args.SequenceOfSendingEvents))
            , IsCheckingEvents(args.IsCheckingEvents)
        {
        }

        virtual ~TVPatchDecorator() {
            if (NActors::TlsActivationContext) {
                std::unique_ptr<IEventBase> ev = std::make_unique<TEvRequestEnd>();
                std::unique_ptr<IEventHandle> handle = std::make_unique<IEventHandle>(EdgeActor, EdgeActor, ev.release());
                TActivationContext::Send(handle.release());
            }
        }

        bool DoBeforeSending(TAutoPtr<IEventHandle> &ev) override {
            if (ev->HasEvent()) {
                Cerr << "Send " << ev->GetTypeName() << Endl;
            } else {
                Cerr << "Send " << ev->Type << Endl;
            }
            if (IsCheckingEvents) {
                UNIT_ASSERT_LT_C(SendingIdx, SequenceOfSendingEvents.size(), "SequenceOfSendingEvents overbounded");
                UNIT_ASSERT_VALUES_EQUAL_C(SequenceOfSendingEvents[SendingIdx], ev->Type, "sending idx " << SendingIdx);
            }

            SendingIdx++;
            return true;
        }

        bool DoBeforeReceiving(TAutoPtr<IEventHandle> &ev, const TActorContext &/*ctx*/) override {
            if (ev->Type == TEvents::TSystem::PoisonPill) {
                PassAway();
                return false;
            }
            if (ev->HasEvent()) {
                Cerr << "Recv " << ev->GetTypeName() << Endl;
            } else {
                Cerr << "Recv " << ev->Type << Endl;
            }

            InStateFunc = true;
            if (IsCheckingEvents) {
                UNIT_ASSERT_LT_C(ReceivingIdx, SequenceOfReceivingEvents.size(), "SequenceOfReceivingEvents overbounded");
                UNIT_ASSERT_VALUES_EQUAL_C(SequenceOfReceivingEvents[ReceivingIdx], ev->Type, "receive idx " << ReceivingIdx);
            }

            ReceivingIdx++;
            return true;
        }

        void DoAfterReceiving(const TActorContext &/*ctx*/) override {
            InStateFunc = false;
        }
    };

    bool ScheduledFilterFunc(NActors::TTestActorRuntimeBase& runtime, TAutoPtr<NActors::IEventHandle>& event,
            TDuration delay, TInstant& deadline) {
        if (runtime.IsScheduleForActorEnabled(event->GetRecipientRewrite())) {
            deadline = runtime.GetTimeProvider()->Now() + delay;
            return false;
        }
        return true;
    }

    struct TVPatchTestGeneralData {
        TBlobStorageGroupType GType;
        TTestBasicRuntime Runtime; // TODO(kruall): change to lighter
        TVector<TActorId> EdgeActors;
        TLogoBlobID OriginalBlobId;
        TLogoBlobID PatchedBlobId;

        TInstant Deadline;
        TInstant Now;

        TVector<ui64> SequenceOfReceivingEvents;
        TVector<ui64> SequenceOfSendingEvents;
        bool IsCheckingEventsByDecorator = false;

        TVector<TActorId> VPatchActorIds;
        TVector<TVDiskID> VDiskIds;

        float ApproximateFreeSpaceShare = 0.1;
        ui32 StatusFlags = 1;

        TVPatchTestGeneralData(const TBlobStorageGroupType &gType, ui32 blobSize, ui32 nodeCount = 1)
            : GType(gType)
            , Runtime(nodeCount, false)
            , OriginalBlobId(1, 2, 3, 4, blobSize, 6)
            , PatchedBlobId(1, 3, 3, 4, blobSize, 6)
            , Deadline()
        {
            InitLogLevels();
            Runtime.SetScheduledEventFilter(&ScheduledFilterFunc);
            Runtime.SetEventFilter([](NActors::TTestActorRuntimeBase&, TAutoPtr<NActors::IEventHandle>&) {
                return false;
            });

            TAppPrepare app;
            app.ClearDomainsAndHive();
            Runtime.Initialize(app.Unwrap());

            for (ui32 nodeIdx = 0; nodeIdx < nodeCount; ++nodeIdx) {
                EdgeActors.push_back(Runtime.AllocateEdgeActor(nodeIdx));
                TVDiskIdShort shortId(0, nodeIdx, 0);
                VDiskIds.emplace_back(0, 1, shortId);
            }

            Now = Runtime.GetCurrentTime();
        }

        void InitLogLevels() {
            Runtime.SetLogPriority(NKikimrServices::BS_VDISK_PATCH, NLog::PRI_DEBUG);
            Runtime.SetLogPriority(NActorsServices::TEST, NLog::PRI_DEBUG);
        }

        std::unique_ptr<TEvBlobStorage::TEvVPatchStart> CreateVPatchStart(TMaybe<ui64> cookie, ui32 nodeId = 0) const {
            return std::make_unique<TEvBlobStorage::TEvVPatchStart>(OriginalBlobId, PatchedBlobId, VDiskIds[nodeId], Deadline,
                    cookie, false);
        }

        std::unique_ptr<TEvBlobStorage::TEvVPatchDiff> CreateVPatchDiff(ui8 partId, ui8 waitedXorDiffs,
                const TVector<TDiff> &diffs, TMaybe<ui64> cookie, ui8 nodeId = 0) const {
            std::unique_ptr<TEvBlobStorage::TEvVPatchDiff> diff = std::make_unique<TEvBlobStorage::TEvVPatchDiff>(TLogoBlobID(OriginalBlobId, partId),
                    TLogoBlobID(PatchedBlobId, partId), VDiskIds[nodeId], waitedXorDiffs, Deadline, cookie);
            for (auto &diffBlock : diffs) {
                diff->AddDiff(diffBlock.Offset, diffBlock.Buffer);
            }
            return std::move(diff);
        }

        std::unique_ptr<TEvBlobStorage::TEvVPatchDiff> CreateForceEndVPatchDiff(ui8 partId, TMaybe<ui64> cookie) const {
            std::unique_ptr<TEvBlobStorage::TEvVPatchDiff> diff = std::make_unique<TEvBlobStorage::TEvVPatchDiff>(TLogoBlobID(OriginalBlobId, partId),
                    TLogoBlobID(PatchedBlobId, partId), VDiskIds[partId - 1], false, Deadline, cookie);
            diff->SetForceEnd();
            return std::move(diff);
        }

        template <typename DecoratorType = void>
        TActorId CreateTVPatchActor(TEvBlobStorage::TEvVPatchStart::TPtr &&ev, ui32 nodeId = 0) {
            TIntrusivePtr<TVPatchCtx> patchCtx = MakeIntrusive<TVPatchCtx>();
            for (ui32 idx = 0; idx < VDiskIds.size(); ++idx) {
                TVDiskIdShort id(VDiskIds[idx]);
                patchCtx->AsyncBlobQueues.emplace(id, EdgeActors[idx]);
            }

            THolder<IActor> actor{CreateSkeletonVPatchActor(EdgeActors[nodeId], GType,
                    ev, TInstant(), nullptr, nullptr, nullptr, nullptr, nullptr, patchCtx, VDiskIds[nodeId].ToString(), 0, nullptr)};

            if constexpr (!std::is_void_v<DecoratorType>) {
                TVPatchDecoratorArgs args{EdgeActors[nodeId], IsCheckingEventsByDecorator,
                        SequenceOfReceivingEvents, SequenceOfSendingEvents};
                actor = MakeHolder<DecoratorType>(std::move(actor), args);
            }

            VPatchActorIds.emplace_back(Runtime.Register(actor.Release(), nodeId));
            return VPatchActorIds.back();
        }

        void WaitEndTest() {
            for (TActorId &edgeActor : EdgeActors) {
                Runtime.GrabEdgeEventRethrow<TEvRequestEnd>({edgeActor});
            }
        }

        void ForceEndTest() {
            std::unique_ptr<IEventHandle> handle;
            ui32 nodeCount = Runtime.GetNodeCount();
            for (ui32 nodeId = 0; nodeId < nodeCount; ++nodeId) {
                handle = std::make_unique<IEventHandle>(VPatchActorIds[nodeId], EdgeActors[nodeId],
                        new NActors::TEvents::TEvPoisonPill);
                Runtime.Send(handle.release());
            }
            WaitEndTest();
        }
    };

    template<typename EventType>
    typename EventType::TPtr CreateEventHandle(const TActorId &recipient, const TActorId &sender,
            std::unique_ptr<EventType> &&ev)
    {
        return static_cast<TEventHandle<EventType>*>(new IEventHandle(recipient, sender, ev.release()));
    }



    Y_UNIT_TEST_SUITE(TVPatchTests) {

        struct TBlob {
            TLogoBlobID BlobId;
            TString Buffer;

            TBlob(const TLogoBlobID &blob, ui8 partId, const TString &buffer = "")
                : BlobId(blob, partId)
                , Buffer(buffer)
            {
            }

            TBlob(const TLogoBlobID &blob, ui8 partId, const TRope &buffer)
                : BlobId(blob, partId)
                , Buffer(buffer.ConvertToString())
            {
            }

            TBlob(const TLogoBlobID &blob, ui8 partId, ui32 bufferSize)
                : BlobId(blob, partId)
            {
                TStringBuilder str;
                for (ui32 idx = 0; idx < bufferSize; ++idx) {
                    str << 'a';
                }
                Buffer = str;
            }
        };

        bool PassFindingParts(TVPatchTestGeneralData &testData, NKikimrProto::EReplyStatus vGetStatus,
                const TVector<ui8> &foundParts, ui32 nodeId = 0) {
            TTestActorRuntimeBase &runtime = testData.Runtime;
            TActorId edgeActor = testData.EdgeActors[nodeId];
            TActorId vPatchActorId = testData.VPatchActorIds[nodeId];

            TAutoPtr<IEventHandle> handle;
            auto evVGetRange = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvVGet>(handle);

            UNIT_ASSERT(evVGetRange->Record.HasCookie());
            UNIT_ASSERT(evVGetRange->Record.HasIndexOnly() && evVGetRange->Record.GetIndexOnly());
            std::unique_ptr<TEvBlobStorage::TEvVGetResult> evVGetRangeResult = std::make_unique<TEvBlobStorage::TEvVGetResult>(
                    vGetStatus, testData.VDiskIds[nodeId], testData.Now, evVGetRange->GetCachedByteSize(), &evVGetRange->Record,
                    nullptr, nullptr, nullptr, evVGetRange->Record.GetCookie(), handle->GetChannel(), 0);

            evVGetRangeResult->AddResult(NKikimrProto::OK, TLogoBlobID(testData.OriginalBlobId, 0));
            for (ui8 partId : foundParts) {
                evVGetRangeResult->Record.MutableResult(0)->AddParts(partId);
            }
            handle = new IEventHandle(vPatchActorId, edgeActor, evVGetRangeResult.release());
            runtime.Send(handle.Release());

            auto evVPatchFoundParts = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvVPatchFoundParts>(handle);
            NKikimrBlobStorage::TEvVPatchFoundParts &vPatchFoundParts = evVPatchFoundParts->Record;
            UNIT_ASSERT(vPatchFoundParts.HasCookie());
            UNIT_ASSERT(vPatchFoundParts.GetCookie() == nodeId);
            if (vPatchFoundParts.OriginalPartsSize()) {
                UNIT_ASSERT(vPatchFoundParts.HasStatus());
                UNIT_ASSERT(vPatchFoundParts.GetStatus() == NKikimrProto::OK);

                TVector<ui8> parts;
                parts.reserve(vPatchFoundParts.OriginalPartsSize());
                for (ui64 part : vPatchFoundParts.GetOriginalParts()) {
                    UNIT_ASSERT(part <= TLogoBlobID::MaxPartId);
                    parts.push_back(part);
                }
                UNIT_ASSERT(foundParts == parts);
                return false;
            } else {
                UNIT_ASSERT(vPatchFoundParts.HasStatus());
                return true;
            }
        }

        void MakeVPatchFindingPartsTest(NKikimrProto::EReplyStatus vGetStatus, const TVector<ui8> &foundParts,
                TVector<ui64> &&receivingEvents, TVector<ui64> &&sendingEvents)
        {
            TBlobStorageGroupType type(TErasureType::Erasure4Plus2Block);
            TVPatchTestGeneralData testData(type, 10);
            TActorId edgeActor = testData.EdgeActors[0];

            testData.IsCheckingEventsByDecorator = true;
            testData.SequenceOfReceivingEvents = std::move(receivingEvents);
            testData.SequenceOfSendingEvents = std::move(sendingEvents);

            std::unique_ptr<TEvBlobStorage::TEvVPatchStart> start = testData.CreateVPatchStart(0);
            TEvBlobStorage::TEvVPatchStart::TPtr ev = CreateEventHandle(edgeActor, edgeActor, std::move(start));
            TActorId vPatchActorId = testData.CreateTVPatchActor<TVPatchDecorator>(std::move(ev));

            bool isKilled = PassFindingParts(testData, vGetStatus, foundParts);
            TAutoPtr<IEventHandle> handle;
            if (!isKilled) {
                std::unique_ptr<TEvBlobStorage::TEvVPatchDiff> diff = testData.CreateForceEndVPatchDiff(1, 0);
                handle = new IEventHandle(vPatchActorId, edgeActor, diff.release());
                testData.Runtime.Send(handle.Release());

                auto result = testData.Runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvVPatchResult>(handle);
                UNIT_ASSERT(result->Record.GetStatus() == NKikimrProto::OK);
                auto diyngRequest = testData.Runtime.GrabEdgeEventRethrow<TEvVPatchDyingRequest>(handle);
                UNIT_ASSERT(diyngRequest->PatchedBlobId == testData.PatchedBlobId);
                handle = new IEventHandle(vPatchActorId, edgeActor, new TEvVPatchDyingConfirm);
                testData.Runtime.Send(handle.Release());
            } else {
                auto diyngRequest = testData.Runtime.GrabEdgeEventRethrow<TEvVPatchDyingRequest>(handle);
                UNIT_ASSERT(diyngRequest->PatchedBlobId == testData.PatchedBlobId);
                handle = new IEventHandle(vPatchActorId, edgeActor, new TEvVPatchDyingConfirm);
                testData.Runtime.Send(handle.Release());
            }

            testData.WaitEndTest();
        }

        Y_UNIT_TEST(FindingPartsWhenPartsAreDontExist) {
            TVector<ui64> receivingEvents {
                    TEvents::TSystem::Bootstrap,
                    TEvBlobStorage::EvVGetResult,
                    TEvBlobStorage::EvVPatchDyingConfirm};
            TVector<ui64> sendingEvents {
                    TEvBlobStorage::EvVGet,
                    TEvBlobStorage::EvVPatchFoundParts,
                    TEvBlobStorage::EvVPatchDyingRequest};
            MakeVPatchFindingPartsTest(NKikimrProto::OK, {}, std::move(receivingEvents), std::move(sendingEvents));
        }

        Y_UNIT_TEST(FindingPartsWhenOnlyOnePartExists) {
            TVector<ui64> receivingEvents {
                    TEvents::TSystem::Bootstrap,
                    TEvBlobStorage::EvVGetResult,
                    TEvBlobStorage::EvVPatchDiff,
                    TEvBlobStorage::EvVPatchDyingConfirm};
            TVector<ui64> sendingEvents {
                    TEvBlobStorage::EvVGet,
                    TEvBlobStorage::EvVPatchFoundParts,
                    TEvBlobStorage::EvVPatchResult,
                    TEvBlobStorage::EvVPatchDyingRequest};
            MakeVPatchFindingPartsTest(NKikimrProto::OK, {1}, std::move(receivingEvents), std::move(sendingEvents));
        }

        Y_UNIT_TEST(FindingPartsWhenSeveralPartsExist) {
            TVector<ui64> receivingEvents {
                    TEvents::TSystem::Bootstrap,
                    TEvBlobStorage::EvVGetResult,
                    TEvBlobStorage::EvVPatchDiff,
                    TEvBlobStorage::EvVPatchDyingConfirm};
            TVector<ui64> sendingEvents {
                    TEvBlobStorage::EvVGet,
                    TEvBlobStorage::EvVPatchFoundParts,
                    TEvBlobStorage::EvVPatchResult,
                    TEvBlobStorage::EvVPatchDyingRequest};
            MakeVPatchFindingPartsTest(NKikimrProto::OK, {1, 2}, std::move(receivingEvents), std::move(sendingEvents));
        }

        Y_UNIT_TEST(FindingPartsWhenError) {
            TVector<ui64> receivingEvents {
                    TEvents::TSystem::Bootstrap,
                    TEvBlobStorage::EvVGetResult,
                    TEvBlobStorage::EvVPatchDyingConfirm};
            TVector<ui64> sendingEvents {
                    TEvBlobStorage::EvVGet,
                    TEvBlobStorage::EvVPatchFoundParts,
                    TEvBlobStorage::EvVPatchDyingRequest};
            MakeVPatchFindingPartsTest(NKikimrProto::ERROR, {}, std::move(receivingEvents), std::move(sendingEvents));
        }

        Y_UNIT_TEST(FindingPartsWithTimeout) {
            TBlobStorageGroupType type(TErasureType::Erasure4Plus2Block);
            TVPatchTestGeneralData testData(type, 10);
            TTestActorRuntimeBase &runtime = testData.Runtime;
            TActorId edgeActor = testData.EdgeActors[0];

            testData.IsCheckingEventsByDecorator = true;
            testData.SequenceOfReceivingEvents = {
                    TEvents::TSystem::Bootstrap,
                    TKikimrEvents::TSystem::Wakeup,
                    TEvBlobStorage::EvVPatchDyingConfirm};
            testData.SequenceOfSendingEvents = {
                    TEvBlobStorage::EvVGet,
                    TEvBlobStorage::EvVPatchFoundParts,
                    TEvBlobStorage::EvVPatchDyingRequest};

            std::unique_ptr<TEvBlobStorage::TEvVPatchStart> start = testData.CreateVPatchStart(0);
            TEvBlobStorage::TEvVPatchStart::TPtr ev = CreateEventHandle(edgeActor, edgeActor, std::move(start));
            TActorId actorId = testData.CreateTVPatchActor<TVPatchDecorator>(std::move(ev));

            runtime.EnableScheduleForActor(actorId);

            TAutoPtr<IEventHandle> handle;
            auto evVPatchFoundParts = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvVPatchFoundParts>(handle);
            NKikimrBlobStorage::TEvVPatchFoundParts &vPatchFoundParts = evVPatchFoundParts->Record;
            UNIT_ASSERT_VALUES_EQUAL(vPatchFoundParts.GetStatus(), NKikimrProto::ERROR);

            auto dyingRequest = runtime.GrabEdgeEventRethrow<TEvVPatchDyingRequest>(handle);
            UNIT_ASSERT_VALUES_EQUAL(dyingRequest->PatchedBlobId, testData.PatchedBlobId);
            handle = new IEventHandle(actorId, edgeActor, new TEvVPatchDyingConfirm);
            testData.Runtime.Send(handle.Release());
            testData.WaitEndTest();
        }

        bool PassPullingPart(TVPatchTestGeneralData &testData, NKikimrProto::EReplyStatus vGetStatus,
                const TBlob &blob, ui32 nodeId = 0) {
            TTestActorRuntimeBase &runtime = testData.Runtime;
            TActorId edgeActor = testData.EdgeActors[nodeId];
            TActorId vPatchActorId = testData.VPatchActorIds[nodeId];


            auto vGetHandle = testData.Runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvVGet>({edgeActor});
            auto evVGet = vGetHandle->Get();

            UNIT_ASSERT(evVGet->Record.HasCookie());
            UNIT_ASSERT(!evVGet->Record.HasIndexOnly() || !evVGet->Record.GetIndexOnly());

            std::unique_ptr<TEvBlobStorage::TEvVGetResult> evVGetResult = std::make_unique<TEvBlobStorage::TEvVGetResult>(
                    vGetStatus, testData.VDiskIds[nodeId], testData.Now, evVGet->GetCachedByteSize(), &evVGet->Record,
                    nullptr, nullptr, nullptr, evVGet->Record.GetCookie(), vGetHandle->GetChannel(), 0);
            evVGetResult->AddResult(NKikimrProto::OK, blob.BlobId, 0, TRope(blob.Buffer));

            std::unique_ptr<IEventHandle> handle = std::make_unique<IEventHandle>(vPatchActorId, edgeActor, evVGetResult.release());
            runtime.Send(handle.release());

            return vGetStatus != NKikimrProto::OK;
        }

        bool PassStoringPart(TVPatchTestGeneralData &testData, NKikimrProto::EReplyStatus vPutStatus, const TBlob &blob,
                ui32 nodeId = 0)
        {
            TTestActorRuntimeBase &runtime = testData.Runtime;
            TActorId edgeActor = testData.EdgeActors[nodeId];
            TActorId vPatchActorId = testData.VPatchActorIds[nodeId];

            TAutoPtr<IEventHandle> handle;
            auto vPut = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvVPut>(handle);
            NKikimrBlobStorage::TEvVPut &record = vPut->Record;

            UNIT_ASSERT(record.HasCookie());
            ui64 cookie = record.GetCookie();
            TLogoBlobID blobId = LogoBlobIDFromLogoBlobID(record.GetBlobID());
            UNIT_ASSERT_VALUES_EQUAL(blobId, blob.BlobId);
            UNIT_ASSERT_C(vPut->GetBuffer() == blob.Buffer, "NodeId# " << nodeId);

            TOutOfSpaceStatus oos = TOutOfSpaceStatus(testData.StatusFlags, testData.ApproximateFreeSpaceShare);
            std::unique_ptr<TEvBlobStorage::TEvVPutResult> vPutResult = std::make_unique<TEvBlobStorage::TEvVPutResult>(
                    vPutStatus, blobId, testData.VDiskIds[nodeId], &cookie, oos, testData.Now,
                    0, &record, nullptr, nullptr, nullptr, vPut->GetBufferBytes(), 0, "");

            handle = new IEventHandle(vPatchActorId, edgeActor, vPutResult.release());
            runtime.Send(handle.Release());
            return true;
        }

        void MakeVPatchTest(NKikimrProto::EReplyStatus pullingStatus, NKikimrProto::EReplyStatus storingStatus,
                ui32 partSize, const TVector<ui8> &foundPartIds, ui8 pullingPart, ui32 xorReceiverCount,
                TVector<ui64> &&receivingEvents, TVector<ui64> &&sendingEvents)
        {
            Y_UNUSED(xorReceiverCount);
            TBlobStorageGroupType type(TErasureType::Erasure4Plus2Block);
            TVPatchTestGeneralData testData(type, 10);
            TTestActorRuntimeBase &runtime = testData.Runtime;
            TActorId edgeActor = testData.EdgeActors[0];

            testData.IsCheckingEventsByDecorator = true;
            testData.SequenceOfReceivingEvents = std::move(receivingEvents);
            testData.SequenceOfSendingEvents = std::move(sendingEvents);

            std::unique_ptr<TEvBlobStorage::TEvVPatchStart> start = testData.CreateVPatchStart(0);
            TEvBlobStorage::TEvVPatchStart::TPtr ev = CreateEventHandle(edgeActor, edgeActor, std::move(start));
            TActorId vPatchActorId = testData.CreateTVPatchActor<TVPatchDecorator>(std::move(ev));


            bool isKilled = false;
            isKilled = PassFindingParts(testData, NKikimrProto::OK, foundPartIds);
            UNIT_ASSERT(!isKilled);

            std::unique_ptr<TEvBlobStorage::TEvVPatchDiff> diff = testData.CreateVPatchDiff(pullingPart, false, {}, 0);
            TAutoPtr<IEventHandle> handle;

            handle = new IEventHandle(vPatchActorId, edgeActor, diff.release());
            runtime.Send(handle.Release());
            TBlob pullingBlob(testData.OriginalBlobId, pullingPart, partSize);

            isKilled = PassPullingPart(testData, pullingStatus, pullingBlob);
            if (isKilled) {
                auto result = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvVPatchResult>(handle);
                UNIT_ASSERT(result->Record.GetStatus() == NKikimrProto::ERROR);
                handle = new IEventHandle(vPatchActorId, edgeActor, new TEvVPatchDyingConfirm);
                runtime.Send(handle.Release());
                testData.WaitEndTest();
                return;
            }

            TBlob storingBlob = pullingBlob;
            storingBlob.BlobId = TLogoBlobID(testData.PatchedBlobId, pullingPart);
            isKilled = PassStoringPart(testData, storingStatus, storingBlob);
            NKikimrProto::EReplyStatus expectedResultStatus =
                    (storingStatus != NKikimrProto::OK) ? NKikimrProto::ERROR : NKikimrProto::OK;

            auto result = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvVPatchResult>(handle);
            UNIT_ASSERT(result->Record.GetStatus() == expectedResultStatus);
            UNIT_ASSERT(result->Record.GetStatusFlags() == testData.StatusFlags);
            UNIT_ASSERT(result->Record.GetApproximateFreeSpaceShare() == testData.ApproximateFreeSpaceShare);


            auto diyngRequest = testData.Runtime.GrabEdgeEventRethrow<TEvVPatchDyingRequest>(handle);
            UNIT_ASSERT(diyngRequest->PatchedBlobId == testData.PatchedBlobId);
            handle = new IEventHandle(vPatchActorId, edgeActor, new TEvVPatchDyingConfirm);
            testData.Runtime.Send(handle.Release());
            testData.WaitEndTest();
        }

        Y_UNIT_TEST(PatchPartOk) {
            TVector<ui64> receivingEvents {
                    TEvents::TSystem::Bootstrap,
                    TEvBlobStorage::EvVGetResult,
                    TEvBlobStorage::EvVPatchDiff,
                    TEvBlobStorage::EvVGetResult,
                    TEvBlobStorage::EvVPutResult,
                    TEvBlobStorage::EvVPatchDyingConfirm};
            TVector<ui64> sendingEvents {
                    TEvBlobStorage::EvVGet,
                    TEvBlobStorage::EvVPatchFoundParts,
                    TEvBlobStorage::EvVGet,
                    TEvBlobStorage::EvVPut,
                    TEvBlobStorage::EvVPatchResult,
                    TEvBlobStorage::EvVPatchDyingRequest};

            TVector<ui8> foundPartIds = {1};
            MakeVPatchTest(NKikimrProto::OK, NKikimrProto::OK, 100, {1}, 1, 0,
                    std::move(receivingEvents), std::move(sendingEvents));
        }

        Y_UNIT_TEST(PatchPartGetError) {
            TVector<ui64> receivingEvents {
                    TEvents::TSystem::Bootstrap,
                    TEvBlobStorage::EvVGetResult,
                    TEvBlobStorage::EvVPatchDiff,
                    TEvBlobStorage::EvVGetResult,
                    TEvBlobStorage::EvVPatchDyingConfirm};
            TVector<ui64> sendingEvents {
                    TEvBlobStorage::EvVGet,
                    TEvBlobStorage::EvVPatchFoundParts,
                    TEvBlobStorage::EvVGet,
                    TEvBlobStorage::EvVPatchResult,
                    TEvBlobStorage::EvVPatchDyingRequest};

            TVector<ui8> foundPartIds = {1};
            MakeVPatchTest(NKikimrProto::ERROR, NKikimrProto::OK, 100, {1}, 1, 0,
                    std::move(receivingEvents), std::move(sendingEvents));
        }

        Y_UNIT_TEST(PatchPartPutError) {
            TVector<ui64> receivingEvents {
                    TEvents::TSystem::Bootstrap,
                    TEvBlobStorage::EvVGetResult,
                    TEvBlobStorage::EvVPatchDiff,
                    TEvBlobStorage::EvVGetResult,
                    TEvBlobStorage::EvVPutResult,
                    TEvBlobStorage::EvVPatchDyingConfirm};
            TVector<ui64> sendingEvents {
                    TEvBlobStorage::EvVGet,
                    TEvBlobStorage::EvVPatchFoundParts,
                    TEvBlobStorage::EvVGet,
                    TEvBlobStorage::EvVPut,
                    TEvBlobStorage::EvVPatchResult,
                    TEvBlobStorage::EvVPatchDyingRequest};

            TVector<ui8> foundPartIds = {1};
            MakeVPatchTest(NKikimrProto::OK, NKikimrProto::ERROR, 100, {1}, 1, 0,
                    std::move(receivingEvents), std::move(sendingEvents));
        }

        void SendXorDiff(TVPatchTestGeneralData &testData, const TVector<TDiff> &xorDiffs, ui32 toPart, ui32 nodeId = 0) {
            TTestActorRuntimeBase &runtime = testData.Runtime;
            TActorId edgeActor = testData.EdgeActors[nodeId];
            TActorId vPatchActorId = testData.VPatchActorIds[nodeId];
            TVDiskID vDiskId = testData.VDiskIds[nodeId];

            std::unique_ptr<TEvBlobStorage::TEvVPatchXorDiff> xorDiff = std::make_unique<TEvBlobStorage::TEvVPatchXorDiff>(
                    TLogoBlobID(testData.OriginalBlobId, toPart),
                    TLogoBlobID(testData.PatchedBlobId, toPart),
                    vDiskId, toPart, testData.Deadline, 0);
            for (auto &diff : xorDiffs) {
                xorDiff->AddDiff(diff.Offset, diff.Buffer);
            }

            std::unique_ptr<IEventHandle> handle = std::make_unique<IEventHandle>(vPatchActorId, edgeActor, xorDiff.release());
            runtime.Send(handle.release());
        }

        void ReceiveVPatchResult(TVPatchTestGeneralData &testData, NKikimrProto::EReplyStatus status) {
            TAutoPtr<IEventHandle> handle;
            auto result = testData.Runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvVPatchResult>(handle);
            UNIT_ASSERT(result->Record.GetStatus() == status);
        }

        void MakeXorDiffFaultToleranceTest(NKikimrProto::EReplyStatus status, ui64 partSize,
                const TVector<TDiff> &diffs, TVector<ui64> &&receivingEvents, TVector<ui64> &&sendingEvents)
        {
            TBlobStorageGroupType type(TErasureType::Erasure4Plus2Block);
            TVPatchTestGeneralData testData(type, partSize);
            TTestActorRuntimeBase &runtime = testData.Runtime;
            TActorId edgeActor = testData.EdgeActors[0];

            testData.IsCheckingEventsByDecorator = true;
            testData.SequenceOfReceivingEvents = std::move(receivingEvents);
            testData.SequenceOfSendingEvents = std::move(sendingEvents);

            std::unique_ptr<TEvBlobStorage::TEvVPatchStart> start = testData.CreateVPatchStart(0);
            TEvBlobStorage::TEvVPatchStart::TPtr ev = CreateEventHandle(edgeActor, edgeActor, std::move(start));
            TActorId vPatchActorId = testData.CreateTVPatchActor<TVPatchDecorator>(std::move(ev));

            ui8 partId = type.DataParts() + 1;
            bool isKilled = false;
            isKilled = PassFindingParts(testData, NKikimrProto::OK, {partId});
            UNIT_ASSERT(!isKilled);

            SendXorDiff(testData, diffs, type.DataParts());
            std::unique_ptr<TEvBlobStorage::TEvVPatchDiff> diff = testData.CreateVPatchDiff(partId, 1, {}, 0);
            std::unique_ptr<IEventHandle> handle;

            handle = std::make_unique<IEventHandle>(vPatchActorId, edgeActor, diff.release());
            runtime.Send(handle.release());

            if (status != NKikimrProto::OK) {
                TAutoPtr<IEventHandle> handle;
                testData.Runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvVPatchXorDiffResult>(handle);
                ReceiveVPatchResult(testData, status);
                handle = new IEventHandle(vPatchActorId, edgeActor, new TEvVPatchDyingConfirm);
                runtime.Send(handle.Release());

                auto diyngRequest = testData.Runtime.GrabEdgeEventRethrow<TEvVPatchDyingRequest>(handle);
                UNIT_ASSERT(diyngRequest->PatchedBlobId == testData.PatchedBlobId);
                handle = new IEventHandle(vPatchActorId, edgeActor, new TEvVPatchDyingConfirm);
                testData.Runtime.Send(handle.Release());

                testData.WaitEndTest();
            } else {
                testData.ForceEndTest();
            }

        }

        Y_UNIT_TEST(PatchPartFastXorDiffWithEmptyDiffBuffer) {
            TVector<ui64> receivingEvents {
                    TEvents::TSystem::Bootstrap,
                    TEvBlobStorage::EvVGetResult,
                    TEvBlobStorage::EvVPatchXorDiff,
                    TEvBlobStorage::EvVPatchDiff,
                    TEvBlobStorage::EvVGetResult,
                    TEvBlobStorage::EvVPutResult,
                    TEvBlobStorage::EvVPatchDyingConfirm};
            TVector<ui64> sendingEvents {
                    TEvBlobStorage::EvVGet,
                    TEvBlobStorage::EvVPatchFoundParts,
                    TEvBlobStorage::EvVPatchXorDiffResult,
                    TEvBlobStorage::EvVGet,
                    TEvBlobStorage::EvVPut,
                    TEvBlobStorage::EvVPatchResult,
                    TEvBlobStorage::EvVPatchDyingRequest};

            TVector<TDiff> diffs;
            diffs.emplace_back("", 0, true, false);
            MakeXorDiffFaultToleranceTest(NKikimrProto::OK, 100, diffs,
                    std::move(receivingEvents), std::move(sendingEvents));
        }

        Y_UNIT_TEST(PatchPartFastXorDiffBeyoundBlob) {
            TVector<ui64> receivingEvents {
                    TEvents::TSystem::Bootstrap,
                    TEvBlobStorage::EvVGetResult,
                    TEvBlobStorage::EvVPatchXorDiff,
                    TEvBlobStorage::EvVPatchDiff,
                    TEvBlobStorage::EvVPatchDyingConfirm};
            TVector<ui64> sendingEvents {
                    TEvBlobStorage::EvVGet,
                    TEvBlobStorage::EvVPatchFoundParts,
                    TEvBlobStorage::EvVPatchXorDiffResult,
                    TEvBlobStorage::EvVPatchDyingRequest,
                    TEvBlobStorage::EvVPatchResult};

            TVector<TDiff> diffs;
            diffs.emplace_back("", 100, true, false);
            MakeXorDiffFaultToleranceTest(NKikimrProto::ERROR, 100, diffs,
                    std::move(receivingEvents), std::move(sendingEvents));
        }

        Y_UNIT_TEST(PatchPartFastXorDiffDisorder) {
            TVector<ui64> receivingEvents {
                    TEvents::TSystem::Bootstrap,
                    TEvBlobStorage::EvVGetResult,
                    TEvBlobStorage::EvVPatchXorDiff,
                    TEvBlobStorage::EvVPatchDiff,
                    TEvBlobStorage::EvVPatchDyingConfirm};
            TVector<ui64> sendingEvents {
                    TEvBlobStorage::EvVGet,
                    TEvBlobStorage::EvVPatchFoundParts,
                    TEvBlobStorage::EvVPatchXorDiffResult,
                    TEvBlobStorage::EvVPatchDyingRequest,
                    TEvBlobStorage::EvVPatchResult};

            TVector<TDiff> diffs;
            diffs.emplace_back("aa", 3, true, false);
            diffs.emplace_back("aa", 0, true, false);
            MakeXorDiffFaultToleranceTest(NKikimrProto::ERROR, 100, diffs,
                    std::move(receivingEvents), std::move(sendingEvents));
        }

        void MakeFullVPatchTest(const TBlobStorageGroupType &type, const TString &data, const TVector<TDiff> &diffs,
                bool quickXorDiffs = false)
        {
            ui32 nodeCount = type.TotalPartCount();
            TVPatchTestGeneralData testData(type, data.Size(), nodeCount);

            for (ui32 nodeIdx = 0; nodeIdx < nodeCount; ++nodeIdx) {
                std::unique_ptr<TEvBlobStorage::TEvVPatchStart> start = testData.CreateVPatchStart(nodeIdx, nodeIdx);
                TActorId edgeActor = testData.EdgeActors[nodeIdx];
                TEvBlobStorage::TEvVPatchStart::TPtr ev = CreateEventHandle(edgeActor, edgeActor, std::move(start));
                testData.CreateTVPatchActor<TVPatchDecorator>(std::move(ev), nodeIdx);
            }

            TString savedData = TString::Uninitialized(data.size());
            memcpy(savedData.begin(), data.begin(), data.size());

            TString result = TString::Uninitialized(data.size());
            memcpy(result.begin(), data.begin(), data.size());
            ui8 *resultBytes = reinterpret_cast<ui8*>(const_cast<char*>(result.data()));
            type.ApplyDiff(TErasureType::CrcModeNone, resultBytes, diffs);
            TDataPartSet partSet;
            TDataPartSet resultPartSet;
            TDataPartSet savedPartSet;
            type.SplitData(TErasureType::CrcModeNone, data, partSet);
            type.SplitData(TErasureType::CrcModeNone, savedData, savedPartSet);
            type.SplitData(TErasureType::CrcModeNone, result, resultPartSet);

            TPartDiffSet diffSet;
            type.SplitDiffs(TErasureType::CrcModeNone, data.size(), diffs, diffSet);

            for (ui32 nodeIdx = 0; nodeIdx < nodeCount; ++nodeIdx) {
                ui8 partId = nodeIdx + 1;
                if (PassFindingParts(testData, NKikimrProto::OK, {partId}, nodeIdx)) {
                    TActorId edgeActor = testData.EdgeActors[nodeIdx];
                    TActorId vPatchActorId = testData.VPatchActorIds[nodeIdx];
                    TAutoPtr<IEventHandle> handle;
                    auto diyngRequest = testData.Runtime.GrabEdgeEventRethrow<TEvVPatchDyingRequest>(handle);
                    UNIT_ASSERT(diyngRequest->PatchedBlobId == testData.PatchedBlobId);
                    handle = new IEventHandle(vPatchActorId, edgeActor, new TEvVPatchDyingConfirm);
                    testData.Runtime.Send(handle.Release());
                }
            }

            ui32 dataPartCount = type.DataParts();
            ui32 totalPartCount = type.TotalPartCount();

            ui32 dataDiffCount = 0;
            for (ui32 partIdx = 0; partIdx < dataPartCount; ++partIdx) {
                ui32 partId = partIdx + 1;
                std::unique_ptr<TEvBlobStorage::TEvVPatchDiff> diff = testData.CreateVPatchDiff(partId, 0,
                        diffSet.PartDiffs[partIdx].Diffs, 0, partIdx);

                for (ui32 parityPartIdx = dataPartCount; parityPartIdx < totalPartCount; ++parityPartIdx) {
                    diff->AddXorReceiver(testData.VDiskIds[parityPartIdx], parityPartIdx + 1);
                }

                std::unique_ptr<IEventHandle> handle;
                handle = std::make_unique<IEventHandle>(testData.VPatchActorIds[partIdx], testData.EdgeActors[partIdx],
                        diff.release());
                testData.Runtime.Send(handle.release());
                dataDiffCount++;
            }

            for (ui32 partIdx = 0; partIdx < dataPartCount; ++partIdx) {
                ui32 partId = partIdx + 1;
                TBlob pullingBlob(testData.OriginalBlobId, partId, partSet.Parts[partIdx].OwnedString);
                PassPullingPart(testData, NKikimrProto::OK, pullingBlob, partIdx);
            }

            if (!quickXorDiffs) {
                for (ui32 partIdx = dataPartCount; partIdx < totalPartCount; ++partIdx) {
                    ui32 partId = partIdx + 1;
                    std::unique_ptr<TEvBlobStorage::TEvVPatchDiff> diff = testData.CreateVPatchDiff(partId, dataDiffCount,
                            {}, 0, partIdx);
                    std::unique_ptr<IEventHandle> handle;
                    handle = std::make_unique<IEventHandle>(testData.VPatchActorIds[partIdx], testData.EdgeActors[partIdx],
                            diff.release());
                    testData.Runtime.Send(handle.release());
                }

                for (ui32 partIdx = dataPartCount; partIdx < totalPartCount; ++partIdx) {
                    ui32 partId = partIdx + 1;
                    TBlob pullingBlob(testData.OriginalBlobId, partId, savedPartSet.Parts[partIdx].OwnedString);
                    PassPullingPart(testData, NKikimrProto::OK, pullingBlob, partIdx);
                }
            }

            for (ui32 partIdx = dataPartCount; partIdx < totalPartCount; ++partIdx) {
                for (ui32 dataDiffIdx = 0; dataDiffIdx < dataDiffCount; ++dataDiffIdx) {
                    TActorId edgeActor = testData.EdgeActors[partIdx];
                    auto handle = testData.Runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvVPatchXorDiff>({edgeActor});
                    auto &record = handle->Get()->Record;

                    ui8 fromPartId = record.GetFromPartId();
                    ui32 patchedPartId = LogoBlobIDFromLogoBlobID(record.GetPatchedPartBlobId()).PartId();

                    TVector<TDiff> xorDiffs;
                    const ui8 *buffer = reinterpret_cast<const ui8*>(partSet.Parts[fromPartId - 1].OwnedString.GetContiguousSpan().data());
                    testData.GType.MakeXorDiff(TErasureType::CrcModeNone, data.size(), buffer, diffSet.PartDiffs[fromPartId - 1].Diffs, &xorDiffs);

                    UNIT_ASSERT_VALUES_EQUAL_C(xorDiffs.size(), record.DiffsSize(), "from# " << (ui32)fromPartId);
                    for (ui32 idx = 0; idx < xorDiffs.size(); ++idx) {
                        UNIT_ASSERT_VALUES_EQUAL_C(xorDiffs[idx].Offset, record.GetDiffs(idx).GetOffset(), "from# " << (ui32)fromPartId);
                        UNIT_ASSERT_EQUAL(xorDiffs[idx].Buffer, record.GetDiffs(idx).GetBuffer());
                    }

                    TActorId patchActor = testData.VPatchActorIds[patchedPartId - 1];
                    auto handle2 = std::make_unique<IEventHandle>(patchActor, edgeActor, handle->Release().Release(), handle->Flags,
                            handle->Cookie, nullptr);
                    testData.Runtime.Send(handle2.release());
                    testData.Runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvVPatchXorDiffResult>({edgeActor});
                }
            }

            for (ui32 partIdx = dataPartCount; partIdx < totalPartCount; ++partIdx) {
                UNIT_ASSERT_EQUAL(partSet.Parts[partIdx].OwnedString, savedPartSet.Parts[partIdx].OwnedString);
            }

            if (quickXorDiffs) {
                for (ui32 partIdx = dataPartCount; partIdx < totalPartCount; ++partIdx) {
                    ui32 partId = partIdx + 1;
                    std::unique_ptr<TEvBlobStorage::TEvVPatchDiff> diff = testData.CreateVPatchDiff(partId, dataDiffCount,
                            {}, 0, partIdx);
                    std::unique_ptr<IEventHandle> handle;
                    handle = std::make_unique<IEventHandle>(testData.VPatchActorIds[partIdx], testData.EdgeActors[partIdx],
                            diff.release());
                    testData.Runtime.Send(handle.release());
                }

                for (ui32 partIdx = dataPartCount; partIdx < totalPartCount; ++partIdx) {
                    ui32 partId = partIdx + 1;
                    TBlob pullingBlob(testData.OriginalBlobId, partId, savedPartSet.Parts[partIdx].OwnedString);
                    PassPullingPart(testData, NKikimrProto::OK, pullingBlob, partIdx);
                }
            }

            for (ui32 partIdx = 0; partIdx < totalPartCount; ++partIdx) {
                ui32 partId = partIdx + 1;
                TBlob storingBlob(testData.PatchedBlobId, partId, resultPartSet.Parts[partIdx].OwnedString);
                PassStoringPart(testData, NKikimrProto::OK, storingBlob, partIdx);
            }

            testData.ForceEndTest();
            Y_UNUSED(partSet, resultPartSet, diffSet);
        }

       Y_UNIT_TEST(FullPatchTest) {
            return;
            ui32 dataSize = 2079;
            TString data = TString::Uninitialized(dataSize);
            Fill(data.begin(), data.vend(), 'a');

            ui32 diffSize = 31;
            UNIT_ASSERT(dataSize % (diffSize + 1) == diffSize);
            ui32 diffCount = dataSize / (diffSize + 1) + 1;
            TVector<TDiff> diffs;
            diffs.reserve(diffCount);
            ui32 left = 0;
            for (ui32 idx = 0; idx < diffCount; ++idx) {
                TString buffer = TString::Uninitialized(diffSize);
                Fill(buffer.begin(), buffer.vend(), 'a' + 1 + (idx % 25));
                diffs.emplace_back(buffer, left);
                left += diffSize + 1;
            }

            TBlobStorageGroupType type(TErasureType::Erasure4Plus2Block);
            MakeFullVPatchTest(type, data, diffs);
        }

       Y_UNIT_TEST(FullPatchTestXorDiffFasterVGetResult) {
            return;
            ui32 dataSize = 2079;
            TString data = TString::Uninitialized(dataSize);
            Fill(data.begin(), data.vend(), 'a');

            ui32 diffSize = 31;
            UNIT_ASSERT(dataSize % (diffSize + 1) == diffSize);
            ui32 diffCount = dataSize / (diffSize + 1) + 1;
            TVector<TDiff> diffs;
            diffs.reserve(diffCount);
            ui32 left = 0;
            for (ui32 idx = 0; idx < diffCount; ++idx) {
                TString buffer = TString::Uninitialized(diffSize);
                Fill(buffer.begin(), buffer.vend(), 'a' + 1 + (idx % 25));
                diffs.emplace_back(buffer, left);
                left += diffSize + 1;
            }

            TBlobStorageGroupType type(TErasureType::Erasure4Plus2Block);
            MakeFullVPatchTest(type, data, diffs, true);
        }

       Y_UNIT_TEST(FullPatchTestSpecialCase1) {
            return;
            ui32 dataSize = 100;
            TString data = TString::Uninitialized(dataSize);
            Fill(data.begin(), data.vend(), 'a');

            TVector<TDiff> diffs;
            diffs.reserve(2);
            diffs.emplace_back("b", 0);
            diffs.emplace_back("b", 99);

            TBlobStorageGroupType type(TErasureType::Erasure4Plus2Block);
            MakeFullVPatchTest(type, data, diffs, true);
        }
    }

} // NKikimr
