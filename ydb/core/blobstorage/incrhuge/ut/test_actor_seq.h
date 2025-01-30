#pragma once

class TTestActorSeq : public TActorBootstrapped<TTestActorSeq> {
public:
    struct TBlobInfo {
        TLogoBlobID LogoBlobId;
        ui64 Lsn;
        TString Data;
        TIncrHugeBlobId Id;
    };

    struct TTestActorState {
        // a set of blobs that were written and confirmed
        TMap<ui64, TBlobInfo> ConfirmedState;
        TMaybe<TIncrHugeBlobId> DeleteRequest;
        TMaybe<TBlobInfo> WriteRequest;
        ui64 Lsn = 0;
    };

    struct TPayload : public TWritePayload {
        ui64 Lsn;
        TLogoBlobID LogoBlobId;

        TPayload(ui64 lsn, const TLogoBlobID& logoBlobId)
            : Lsn(lsn)
            , LogoBlobId(logoBlobId)
        {}
    };

private:
    TActorId KeeperId;
    TTestActorState& State;
    ui8 Owner = 1;
    ui32 NumReadsPending = 0;
    TReallyFastRng32 Rng;
    const ui32 MinLen = 512 << 10;
    const ui32 MaxLen = 2048 << 10;
    const ui32 Generation;
    TManualEvent *Event;

public:
    TTestActorSeq(const TActorId& keeperId, TTestActorState& state, ui32 generation, TManualEvent *event)
        : KeeperId(keeperId)
        , State(state)
        , Rng(42)
        , Generation(generation)
        , Event(event)
    {}

    void Bootstrap(const TActorContext& ctx) {
        TVDiskID vdiskId(TGroupId::FromValue(~0), ~0, 'H', 'I', 'K');
        ctx.Send(KeeperId, new TEvIncrHugeInit(vdiskId, Owner, 0));
        Become(&TTestActorSeq::StateFunc);
    }

    void Handle(TEvIncrHugeInitResult::TPtr& ev, const TActorContext& ctx) {
        TEvIncrHugeInitResult *msg = ev->Get();
        Y_ABORT_UNLESS(msg->Status == NKikimrProto::OK);

        for (const auto& item : msg->Items) {
            LOG_DEBUG(ctx, NActorsServices::TEST, "Item# {Id# %016" PRIx64 " Lsn# %" PRIu64 " LogoBlobId# %s}",
                    item.Id, item.Lsn, TLogoBlobID(item.Meta.RawU64).ToString().data());
        }

        auto itemIt = msg->Items.begin();
        auto stateIt = State.ConfirmedState.begin();
        while (itemIt != msg->Items.end() || stateIt != State.ConfirmedState.end()) {
            const bool itemValid = itemIt != msg->Items.end();
            const bool stateValid = stateIt != State.ConfirmedState.end();
            if (!itemValid || stateIt->second.Lsn < itemIt->Lsn) {
                // we have an entry in confirmed state that is not in msg->Items; this happens only if we had delete
                // request in flight that succeeded, but we haven't received reply
                LOG_DEBUG(ctx, NActorsServices::TEST, "deleted Id# %016" PRIx64, stateIt->second.Id);
                Y_ABORT_UNLESS(State.DeleteRequest);
                Y_ABORT_UNLESS(*State.DeleteRequest == stateIt->second.Lsn);
                stateIt = State.ConfirmedState.erase(stateIt);
                continue;
            } else if (!stateValid || itemIt->Lsn < stateIt->second.Lsn) {
                // here we have entry in msg->Items that is not in confirmed state -- this happens only if we had
                // in-flight write request, but haven't got reply
                LOG_DEBUG(ctx, NActorsServices::TEST, "added Id# %016" PRIx64, itemIt->Id);
                Y_ABORT_UNLESS(State.WriteRequest);
                Y_ABORT_UNLESS(itemIt->Lsn == State.WriteRequest->Lsn);
                Y_ABORT_UNLESS(TLogoBlobID(itemIt->Meta.RawU64) == State.WriteRequest->LogoBlobId);
                State.WriteRequest->Id = itemIt->Id;
                stateIt = State.ConfirmedState.insert(stateIt, std::make_pair(itemIt->Lsn, *State.WriteRequest));
            }

            // LSNs are identical -- compare other values
            Y_ABORT_UNLESS(itemIt->Lsn == stateIt->second.Lsn);
            Y_ABORT_UNLESS(itemIt->Id == stateIt->second.Id);
            Y_ABORT_UNLESS(TLogoBlobID(itemIt->Meta.RawU64) == stateIt->second.LogoBlobId);

            // issue read request to verify data
            if (!stateIt->second.Data) {
                ctx.Send(KeeperId, new TEvIncrHugeRead(Owner, stateIt->second.Id, 0, 0), 0, stateIt->first);
                ++NumReadsPending;
            }

            // advance
            ++itemIt;
            ++stateIt;
        }

        State.DeleteRequest.Clear();
        State.WriteRequest.Clear();

        if (!NumReadsPending) {
            DoSomething(ctx);
        }
    }

    void Handle(TEvIncrHugeReadResult::TPtr& ev, const TActorContext& ctx) {
        TEvIncrHugeReadResult *msg = ev->Get();
        Y_ABORT_UNLESS(msg->Status == NKikimrProto::OK);
        auto it = State.ConfirmedState.find(ev->Cookie);
        Y_ABORT_UNLESS(it != State.ConfirmedState.end());
        Y_ABORT_UNLESS(it->second.Data == msg->Data);
        if (!--NumReadsPending) {
            DoSomething(ctx);
        }
    }

    void DoSomething(const TActorContext& ctx) {
        if (Event) {
            Event->Signal();
        }

        Y_ABORT_UNLESS(!State.DeleteRequest && !State.WriteRequest);

        ui32 deleteScore = State.ConfirmedState ? 3 : 0;
        ui32 writeScore = 5;
        ui32 total = deleteScore + writeScore;
        ui32 option = Rng() % total;
        if (option < deleteScore) {
            size_t num = Rng() % State.ConfirmedState.size();
            auto it = State.ConfirmedState.begin();
            std::advance(it, num);
            State.DeleteRequest = it->second.Lsn;
            ui64 seqNo = State.Lsn++;
            ctx.Send(KeeperId, new TEvIncrHugeDelete(Owner, seqNo, {it->second.Id}), 0, it->first);
            LOG_DEBUG(ctx, NActorsServices::TEST, "sent Delete Id# %016" PRIx64 " SeqNo# %" PRIu64, it->second.Id, seqNo);
            return;
        } else {
            option -= deleteScore;
        }
        if (option < writeScore) {
            ui32 len = Rng() % (MaxLen - MinLen + 1) + MinLen;
            TString data;
            data.resize(len);
            ui32 pattern = Rng();
            ui32 i;
            for (i = 0; i + 4 <= len; i += 4) {
                *(ui32 *)(data.data() + i) = pattern;
            }
            while (i < len) {
                *(ui8 *)(data.data() + i) = pattern;
                ++i;
            }

            TBlobInfo blob;
            blob.Lsn = State.Lsn++;
            blob.LogoBlobId = TLogoBlobID(12345678910UL, Generation, 1, 0, blob.Lsn, len);
            blob.Data = data;

            TBlobMetadata meta;
            memset(&meta, 0, sizeof(meta));
            memcpy(meta.RawU64, blob.LogoBlobId.GetRaw(), 3 * sizeof(ui64));

            ctx.Send(KeeperId, new TEvIncrHugeWrite(Owner, blob.Lsn, meta, std::move(data),
                    std::make_unique<TPayload>(blob.Lsn, blob.LogoBlobId)));
            State.WriteRequest = blob;
            LOG_DEBUG(ctx, NActorsServices::TEST, "sent Write Lsn# %" PRIu64 " LogoBlobId# %s", blob.Lsn,
                    blob.LogoBlobId.ToString().data());
            return;
        } else {
            option -= writeScore;
        }
        Y_ABORT();
    }

    void Handle(TEvIncrHugeDeleteResult::TPtr& ev, const TActorContext& ctx) {
        TEvIncrHugeDeleteResult *msg = ev->Get();
        LOG_DEBUG(ctx, NActorsServices::TEST, "finished Delete Status# %s Id# %016" PRIx64,
                NKikimrProto::EReplyStatus_Name(msg->Status).data(), ev->Cookie);
        Y_ABORT_UNLESS(msg->Status == NKikimrProto::OK);
        Y_ABORT_UNLESS(ev->Cookie == *State.DeleteRequest);
        State.DeleteRequest.Clear();
        State.ConfirmedState.erase(ev->Cookie);
        DoSomething(ctx);
    }

    void Handle(TEvIncrHugeWriteResult::TPtr& ev, const TActorContext& ctx) {
        TEvIncrHugeWriteResult *msg = ev->Get();
        TPayload *payload = static_cast<TPayload *>(msg->Payload.get());
        LOG_DEBUG(ctx, NActorsServices::TEST, "finished Write Status# %s Lsn# %" PRIu64 " LogoBlobId# %s Id# %016" PRIx64,
                NKikimrProto::EReplyStatus_Name(msg->Status).data(), payload->Lsn, payload->LogoBlobId.ToString().data(), msg->Id);
        Y_ABORT_UNLESS(msg->Status == NKikimrProto::OK);
        Y_ABORT_UNLESS(State.WriteRequest);
        State.WriteRequest->Id = msg->Id;
        State.ConfirmedState.emplace(payload->Lsn, *State.WriteRequest);
        State.WriteRequest.Clear();
        DoSomething(ctx);
    }

    STFUNC(StateFunc) {
        switch (const ui32 type = ev->GetTypeRewrite()) {
            HFunc(TEvIncrHugeInitResult, Handle);
            HFunc(TEvIncrHugeReadResult, Handle);
            HFunc(TEvIncrHugeDeleteResult, Handle);
            HFunc(TEvIncrHugeWriteResult, Handle);
            default:
                Y_ABORT("unexpected message 0x%08" PRIx32, type);
        }
    }
};
