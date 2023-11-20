#pragma once

#include <library/cpp/digest/md5/md5.h>

class TTestActorConcurrent : public TActorBootstrapped<TTestActorConcurrent> {
public:
    struct TBlobInfo {
        unsigned char DataDigest[16];
        ui64 Lsn;
        TLogoBlobID LogoBlobId;
    };

    struct TWriteInfo {
        ui64 Lsn;
        TLogoBlobID LogoBlobId;
        TString Data;

        friend bool operator <(const TWriteInfo& left, const TWriteInfo& right) {
            return left.Lsn < right.Lsn || (left.Lsn == right.Lsn && left.LogoBlobId < right.LogoBlobId);
        }
    };

    struct TDeleteInfo {
        TIncrHugeBlobId Id;

        friend bool operator <(const TDeleteInfo& left, const TDeleteInfo& right) {
            return left.Id < right.Id;
        }
    };

    struct TTestActorState {
        TMap<TIncrHugeBlobId, TBlobInfo> ConfirmedState;
        TMap<std::pair<ui64, TLogoBlobID>, TBlobInfo> InFlightWrites;
        TMap<TIncrHugeBlobId, TBlobInfo> InFlightDeletes;
        THashMap<ui64, TBlobInfo> InFlightReads;
        ui64 Lsn = 0;
        ui64 BytesWritten = 0;
        TInstant StartTime = Now();
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
    TVDiskID VDiskId;
    TActorId KeeperId;
    TManualEvent *Event;
    ui8 Owner = 1;
    TTestActorState& State;
    const ui32 MinLen = 512 << 10;
    const ui32 MaxLen = 2048 << 10;
    TReallyFastRng32 Rng;
    ui32 ActionsTaken = 0;
    const ui32 NumActions;
    const ui32 Generation;

    const ui32 WriteScore;
    const ui32 DeleteScore;
    const ui32 ReadScore;

    struct TWriteRequestInfo {
        TBlobInfo BlobInfo;
    };

public:
    TTestActorConcurrent(const TActorId& keeperId, TManualEvent *event, TTestActorState& state, ui32 numActions,
            ui32 generation, ui32 writeScore = 10, ui32 deleteScore = 10, ui32 readScore = 5)
        : KeeperId(keeperId)
        , Event(event)
        , State(state)
        , Rng(1)
        , NumActions(numActions)
        , Generation(generation)
        , WriteScore(writeScore)
        , DeleteScore(deleteScore)
        , ReadScore(readScore)
    {}

    void Bootstrap(const TActorContext& ctx) {
        ctx.Send(KeeperId, new TEvIncrHugeInit(VDiskId, Owner, 0));

        Become(&TTestActorConcurrent::StateFunc);
    }

    void Handle(TEvIncrHugeInitResult::TPtr& ev, const TActorContext& ctx) {
        TEvIncrHugeInitResult *msg = ev->Get();
        Y_ABORT_UNLESS(msg->Status == NKikimrProto::OK);

        TVector<TEvIncrHugeInitResult::TItem> referenceItems;
        for (const auto& pair : State.ConfirmedState) {
            TBlobMetadata meta;
            memset(&meta, 0, sizeof(meta));
            memcpy(meta.RawU64, pair.second.LogoBlobId.GetRaw(), 3 * sizeof(ui64));
            referenceItems.push_back(TEvIncrHugeInitResult::TItem{
                    pair.first,
                    pair.second.Lsn,
                    meta
                });
        }
        std::sort(referenceItems.begin(), referenceItems.end(), [](const auto& left, const auto& right) {
                return left.Lsn < right.Lsn;
            });

        TStringStream str;
        str << "Reference# [";
        bool first = true;
        ui32 index = 0;
        for (const auto& ref : referenceItems) {
            if (first) {
                first = false;
            } else {
                str << " ";
            }
            if (++index == 100) {
                str << "...";
                break;
            }
            str << Sprintf("{Id# %016" PRIx64 " Lsn# %" PRIu64 "}", ref.Id, ref.Lsn);
        }
        str << "] Enumerated# [";
        first = true;
        index = 0;
        for (const auto& item : msg->Items) {
            if (first) {
                first = false;
            } else {
                str << " ";
            }
            if (++index == 100) {
                str << "...";
                break;
            }
            str << Sprintf("{Id# %016" PRIx64 " Lsn# %" PRIu64 "}", item.Id, item.Lsn);
        }
        str << "] InFlightDeletes# [";
        first = true;
        for (const auto& item : State.InFlightDeletes) {
            if (first) {
                first = false;
            } else {
                str << " ";
            }
            str << Sprintf("%016" PRIx64, item.first);
        }
        str << "]";
        LOG_DEBUG(ctx, NActorsServices::TEST, "finished Init %s", str.Str().data());

        auto refIt = referenceItems.begin();
        auto it = msg->Items.begin();
        while (refIt != referenceItems.end() || it != msg->Items.end()) {
            bool refEnd = refIt == referenceItems.end();
            bool itemsEnd = it == msg->Items.end();

            if ((!refEnd && !itemsEnd && refIt->Lsn < it->Lsn) || itemsEnd) {
                Y_ABORT("lost blob");
            } else if ((!refEnd && !itemsEnd && it->Lsn < refIt->Lsn) || refEnd) {
                // no matching reference item for returned one -- possibly write succeeded or delete hasn't completed yet
                auto& item = *it++;

                auto it = State.InFlightWrites.find(std::make_pair(item.Lsn, TLogoBlobID(item.Meta.RawU64)));
                if (it != State.InFlightWrites.end()) {
                    State.ConfirmedState.emplace(item.Id, std::move(it->second));
                    State.InFlightWrites.erase(it);
                } else {
                    auto it = State.InFlightDeletes.find(item.Id);
                    if (it != State.InFlightDeletes.end()) {
                        // restore deleted blob
                        State.ConfirmedState.emplace(it->first, std::move(it->second));
                   } else {
                        Y_ABORT("extra blob Lsn# %" PRIu64 " Id# %016" PRIx64, item.Lsn, item.Id);
                   }
                }
            } else {
                // advance both iterators, it's okay
                Y_ABORT_UNLESS(TLogoBlobID(refIt->Meta.RawU64) == TLogoBlobID(it->Meta.RawU64) &&
                        refIt->Id == it->Id && refIt->Lsn == it->Lsn);
                ++refIt;
                ++it;
            }
        }

        State.InFlightWrites.clear();
        State.InFlightReads.clear();
        State.InFlightDeletes.clear();

        DoSomething(ctx);
    }

    ui32 GetNumRequestsInFlight() const {
        return State.InFlightWrites.size() + State.InFlightReads.size() + State.InFlightDeletes.size();
    }

    void DoSomething(const TActorContext& ctx) {
        ui32 writeScore = WriteScore;
        ui32 readScore = State.ConfirmedState.empty() ? 0 : ReadScore;
        ui32 deleteScore = State.ConfirmedState.empty() ? 0 : DeleteScore;

        ++ActionsTaken;
        const bool timeToDie = ActionsTaken >= NumActions;
        ui32 numRequests = timeToDie ? 50 : 8;

        LOG_DEBUG(ctx, NActorsServices::TEST, "ActionsTaken# %" PRIu32, ActionsTaken);

        while (GetNumRequestsInFlight() < numRequests) {
            LOG_DEBUG(ctx, NActorsServices::TEST, "GetNumRequestsInFlight# %" PRIu32 " InFlightWritesSize# %zu",
                    GetNumRequestsInFlight(), State.InFlightWrites.size());

            // do not allow more than 2 reads at once or when its time to die
            if (State.InFlightReads.size() >= 2 || timeToDie) {
                readScore = 0;
            }

            bool exit = false;
            for (;;) {
                ui32 total = writeScore + readScore + deleteScore;
                if (!total) {
                    exit = true;
                    break;
                }
                ui32 option = Rng() % total;
                if (option < writeScore) {
                    SendWriteRequest(ctx);
                    break;
                } else {
                    option -= writeScore;
                }
                if (option < readScore) {
                    if (!SendReadRequest(ctx)) {
                        readScore = 0;
                        continue;
                    } else {
                        break;
                    }
                } else {
                    option -= readScore;
                }
                if (option < deleteScore) {
                    if (!SendDeleteRequest(ctx)) {
                        deleteScore = 0;
                        continue;
                    } else {
                        break;
                    }
                } else {
                    option -= deleteScore;
                }
                Y_ABORT("this point should be unreachable");
            }
            if (exit) {
                break;
            }
        }

        if (timeToDie) {
            Event->Signal();
            Die(ctx);
        }
    }

    void SendWriteRequest(const TActorContext& ctx) {
        ui32 len = MinLen + Rng() % (MaxLen - MinLen + 1);

        char *temp = (char *)alloca(len);
        ui32 pos;
        const ui32 pattern = Rng();
        for (pos = 0; pos + 4 <= len; pos += 4) {
            *(ui32 *)(temp + pos) = pattern;
        }
        for (; pos < len; ++pos) {
            temp[pos] = Rng();
        }

        TString data(temp, len);

        // allocate LSN for new record
        ui64 lsn = State.Lsn++;

        // create LogoBlobId for new record
        TLogoBlobID logoBlobId(1, Generation, 1, 0, lsn, len);
        TBlobMetadata meta;
        memset(&meta, 0, sizeof(meta));
        memcpy(meta.RawU64, logoBlobId.GetRaw(), 3 * sizeof(ui64));

        // send request
        ctx.Send(KeeperId, new TEvIncrHugeWrite(Owner, lsn, meta, TString(data), std::make_unique<TPayload>(lsn, logoBlobId)));

        LOG_DEBUG(ctx, NActorsServices::TEST, "sent Write LogoBlobId# %s Lsn# %" PRIu64 " NumReq# %" PRIu32,
                logoBlobId.ToString().data(), lsn, GetNumRequestsInFlight());

        // register in-flight write
        TBlobInfo blobInfo{{}, lsn, logoBlobId};
        MD5().Update(data.data(), data.size()).Final(blobInfo.DataDigest);
        State.InFlightWrites.emplace(std::make_pair(lsn, logoBlobId), std::move(blobInfo));
    }

    void Handle(TEvIncrHugeWriteResult::TPtr& ev, const TActorContext& ctx) {
        TEvIncrHugeWriteResult *msg = ev->Get();
        TPayload *payload = static_cast<TPayload *>(msg->Payload.get());

        LOG_DEBUG(ctx, NActorsServices::TEST, "finished Write Id# %016" PRIx64 " LogoBlobId# %s Lsn# %" PRIu64,
                msg->Id, payload->LogoBlobId.ToString().data(), payload->Lsn);

        Y_ABORT_UNLESS(msg->Status == NKikimrProto::OK, "Status# %s", NKikimrProto::EReplyStatus_Name(msg->Status).data());

        // find matching in-flight request
        auto it = State.InFlightWrites.find(std::make_pair(payload->Lsn, payload->LogoBlobId));
        Y_ABORT_UNLESS(it != State.InFlightWrites.end());

        State.BytesWritten += it->second.LogoBlobId.BlobSize();
        TDuration delta = Now() - State.StartTime;
        double speed = State.BytesWritten * 1000 * 1000 / delta.GetValue() / 1048576.0;
        LOG_INFO(ctx, NActorsServices::TEST, "BytesWritten# %" PRIu64 " MB ElapsedTime# %s Speed# %.2lf MB/s",
                (State.BytesWritten + 512 * 1024) / 1048576, delta.ToString().data(), speed);

        // insert new entry into confirmed state
        Y_ABORT_UNLESS(!State.ConfirmedState.count(msg->Id));
        State.ConfirmedState.emplace(msg->Id, std::move(it->second));

        // delete in-flight request
        State.InFlightWrites.erase(it);

        DoSomething(ctx);
    }

    bool SendReadRequest(const TActorContext& ctx) {
        if (!State.ConfirmedState) {
            return false;
        }

        // choose random item to read
        auto it = State.ConfirmedState.begin();
        std::advance(it, Rng() % State.ConfirmedState.size());

        // send request
        ctx.Send(KeeperId, new TEvIncrHugeRead(Owner, it->first, 0, 0), 0, State.Lsn);

        // store request info
        State.InFlightReads.emplace(State.Lsn, it->second);

        LOG_DEBUG(ctx, NActorsServices::TEST, "sent Read Id# %016" PRIx64 " Lsn# %" PRIu64, it->first, State.Lsn);
        ++State.Lsn;

        return true;
    }

    void Handle(TEvIncrHugeReadResult::TPtr& ev, const TActorContext& ctx) {
        TEvIncrHugeReadResult *msg = ev->Get();
        ui64 lsn = ev->Cookie;
        LOG_DEBUG(ctx, NActorsServices::TEST, "finished Read Status# %s Lsn# %" PRIu64,
                NKikimrProto::EReplyStatus_Name(msg->Status).data(), lsn);

        Y_ABORT_UNLESS(msg->Status == NKikimrProto::OK);

        auto it = State.InFlightReads.find(lsn);
        Y_ABORT_UNLESS(it != State.InFlightReads.end());
        Y_ABORT_UNLESS(MD5::CalcRaw(msg->Data) == TStringBuf(reinterpret_cast<const char *>(it->second.DataDigest), 16));
        State.InFlightReads.erase(it);

        DoSomething(ctx);
    }

    bool SendDeleteRequest(const TActorContext& ctx) {
        if (!State.ConfirmedState) {
            return false;
        }

        // choose random item to delete
        auto it = State.ConfirmedState.begin();
        std::advance(it, Rng() % State.ConfirmedState.size());

        // send request to keeper
        ctx.Send(KeeperId, new TEvIncrHugeDelete(Owner, State.Lsn++, {it->first}), 0, it->first);
        LOG_DEBUG(ctx, NActorsServices::TEST, "sent Delete Id# %016" PRIx64 " NumReq# %" PRIu32, it->first,
                GetNumRequestsInFlight());

        // move item from confirmed state into in-flight delete
        Y_ABORT_UNLESS(it != State.ConfirmedState.end());
        State.InFlightDeletes.emplace(it->first, std::move(it->second));
        State.ConfirmedState.erase(it);

        return true;
    }

    void Handle(TEvIncrHugeDeleteResult::TPtr& ev, const TActorContext& ctx) {
        TEvIncrHugeDeleteResult *msg = ev->Get();

        TIncrHugeBlobId id = ev->Cookie;

        LOG_DEBUG(ctx, NActorsServices::TEST, "finished Delete Status# %s Id# %016" PRIx64,
                NKikimrProto::EReplyStatus_Name(msg->Status).data(), id);

        Y_ABORT_UNLESS(msg->Status == NKikimrProto::OK);

        // remove item from delete in-flight map
        auto deleteIt = State.InFlightDeletes.find(id);
        Y_ABORT_UNLESS(deleteIt != State.InFlightDeletes.end());
        State.InFlightDeletes.erase(deleteIt);

        DoSomething(ctx);
    }

    STFUNC(StateFunc) {
        switch (const ui32 type = ev->GetTypeRewrite()) {
            HFunc(TEvIncrHugeInitResult, Handle);
            HFunc(TEvIncrHugeWriteResult, Handle);
            HFunc(TEvIncrHugeReadResult, Handle);
            HFunc(TEvIncrHugeDeleteResult, Handle);
            default:
                Y_ABORT("unexpected message 0x%08" PRIx32, type);
        }
    }
};
