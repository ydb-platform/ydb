#pragma once

#include "flat_update_op.h"
#include "flat_redo_layout.h"
#include "flat_sausage_solid.h"
#include "util_fmt_abort.h"
#include "util_basics.h"
#include "util_deref.h"

namespace NKikimr {
namespace NTable {
namespace NRedo {

    class TReader {
    public:
        TReader(TArrayRef<const char> plain): Plain(plain), On(Plain.data()) { }

        TArrayRef<const char> Next() noexcept
        {
            if (On >= Plain.end()) {
                return { nullptr, size_t(0) };
            } else if (size_t(Plain.end() - On) >= sizeof(TChunk_Legacy)) {
                auto size = reinterpret_cast<const TChunk_Legacy*>(On)->Size;

                if (size >= sizeof(TChunk_Legacy) && (size <= Plain.end() - On))
                    return { std::exchange(On, On + size), size };
            }

            Y_ABORT("Damaged or invalid plainfied db redo log");
        }

    private:
        TArrayRef<const char> Plain;
        const char *On = nullptr;
    };

    template<typename TBase>
    class TPlayer {
    public:
        TPlayer(TBase &base) : Base(base) { }

        void Replay(TArrayRef<const char> plain)
        {
            TReader iter(plain);

            while (auto chunk = iter.Next()) {
                auto *legacy = reinterpret_cast<const TChunk_Legacy*>(chunk.data());

                if (Y_LIKELY(legacy->RootId & (ui32(1) << 31))) {
                    auto *label = reinterpret_cast<const TChunk*>(chunk.data());

                    Handle(label, chunk);
                } else {
                    // legacy records, required for (Evolution < 12)
                    HandleLegacy(legacy, chunk);
                }
            }
        }

    private:
        void Handle(const TChunk* label, const TArrayRef<const char> chunk)
        {
            switch (label->Event) {
                case ERedo::Noop:
                    return;
                case ERedo::Update:
                case ERedo::UpdateV:
                    return DoUpdate(chunk);
                case ERedo::Flush:
                    return DoFlush(chunk);
                case ERedo::Begin:
                    return DoBegin(chunk);
                case ERedo::Annex:
                    return DoAnnex(chunk);
                case ERedo::UpdateTx:
                    return DoUpdateTx(chunk);
                case ERedo::RemoveTx:
                    return DoRemoveTx(chunk);
                case ERedo::CommitTx:
                    return DoCommitTx(chunk);
                case ERedo::Erase:
                    // Not used in current log format
                    break;
            }

            Y_ABORT("Unexpected rodo log chunk type");
        }

        void HandleLegacy(const TChunk_Legacy* label, const TArrayRef<const char> chunk)
        {
            if (!Base.NeedIn(label->RootId)) {
                return;
            }

            switch (label->Op) {
                case ERedo::Noop:
                    return;
                case ERedo::Update:
                    return DoUpdateLegacy(chunk);
                case ERedo::Erase:
                    return DoEraseLegacy(chunk);
                case ERedo::Flush:
                    return DoFlushLegacy(chunk);
                case ERedo::Begin:
                case ERedo::Annex:
                case ERedo::UpdateV:
                case ERedo::UpdateTx:
                case ERedo::RemoveTx:
                case ERedo::CommitTx:
                    // Not used in legacy log format
                    break;
            }

            Y_ABORT("Unexpected rodo log legacy chunk type");
        }

    private:
        void DoBegin(const TArrayRef<const char> chunk)
        {
            if (chunk.size() < sizeof(TEvBegin_v0)) {
                Y_Fail("EvBegin event is tool small, " << chunk.size() << "b");
            } else if (chunk.size() < sizeof(TEvBegin_v1)) {
                auto *ev = reinterpret_cast<const TEvBegin_v0*>(chunk.data());

                Base.DoBegin(ev->Tail, ev->Head, 0 /* scn */, 0 /* stamp */);
            } else {
                auto *ev = reinterpret_cast<const TEvBegin_v1*>(chunk.data());

                Y_ABORT_UNLESS(ev->Serial > 0, "Invalid serial in EvBegin record");

                Base.DoBegin(ev->Tail, ev->Head, ev->Serial, ev->Stamp);
            }
        }

        void DoAnnex(const TArrayRef<const char> chunk)
        {
            Y_ABORT_UNLESS(chunk.size() >= sizeof(TEvAnnex));

            using TGlobId = TStdPad<NPageCollection::TGlobId>;

            auto *ev = reinterpret_cast<const TEvAnnex*>(chunk.data());
            auto *raw = reinterpret_cast<const TGlobId*>(ev + 1);

            Base.DoAnnex({ raw, ev->Items });
        }

        void DoFlush(const TArrayRef<const char> chunk)
        {
            Y_ABORT_UNLESS(chunk.size() >= sizeof(TEvFlush));

            auto *ev = reinterpret_cast<const TEvFlush*>(chunk.begin());

            if (Base.NeedIn(ev->Table))
                Base.DoFlush(ev->Table, ev->Stamp, TEpoch(ev->Epoch));
        }

        void DoUpdate(const TArrayRef<const char> chunk)
        {
            auto *ev = reinterpret_cast<const TEvUpdate*>(chunk.data());

            if (Base.NeedIn(ev->Table)) {
                const char *buf = chunk.begin() + sizeof(*ev);

                TRowVersion rowVersion;
                if (ev->Label.Event == ERedo::UpdateV) {
                    auto *v = reinterpret_cast<const TEvUpdateV*>(buf);
                    rowVersion.Step = v->RowVersionStep;
                    rowVersion.TxId = v->RowVersionTxId;
                    buf += sizeof(*v);
                } else {
                    rowVersion = TRowVersion::Min();
                }

                buf += ReadKey(buf, chunk.end() - buf, ev->Keys);
                buf += ReadOps(buf, chunk.end() - buf, ev->Ops);

                Base.DoUpdate(ev->Table, ev->Rop, KeyVec, OpsVec, rowVersion);
            }
        }

        void DoUpdateTx(const TArrayRef<const char> chunk)
        {
            auto *ev = reinterpret_cast<const TEvUpdate*>(chunk.data());

            if (Base.NeedIn(ev->Table)) {
                const char *buf = chunk.begin() + sizeof(*ev);

                auto *v = reinterpret_cast<const TEvUpdateTx*>(buf);
                buf += sizeof(*v);

                buf += ReadKey(buf, chunk.end() - buf, ev->Keys);
                buf += ReadOps(buf, chunk.end() - buf, ev->Ops);

                Base.DoUpdateTx(ev->Table, ev->Rop, KeyVec, OpsVec, v->TxId);
            }
        }

        void DoCommitTx(const TArrayRef<const char> chunk)
        {
            auto *ev = reinterpret_cast<const TEvCommitTx*>(chunk.data());

            if (Base.NeedIn(ev->Table)) {
                TRowVersion rowVersion(ev->RowVersionStep, ev->RowVersionTxId);

                Base.DoCommitTx(ev->Table, ev->TxId, rowVersion);
            }
        }

        void DoRemoveTx(const TArrayRef<const char> chunk)
        {
            auto *ev = reinterpret_cast<const TEvRemoveTx*>(chunk.data());

            if (Base.NeedIn(ev->Table)) {
                Base.DoRemoveTx(ev->Table, ev->TxId);
            }
        }

        void DoUpdateLegacy(const TArrayRef<const char> chunk)
        {
            const char *buf = chunk.begin();
            auto *op = (const TEvUpdate_Legacy*)buf;
            buf += sizeof(*op);
            buf += ReadKey(buf, chunk.end() - buf, op->Keys);
            buf += ReadOps(buf, chunk.end() - buf, op->Ops);

            Base.DoUpdate(op->OpHeader.RootId, ERowOp::Upsert, KeyVec, OpsVec, TRowVersion::Min());
        }

        void DoEraseLegacy(const TArrayRef<const char> chunk)
        {
            const char *buf = chunk.begin();
            auto *op = (const TEvErase_Legacy*)buf;
            buf += sizeof(*op);
            buf += ReadKey(buf, chunk.end() - buf, op->Keys);

            Base.DoUpdate(op->OpHeader.RootId, ERowOp::Erase, KeyVec, { }, TRowVersion::Min());
        }

        void DoFlushLegacy(const TArrayRef<const char> chunk)
        {
            Y_ABORT_UNLESS(chunk.size() >= sizeof(TEvFlush_Legacy));

            auto *op = reinterpret_cast<const TEvFlush_Legacy*>(chunk.begin());

            Base.DoFlush(op->Hdr.RootId, op->TxStamp, TEpoch(op->Epoch));
        }

        ui32 ReadKey(const char* buf, ui32 maxSz, ui32 count)
        {
            const char *bufStart = buf;
            const char *end = buf + maxSz;
            KeyVec.clear();
            KeyVec.resize(count);

            for (ui32 ki = 0; ki < count; ++ki) {
                buf += ReadValue(buf, end - buf, KeyVec[ki]);
            }
            return buf - bufStart;
        }

        ui32 ReadValue(const char* buf, ui32 maxSz, TRawTypeValue& val)
        {
            Y_ABORT_UNLESS(maxSz >= sizeof(TValue), "Buffer to small");
            const TValue* vp = (const TValue*)buf;
            Y_ABORT_UNLESS(maxSz >= sizeof(TValue) + vp->Size, "Value size execeeds the buffer size");
            val = vp->IsNull() ? TRawTypeValue() : TRawTypeValue(vp + 1, vp->Size, vp->TypeId);
            return sizeof(TValue) + vp->Size;
        }

        ui32 ReadOps(const char* buf, ui32 maxSz, ui32 count)
        {
            const char *bufStart = buf;
            const char *end = buf + maxSz;
            OpsVec.clear();
            OpsVec.resize(count);

            for (ui32 ui = 0; ui < count; ++ui) {
                buf += ReadOneOp(buf, end - buf, OpsVec[ui]);
            }
            return buf - bufStart;
        }

        ui32 ReadOneOp(const char* buf, ui32 maxSz, TUpdateOp& uo)
        {
            Y_ABORT_UNLESS(maxSz >= sizeof(TUpdate), "Buffer to small");
            const TUpdate* up = (const TUpdate*)buf;
            Y_ABORT_UNLESS(maxSz >= sizeof(TUpdate) + up->Val.Size, "Value size execeeds the buffer size");
            bool null = TCellOp::HaveNoPayload(up->CellOp) || up->Val.IsNull();
            uo = TUpdateOp(up->Tag, up->CellOp, null ? TRawTypeValue() : TRawTypeValue(up + 1, up->Val.Size, up->Val.TypeId));

            Y_ABORT_UNLESS(up->CellOp == ELargeObj::Inline || (up->CellOp == ELargeObj::Extern && up->Val.Size == sizeof(ui32)));

            return sizeof(TUpdate) + up->Val.Size;
        }

    private:
        TBase &Base;
        TVector<TRawTypeValue> KeyVec;
        TVector<TUpdateOp> OpsVec;
    };

}
}
}
