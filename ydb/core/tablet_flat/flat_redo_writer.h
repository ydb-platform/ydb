#pragma once

#include "flat_redo_layout.h"
#include "flat_update_op.h"
#include "flat_util_binary.h"
#include "flat_sausage_solid.h"
#include "util_basics.h"

#include <util/stream/buffer.h>
#include <util/system/sanitizers.h>
#include <util/generic/list.h>

namespace NKikimr {
namespace NTable {
namespace NRedo {

    struct IAnnex {
        struct TLimit {
            ui32 MinExternSize;
            ui32 MaxExternSize;

            bool IsExtern(ui32 size) const noexcept {
                return size >= MinExternSize && size <= MaxExternSize;
            }
        };

        struct TResult {
            TResult() = default;

            TResult(ui32 ref) : Ref(ref) { }

            explicit operator bool() const noexcept
            {
                return Ref != Max<ui32>();
            }

            ui32 Ref = Max<ui32>();
        };

        virtual ~IAnnex() = default;

        /* Limit(...) is the temporary hack required for calculation exact
            space occupied by event in redo log buffer. It will work with
            single family tables but it is not scaled to multiple families.
         */

        virtual TLimit Limit(ui32 table) noexcept = 0;
        virtual TResult Place(ui32 table, TTag, TArrayRef<const char>) noexcept = 0;
    };

    class TWriter {
    private:
        struct TOutputMark {
        };

    public:
        using TOpsRef = TArrayRef<const TUpdateOp>;
        using IOut = TOutputMark;

        TWriter() = default;

        explicit operator bool() const noexcept
        {
            return bool(Events);
        }

        size_t Bytes() const noexcept
        {
            return TotalSize;
        }

        TWriter& EvBegin(ui32 tail, ui32 head, ui64 serial, ui64 stamp)
        {
            Y_ABORT_UNLESS(tail <= head, "Invalid ABI/API evolution span");
            Y_ABORT_UNLESS(serial > 0, "Serial of EvBegin starts with 1");

            const ui32 size = sizeof(TEvBegin_v1);

            TEvBegin_v1 ev{ { ERedo::Begin, 0, 0x8000, size },
                                tail, head, serial, stamp };

            auto out = Begin(size);

            Write(out, &ev, sizeof(ev));

            return Flush(size);
        }

        TWriter& EvFlush(ui32 table, ui64 stamp, TEpoch epoch)
        {
            const ui32 size = sizeof(TEvFlush);

            TEvFlush ev{ { ERedo::Flush, 0, 0x8000, size },
                                table, 0, stamp, epoch.ToRedoLog() };

            auto out = Begin(size);

            Write(out, &ev, sizeof(ev));

            return Flush(size);
        }

        TWriter& EvAnnex(TArrayRef<const NPageCollection::TGlobId> blobs)
        {
            using namespace  NUtil::NBin;

            Y_ABORT_UNLESS(blobs.size() <= Max<ui32>(), "Too large blobs catalog");

            const ui32 size = sizeof(TEvAnnex) + SizeOf(blobs);

            TEvAnnex ev{ { ERedo::Annex, 0, 0x8000, size }, ui32(blobs.size()) };

            auto out = Begin(size);

            Write(out, &ev, sizeof(ev));
            Write(out, static_cast<const void*>(blobs.begin()), SizeOf(blobs));

            return Flush(size);
        }

        template<class TCallback>
        TWriter& EvUpdate(ui32 table, ERowOp rop, TRawVals key, TOpsRef ops, ERedo tag, ui32 tailSize, TCallback&& tailCallback)
        {
            if (TCellOp::HaveNoOps(rop) && ops) {
                Y_ABORT("Given ERowOp cannot have update operations");
            } else if (key.size() + ops.size() > Max<ui16>()) {
                Y_ABORT("Too large key or too many operations in one ops");
            }

            const ui32 size = sizeof(TEvUpdate) + tailSize + CalcSize(key, ops);
            auto out = Begin(size);

            TEvUpdate ev{ { tag, 0, 0x8000, size },
                                table, rop, 0, ui16(key.size()), ui16(ops.size()) };

            Write(out, &ev, sizeof(ev));

            tailCallback(out);

            Write(out, key);
            Write(out, ops);

            return Flush(size);
        }

        TWriter& EvUpdate(ui32 table, ERowOp rop, TRawVals key, TOpsRef ops, TRowVersion rowVersion)
        {
            if (rowVersion > TRowVersion::Min()) {
                return EvUpdate(table, rop, key, ops, ERedo::UpdateV, sizeof(TEvUpdateV), [&](auto& out) {
                    TEvUpdateV tail{ rowVersion.Step, rowVersion.TxId };
                    Write(out, &tail, sizeof(tail));
                });
            } else {
                return EvUpdate(table, rop, key, ops, ERedo::Update, 0, [](auto&){
                    // nothing
                });
            }
        }

        TWriter& EvUpdateTx(ui32 table, ERowOp rop, TRawVals key, TOpsRef ops, ui64 txId)
        {
            return EvUpdate(table, rop, key, ops, ERedo::UpdateTx, sizeof(TEvUpdateTx),
                [&](auto& out) {
                    TEvUpdateTx tail{ txId };
                    Write(out, &tail, sizeof(tail));
                });
        }

        TWriter& EvRemoveTx(ui32 table, ui64 txId)
        {
            const ui32 size = sizeof(TEvRemoveTx);

            TEvRemoveTx ev{ { ERedo::RemoveTx, 0, 0x8000, size },
                            table, 0, txId };

            auto out = Begin(size);

            Write(out, &ev, sizeof(ev));

            return Flush(size);
        }

        TWriter& EvCommitTx(ui32 table, ui64 txId, TRowVersion rowVersion)
        {
            const ui32 size = sizeof(TEvCommitTx);

            TEvCommitTx ev{ { ERedo::CommitTx, 0, 0x8000, size },
                            table, 0, txId, rowVersion.Step, rowVersion.TxId };

            auto out = Begin(size);

            Write(out, &ev, sizeof(ev));

            return Flush(size);
        }

        TWriter& Join(TWriter &&log)
        {
            TotalSize += std::exchange(log.TotalSize, 0);
            Events.append(log.Events);
            log.Events.clear();

            return *this;
        }

        TString Finish() && noexcept
        {
            TString events;
            events.swap(Events);

            NSan::CheckMemIsInitialized(events.data(), events.size());

            TotalSize = 0;
            return events;
        }

    private:
        TOutputMark Begin(size_t bytes)
        {
            if ((Events.capacity() - Events.size()) < bytes) {
                Events.reserve(FastClp2(Events.size() + bytes));
            }

            return TOutputMark{};
        }

        TWriter& Flush(size_t bytes)
        {
            TotalSize += bytes;

            Y_ABORT_UNLESS(Events.size() == TotalSize, "Got an inconsistent redo entry size");

            return *this;
        }

        ui32 CalcSize(TRawVals key) const noexcept
        {
            ui32 size = 0;

            for (const auto &one: key)
                size += sizeof(TValue) + one.Size();

            return size;
        }

        ui32 CalcSize(TRawVals key, TOpsRef ops) const noexcept
        {
            ui32 size = CalcSize(key);

            for (const auto &one : ops) {
                size += sizeof(TUpdate) + one.Value.Size();
            }

            return size;
        }

        void Write(IOut &out, TRawVals key) noexcept
        {
            for (const auto &one : key) {
                /* The only way of converting nulls in keys now */

                const ui16 type = one.IsEmpty() ? 0 : one.Type();

                TValue keyEl = { type, one.Size() };

                Write(out, &keyEl, sizeof(keyEl));
                Write(out, one.Data(), one.Size());
            }
        }

        void Write(IOut &out, TOpsRef ops) noexcept
        {
            for (const auto &one: ops) {
                /* Log enty cannot represent this ECellOp types with payload */

                Y_ABORT_UNLESS(!(one.Value && TCellOp::HaveNoPayload(one.Op)));

                const ui16 type = one.Value.Type();

                if (one.Value.IsEmpty()) {
                    // Log entry cannot recover null value type, since we
                    // store null values using a special 0 type id.
                    Y_ABORT_UNLESS(type == 0, "Cannot write typed null values");
                    // Null value cannot have ECellOp::Set as its op, since we
                    // don't have the necessary type id, instead we expect
                    // it to be either ECellOp::Null or ECellOp::Reset.
                    Y_ABORT_UNLESS(one.Op != ECellOp::Set, "Cannot write ECellOp::Set with a null value");
                }

                Write(out, one.Op, one.Tag, type, one.Value.AsRef());
            }
        }

        void Write(IOut &out, TCellOp cellOp, TTag tag, ui16 type, TArrayRef<const char> raw)
        {
            TUpdate up = { cellOp, tag, { type , ui32(raw.size()) } };

            Write(out, &up, sizeof(up));
            Write(out, raw.data(), raw.size());
        }

        void Write(IOut &, const void *ptr, size_t size)
        {
            NSan::CheckMemIsInitialized(ptr, size);
            Events.append(reinterpret_cast<const char*>(ptr), size);
        }

    private:
        size_t TotalSize = 0;
        TString Events;
    };

}
}
}
