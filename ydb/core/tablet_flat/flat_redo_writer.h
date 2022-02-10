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
        struct TSizeInfo {
            ui32 Size;
            IAnnex::TLimit Limit;
        };

    public:
        using TOpsRef = TArrayRef<const TUpdateOp>;
        using IOut = IOutputStream;

        TWriter(IAnnex *annex = nullptr, ui32 edge = 1024)
            : Edge(edge)
            , Annex(annex)
        {

        }

        explicit operator bool() const noexcept
        {
            return bool(Events);
        }

        size_t Bytes() const noexcept
        {
            return TotalSize;
        }

        TList<TString> Unwrap() noexcept
        {
            return TotalSize = 0, std::move(Events);
        }

        TWriter& EvBegin(ui32 tail, ui32 head, ui64 serial, ui64 stamp)
        {
            Y_VERIFY(tail <= head, "Invalid ABI/API evolution span");
            Y_VERIFY(serial > 0, "Serial of EvBegin starts with 1");

            const ui32 size =  sizeof(TEvBegin_v1);

            TEvBegin_v1 ev{ { ERedo::Begin, 0, 0x8000, size },
                                tail, head, serial, stamp };

            void* evBegin = &ev; 
            return Push(TString(NUtil::NBin::ToByte(evBegin), size), size); 
        }

        TWriter& EvFlush(ui32 table, ui64 stamp, TEpoch epoch)
        {
            const ui32 size = sizeof(TEvFlush);

            TEvFlush ev{ { ERedo::Flush, 0, 0x8000, size },
                                table, 0, stamp, epoch.ToRedoLog() };
            void* evBegin = &ev; 
            return Push(TString(NUtil::NBin::ToByte(evBegin), size), size); 
        }

        TWriter& EvAnnex(TArrayRef<const NPageCollection::TGlobId> blobs)
        {
            using namespace  NUtil::NBin;

            Y_VERIFY(blobs.size() <= Max<ui32>(), "Too large blobs catalog");

             const ui32 size = sizeof(TEvAnnex) + SizeOf(blobs);

            TEvAnnex ev{ { ERedo::Annex, 0, 0x8000, size }, ui32(blobs.size()) };

            auto out = Begin(size);

            Write(out, &ev, sizeof(ev));
            Write(out, static_cast<const void*>(blobs.begin()), SizeOf(blobs));

            return Push(std::move(out.Str()), size);
        }

        template<class TCallback>
        TWriter& EvUpdate(ui32 table, ERowOp rop, TRawVals key, TOpsRef ops, ERedo tag, ui32 tailSize, TCallback&& tailCallback, bool isDelta = false)
        {
            if (TCellOp::HaveNoOps(rop) && ops) {
                Y_FAIL("Given ERowOp cannot have update operations");
            } else if (key.size() + ops.size() > Max<ui16>()) {
                Y_FAIL("Too large key or too many operations in one ops");
            }

            const auto sizeInfo = CalcSize(key, ops, table, isDelta);

            const ui32 size = sizeof(TEvUpdate) + tailSize + sizeInfo.Size;
            auto out = Begin(size);

            TEvUpdate ev{ { tag, 0, 0x8000, size },
                                table, rop, 0, ui16(key.size()), ui16(ops.size()) };

            Write(out, &ev, sizeof(ev));

            tailCallback(out);

            Write(out, key);
            Write(out, ops, table, sizeInfo.Limit);

            return Push(std::move(out.Str()), size);
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
                },
                /* isDelta */ true);
        }

        TWriter& EvRemoveTx(ui32 table, ui64 txId)
        {
            const ui32 size = sizeof(TEvRemoveTx);

            TEvRemoveTx ev{ { ERedo::RemoveTx, 0, 0x8000, size },
                            table, 0, txId };

            return Push(TString(NUtil::NBin::ToByte(&ev), size), size);
        }

        TWriter& EvCommitTx(ui32 table, ui64 txId, TRowVersion rowVersion)
        {
            const ui32 size = sizeof(TEvCommitTx);

            TEvCommitTx ev{ { ERedo::CommitTx, 0, 0x8000, size },
                            table, 0, txId, rowVersion.Step, rowVersion.TxId };

            return Push(TString(NUtil::NBin::ToByte(&ev), size), size);
        }

        TWriter& Join(TWriter &log)
        {
            TotalSize += std::exchange(log.TotalSize, 0);
            Events.splice(Events.end(), log.Events);

            return *this;
        }

        TString Dump() const noexcept
        {
            auto out = Begin(TotalSize);

            for (auto one : Events)
                out.Write(one);

            Y_VERIFY(out.Str().size() == TotalSize);

            NSan::CheckMemIsInitialized(out.Str().data(), out.Str().size());

            return std::move(out.Str());
        }

    private:
        static TStringStream Begin(size_t bytes)
        {
            TStringStream out;

            return out.Reserve(bytes), out;
        }

        TWriter& Push(TString buf, size_t bytes)
        {
            Y_VERIFY(buf.size() == bytes, "Got an invalid redo entry buffer");

            TotalSize += buf.size();

            Events.emplace_back(std::move(buf));

            return *this;
        }

        IAnnex::TLimit GetLimit(ui32 table, bool isDelta) const noexcept
        {
            IAnnex::TLimit limit;
            // FIXME: we cannot handle blob references during scans, so we
            //        avoid creating large objects when they are in deltas
            if (!isDelta && Annex && table != Max<ui32>()) {
                limit = Annex->Limit(table);
            } else {
                limit = { Max<ui32>(), Max<ui32>() };
            }
            limit.MinExternSize = Max(limit.MinExternSize, Edge);
            return limit;
        }

        TSizeInfo CalcSize(TRawVals key, TOpsRef ops, ui32 table, bool isDelta) const noexcept
        {
            auto limit = GetLimit(table, isDelta);

            ui32 size = 0;

            for (const auto &one : ops) {
                /* hack for annex blobs, its replaced by 4-byte reference */
                auto valueSize = one.Value.Size();
                if (Annex && table != Max<ui32>() && limit.IsExtern(valueSize)) {
                    valueSize = 4;
                }

                size += sizeof(TUpdate) + valueSize;
            }

            size += CalcSize(key);

            return TSizeInfo{ size, limit };
        }

        ui32 CalcSize(TRawVals key) const noexcept
        {
            ui32 size = 0;

            for (const auto &one: key)
                size += sizeof(TValue) + one.Size();

            return size;
        }

        void Write(IOut &out, TRawVals key) const noexcept
        {
            for (const auto &one : key) {
                /* The only way of converting nulls in keys now */

                const ui16 type = one.IsEmpty() ? 0 : one.Type();

                TValue keyEl = { type, one.Size() };

                Write(out, &keyEl, sizeof(keyEl));
                Write(out, one.Data(), one.Size());
            }
        }

        void Write(IOut &out, TOpsRef ops, ui32 table, const IAnnex::TLimit &limit) const noexcept
        {
            for (const auto &one: ops) {
                /* Log enty cannot represent this ECellOp types with payload */

                Y_VERIFY(!(one.Value && TCellOp::HaveNoPayload(one.Op)));

                /* Log entry cannot recover nulls written as ECellOp::Set with
                    null cell. Nulls was encoded with hacky TypeId = 0, but
                    the correct way is to use ECellOp::Null.
                 */

                const ui16 type = one.Value ? one.Value.Type() : 0;

                auto cellOp = one.NormalizedCellOp();

                if (cellOp != ELargeObj::Inline) {
                    Y_FAIL("User supplied cell value has an invalid ECellOp");
                } else if (auto got = Place(table, limit, one.Tag, one.AsRef())) {
                    const auto payload = NUtil::NBin::ToRef(got.Ref);

                    Write(out, cellOp = ELargeObj::Extern, one.Tag, type, payload);

                } else {
                    Write(out, cellOp, one.Tag, type, one.Value.AsRef());
                }
            }
        }

        IAnnex::TResult Place(ui32 table, const IAnnex::TLimit &limit, TTag tag, TArrayRef<const char> raw) const noexcept
        {
            if (Annex && table != Max<ui32>() && limit.IsExtern(raw.size())) {
                return Annex->Place(table, tag, raw);
            } else {
                return { };
            }
        }

        static void Write(IOut &out, TCellOp cellOp, TTag tag, ui16 type, TArrayRef<const char> raw)
        {
            TUpdate up = { cellOp, tag, { type , ui32(raw.size()) } };

            Write(out, &up, sizeof(up));
            Write(out, raw.data(), raw.size());
        }

        static void Write(IOut &out, const void *ptr, size_t size)
        {
            NSan::CheckMemIsInitialized(ptr, size);

            out.Write(ptr, size);
        }

    private:
        const ui32 Edge = 1024;
        IAnnex * const Annex = nullptr;
        size_t TotalSize = 0;
        TList<TString> Events;
    };

}
}
}
