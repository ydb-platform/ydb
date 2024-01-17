#pragma once

#include "defs.h"
#include <util/generic/string.h>
#include <util/generic/set.h>
#include <util/generic/map.h>
#include <util/generic/vector.h>
#include <util/generic/algorithm.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

namespace NKikimr {

    /* Buffer with gaps allows user to manipulate data obtained from Yard which can be partially unavailable.
     * Originally problem arises from cases when user reads data that was not yet written to disk. Consider
     * chunk with three blocks: block-0, block-1, block-2. User writes block-0, then block-2, while block-1 remains
     * untouched. Then, user reads data from block-0 and block-2. Request batcher contatenates those requests with
     * block-1 and reads all three blocks. But data in block-1 is invalid and cannot be read, so it contains a gap
     * marker at that place. Another case is when some part of data is corrupt and cannot be restored. In this case
     * vdisk receives gap marker and have to decide what to do -- crash, mark blob as corrupt and try to restore it
     * through replication, or do something else. */

    class TBufferWithGaps {
        TRcBuf Data;
        // <begin, size>
        TMap<ui32, ui32> Gaps;
        ui32 Offset; // Data's offset in Gaps space
        bool IsCommited;

        std::function<TString()> DebugInfoGenerator;

    public:
        TBufferWithGaps()
            : Offset(0)
            , IsCommited(false)
        {}

        TBufferWithGaps(ui32 offset)
            : Offset(offset)
            , IsCommited(false)
        {}

        TBufferWithGaps(ui32 offset, ui32 size)
            : Data(TRcBuf::Uninitialized(size))
            , Offset(offset)
            , IsCommited(false)
        {}

        TBufferWithGaps(TBufferWithGaps &&) = default;
        TBufferWithGaps &operator=(TBufferWithGaps &&) = default;

        void AddGap(ui32 begin, ui32 end) {
            // ensure gaps never overlap
            ui32 size = end - begin;
            auto f = Gaps.upper_bound(begin);
            if (!Gaps.empty() && f != Gaps.begin()) {
                auto prev = std::prev(f);
                if (prev->first + prev->second == begin) {
                    prev->second += size;
                    return;
                }
            }
            // add new gap
            Gaps.emplace(begin, size);
        }

        void SetData(TRcBuf&& data) {
            Data = std::move(data);
            IsCommited = true;
        }

        TRcBuf ToString() const {
            Y_VERIFY_S(IsReadable(), "returned data is corrupt (or was never written) and therefore could not be used safely, State# "
                    << PrintState());
            return Data;
        }

        TRcBuf Substr(ui32 offset, ui32 len) const {
            Y_VERIFY_S(IsReadable(offset, len), "returned data is corrupt (or was never written) at offset# %" << offset
                   << " len# " << len << " and therefore could not be used safely, State# " << PrintState());
            return TRcBuf(TRcBuf::Piece, Data.data() + offset, len, Data);
        }

        template<typename T>
        const T *DataPtr(ui32 offset, ui32 len = sizeof(T)) const {
            Y_VERIFY_S(IsReadable(offset, len), "returned data is corrupt (or was never written) at offset# " << offset
                   << " len# " << len << " and therefore could not be used safely, State# " << PrintState());
            return reinterpret_cast<T *>(Data.data() + offset);
        }

        ui8 *RawDataPtr(ui32 offset, ui32 len) {
            Y_ABORT_UNLESS(offset + len <= Data.size(), "Buffer has size# %zu less then requested offset# %" PRIu32
                    " len# %" PRIu32, Data.size(), offset, len);
            IsCommited = false;
            return reinterpret_cast<ui8 *>(Data.GetDataMut() + offset);
        }

        void Commit() {
            IsCommited = true;
        }

        bool IsReadable() const {
            Y_ABORT_UNLESS(IsCommited, "returned data was not commited");
            return Gaps.empty();
        }

        bool IsReadable(ui32 offset, ui32 len) const {
            Y_ABORT_UNLESS(IsCommited, "returned data was not commited");
            if (offset + len > Data.size()) {
                return false;
            }
            const ui32 begin = Offset + offset;
            const ui32 end = begin + len;

            auto f = Gaps.upper_bound(begin);
            if (Gaps.empty()) {
                return true;
            } else if (f == Gaps.begin()) {
                // intersection occurs only when 'end > f->Begin'
                //  [begin            ) end
                //                      [ f->Begin     ) f->End
                return end <= f->first;
            } else {
                // There are two possible intersections can occur:
                // 1. The gap before (there are always such one) f can have prev->end > begin
                // 2. f may be either Gaps.end() or the last element. If it is the last element, check that f->begin >= end
                // [ prev->begin     ) prev->end
                //                      [begin            ) end
                //                                          [ f->Begin     ) f->End
                auto prev = std::prev(f);
                return prev->first + prev->second <= begin && (f == Gaps.end() || end <= f->first);
            }
        }

        ui32 Size() const {
            return Data.size();
        }

        void Swap(TBufferWithGaps& other) {
            std::swap(Data, other.Data);
            Gaps.swap(other.Gaps);
            DoSwap(Offset, other.Offset);
            DoSwap(IsCommited, other.IsCommited);
        }

        void Clear() {
            Data = {};
            Gaps.clear();
            Offset = 0;
            IsCommited = false;
        }

        bool Empty() const {
            return Data.empty();
        }

        void Sanitize() const {
            if (Data.size()) {
                ui64 a = 0;
                for (const auto &gap : Gaps) {
                    ui64 b = gap.first - Offset;
                    if (a < b) {
                        ui64 size = gap.second;
                        Y_UNUSED(size);
                        REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(DataPtr<const char>(a, size), size);
                    }
                    a = b + gap.second;
                }
                ui64 b = Data.size();
                if (a < b) {
                    ui64 size = b - a;
                    Y_UNUSED(size);
                    REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(DataPtr<const char>(a, size), size);
                }
            }
        }

        void SetDebugInfoGenerator(const std::function<TString()>& debugInfoGenerator) {
            DebugInfoGenerator = debugInfoGenerator;
        }

        void SetDebugInfoGenerator(std::function<TString()>&& debugInfoGenerator) {
            DebugInfoGenerator = std::move(debugInfoGenerator);
        }

        TString PrintState() const {
            TStringStream str;
            str << "Offset# " << Offset;
            str << " Gaps# { ";
            for (auto [begin, size] : Gaps) {
                str << "{ Begin# " << begin << " Size# " << size << " } ";
            }
            str << "}";
            str << " IsCommitted# " << IsCommited;
            if (DebugInfoGenerator) {
                str << " DebugInfo# " << DebugInfoGenerator();
            }
            return str.Str();
        }
    };

} // NKikimr
