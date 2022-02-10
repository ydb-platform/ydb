#pragma once
 
#include <util/stream/walk.h>
#include <util/system/types.h>
#include <util/generic/string.h>
#include <library/cpp/actors/util/rope.h>
#include <library/cpp/actors/wilson/wilson_trace.h>

namespace NActors {
    class IEventHandle; 

    struct TConstIoVec { 
        const void* Data; 
        size_t Size; 
    }; 

    struct TIoVec { 
        void* Data; 
        size_t Size; 
    }; 

    class TEventSerializedData
       : public TThrRefBase
    {
        TRope Rope;
        bool ExtendedFormat = false;

    public:
        TEventSerializedData() = default;

        TEventSerializedData(TRope&& rope, bool extendedFormat)
            : Rope(std::move(rope))
            , ExtendedFormat(extendedFormat)
        {}

        TEventSerializedData(const TEventSerializedData& original, TString extraBuffer)
            : Rope(original.Rope)
            , ExtendedFormat(original.ExtendedFormat)
        {
            Append(std::move(extraBuffer));
        }

        TEventSerializedData(TString buffer, bool extendedFormat)
            : ExtendedFormat(extendedFormat)
        {
            Append(std::move(buffer));
        }

        void SetExtendedFormat() {
            ExtendedFormat = true;
        }

        bool IsExtendedFormat() const {
            return ExtendedFormat;
        }

        TRope::TConstIterator GetBeginIter() const {
            return Rope.Begin();
        }

        size_t GetSize() const {
            return Rope.GetSize();
        }

        TString GetString() const {
            TString result;
            result.reserve(GetSize());
            for (auto it = Rope.Begin(); it.Valid(); it.AdvanceToNextContiguousBlock()) {
                result.append(it.ContiguousData(), it.ContiguousSize());
            }
            return result;
        }

        TRope EraseBack(size_t count) {
            Y_VERIFY(count <= Rope.GetSize());
            TRope::TIterator iter = Rope.End();
            iter -= count;
            return Rope.Extract(iter, Rope.End());
        }

        void Append(TRope&& from) {
            Rope.Insert(Rope.End(), std::move(from));
        } 

        void Append(TString buffer) {
            if (buffer) {
                Rope.Insert(Rope.End(), TRope(std::move(buffer)));
            }
        } 
    }; 
}

class TChainBufWalk : public IWalkInput {
    TIntrusivePtr<NActors::TEventSerializedData> Buffer;
    TRope::TConstIterator Iter;

public:
    TChainBufWalk(TIntrusivePtr<NActors::TEventSerializedData> buffer)
        : Buffer(std::move(buffer))
        , Iter(Buffer->GetBeginIter())
    {}

private:
    size_t DoUnboundedNext(const void **ptr) override {
        const size_t size = Iter.ContiguousSize();
        *ptr = Iter.ContiguousData();
        if (Iter.Valid()) {
            Iter.AdvanceToNextContiguousBlock();
        }
        return size;
    }
};
