#pragma once

#include <util/stream/walk.h>
#include <util/system/types.h>
#include <util/generic/string.h>
#include <ydb/library/actors/util/rope.h>
#include <ydb/library/actors/wilson/wilson_trace.h>

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

    struct TEventSectionInfo {
        size_t Headroom = 0; // headroom to be created on the receiving side
        size_t Size = 0; // full size of serialized event section (a chunk in rope)
        size_t Tailroom = 0; // tailroom for the chunk
        size_t Alignment = 0; // required alignment
        bool IsInline = false; // if true, goes through ordinary channel
    };

    struct TEventSerializationInfo {
        bool IsExtendedFormat = {};
        std::vector<TEventSectionInfo> Sections;
        // total sum of Size for every section must match actual serialized size of the event
    };

    class TEventSerializedData
       : public TThrRefBase
    {
        TRope Rope;
        TEventSerializationInfo SerializationInfo;

    public:
        TEventSerializedData() = default;

        TEventSerializedData(TRope&& rope, TEventSerializationInfo&& serializationInfo)
            : Rope(std::move(rope))
            , SerializationInfo(std::move(serializationInfo))
        {}

        TEventSerializedData(const TEventSerializedData& original, TString extraBuffer)
            : Rope(original.Rope)
            , SerializationInfo(original.SerializationInfo)
        {
            if (!SerializationInfo.Sections.empty()) {
                SerializationInfo.Sections.push_back(TEventSectionInfo{0, extraBuffer.size(), 0, 0, true});
            }
            Append(std::move(extraBuffer));
        }

        TEventSerializedData(TString buffer, TEventSerializationInfo&& serializationInfo)
            : SerializationInfo(std::move(serializationInfo))
        {
            Append(std::move(buffer));
        }

        void SetSerializationInfo(TEventSerializationInfo&& serializationInfo) {
            SerializationInfo = std::move(serializationInfo);
        }

        const TEventSerializationInfo& GetSerializationInfo() const {
            return SerializationInfo;
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

        TRope GetRope() const {
            return TRope(Rope);
        }

        TRope EraseBack(size_t count) {
            Y_ABORT_UNLESS(count <= Rope.GetSize());
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
