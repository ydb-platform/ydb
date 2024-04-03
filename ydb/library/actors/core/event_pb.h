#pragma once

#include "event.h"
#include "event_load.h"

#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/arena.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <util/generic/deque.h>
#include <util/system/context.h>
#include <util/system/filemap.h>
#include <util/string/builder.h>
#include <util/thread/lfstack.h>
#include <array>
#include <span>

// enable only when patch with this macro was successfully deployed
#define USE_EXTENDED_PAYLOAD_FORMAT 0

namespace NActorsProto {
    class TActorId;
} // NActorsProto

namespace NActors {

    class TRopeStream : public NProtoBuf::io::ZeroCopyInputStream {
        TRope::TConstIterator Iter;
        const size_t Size;

    public:
        TRopeStream(TRope::TConstIterator iter, size_t size)
            : Iter(iter)
            , Size(size)
        {}

        bool Next(const void** data, int* size) override;
        void BackUp(int count) override;
        bool Skip(int count) override;
        int64_t ByteCount() const override {
            return TotalByteCount;
        }

    private:
        int64_t TotalByteCount = 0;
    };

    class TChunkSerializer : public NProtoBuf::io::ZeroCopyOutputStream {
    public:
        TChunkSerializer() = default;
        virtual ~TChunkSerializer() = default;

        virtual bool WriteRope(const TRope *rope) = 0;
        virtual bool WriteString(const TString *s) = 0;
    };

    class TAllocChunkSerializer final : public TChunkSerializer {
    public:
        bool Next(void** data, int* size) override;
        void BackUp(int count) override;
        int64_t ByteCount() const override {
            return Buffers->GetSize();
        }
        bool WriteAliasedRaw(const void* data, int size) override;

        // WARNING: these methods require owner to retain ownership and immutability of passed objects
        bool WriteRope(const TRope *rope) override;
        bool WriteString(const TString *s) override;

        inline TIntrusivePtr<TEventSerializedData> Release(TEventSerializationInfo&& serializationInfo) {
            Buffers->SetSerializationInfo(std::move(serializationInfo));
            return std::move(Buffers);
        }

    protected:
        TIntrusivePtr<TEventSerializedData> Buffers = new TEventSerializedData;
        TRope Backup;
    };

    class TCoroutineChunkSerializer final : public TChunkSerializer, protected ITrampoLine {
    public:
        using TChunk = std::pair<const char*, size_t>;

        TCoroutineChunkSerializer();
        ~TCoroutineChunkSerializer();

        void SetSerializingEvent(const IEventBase *event);
        void Abort();
        std::span<TChunk> FeedBuf(void* data, size_t size);
        bool IsComplete() const {
            return !Event;
        }
        bool IsSuccessfull() const {
            return SerializationSuccess;
        }
        const IEventBase *GetCurrentEvent() const {
            return Event;
        }

        bool Next(void** data, int* size) override;
        void BackUp(int count) override;
        int64_t ByteCount() const override {
            return TotalSerializedDataSize;
        }
        bool WriteAliasedRaw(const void* data, int size) override;
        bool AllowsAliasing() const override;

        bool WriteRope(const TRope *rope) override;
        bool WriteString(const TString *s) override;

    protected:
        void DoRun() override;
        void Resume();
        void Produce(const void *data, size_t size);

        i64 TotalSerializedDataSize;
        TMappedAllocation Stack;
        TContClosure SelfClosure;
        TContMachineContext InnerContext;
        TContMachineContext *BufFeedContext = nullptr;
        char *BufferPtr;
        size_t SizeRemain;
        std::vector<TChunk> Chunks;
        const IEventBase *Event = nullptr;
        bool CancelFlag = false;
        bool AbortFlag;
        bool SerializationSuccess;
        bool Finished = false;
    };

    struct TProtoArenaHolder : public TAtomicRefCount<TProtoArenaHolder> {
        google::protobuf::Arena Arena;
        TProtoArenaHolder() = default;

        explicit TProtoArenaHolder(const google::protobuf::ArenaOptions& arenaOptions)
            : Arena(arenaOptions)
        {};

        google::protobuf::Arena* Get() {
            return &Arena;
        }

        template<typename TRecord>
        TRecord* Allocate() {
            return google::protobuf::Arena::CreateMessage<TRecord>(&Arena);
        }
    };

    static const size_t EventMaxByteSize = 140 << 20; // (140MB)

    template <typename TEv, typename TRecord /*protobuf record*/, ui32 TEventType, typename TRecHolder>
    class TEventPBBase: public TEventBase<TEv, TEventType> , public TRecHolder {
        // a vector of data buffers referenced by record; if filled, then extended serialization mechanism applies
        TVector<TRope> Payload;
        size_t TotalPayloadSize = 0;

    public:
        using TRecHolder::Record;

    public:
        using ProtoRecordType = TRecord;

        TEventPBBase() = default;

        explicit TEventPBBase(const TRecord& rec)
            : TRecHolder(rec)
        {}

        explicit TEventPBBase(TRecord&& rec)
            : TRecHolder(rec)
        {}

        explicit TEventPBBase(TIntrusivePtr<TProtoArenaHolder> arena)
            : TRecHolder(std::move(arena))
        {}

        TString ToStringHeader() const override {
            return Record.GetTypeName();
        }

        TString ToString() const override {
            TStringStream ss;
            ss << ToStringHeader() << " " << Record.ShortDebugString();
            return ss.Str();
        }

        bool IsSerializable() const override {
            return true;
        }

        bool SerializeToArcadiaStream(TChunkSerializer* chunker) const override {
            return SerializeToArcadiaStreamImpl(chunker, TString());
        }

        ui32 CalculateSerializedSize() const override {
            ssize_t result = Record.ByteSize();
            if (result >= 0 && Payload) {
                ++result; // marker
                char buf[MaxNumberBytes];
                result += SerializeNumber(Payload.size(), buf);
                for (const TRope& rope : Payload) {
                    result += SerializeNumber(rope.GetSize(), buf);
                }
                result += TotalPayloadSize;
            }
            return result;
        }

        static IEventBase* Load(TEventSerializedData *input) {
            THolder<TEventPBBase> ev(new TEv());
            if (!input->GetSize()) {
                Y_PROTOBUF_SUPPRESS_NODISCARD ev->Record.ParseFromString(TString());
            } else {
                TRope::TConstIterator iter = input->GetBeginIter();
                ui64 size = input->GetSize();

                if (const auto& info = input->GetSerializationInfo(); info.IsExtendedFormat) {
                    // check marker
                    if (!iter.Valid() || (*iter.ContiguousData() != PayloadMarker && *iter.ContiguousData() != ExtendedPayloadMarker)) {
                        Y_ABORT("invalid event");
                    }

                    const bool dataIsSeparate = *iter.ContiguousData() == ExtendedPayloadMarker; // ropes go after sizes

                    auto fetchRope = [&](size_t len) {
                        TRope::TConstIterator begin = iter;
                        iter += len;
                        size -= len;
                        ev->Payload.emplace_back(begin, iter);
                        ev->TotalPayloadSize += len;
                    };

                    // skip marker
                    iter += 1;
                    --size;
                    // parse number of payload ropes
                    size_t numRopes = DeserializeNumber(iter, size);
                    if (numRopes == Max<size_t>()) {
                        Y_ABORT("invalid event");
                    }
                    TStackVec<size_t, 16> ropeLens;
                    if (dataIsSeparate) {
                        ropeLens.reserve(numRopes);
                    }
                    while (numRopes--) {
                        // parse length of the rope
                        const size_t len = DeserializeNumber(iter, size);
                        if (len == Max<size_t>() || size < len) {
                            Y_ABORT("invalid event len# %zu size# %" PRIu64, len, size);
                        }
                        // extract the rope
                        if (dataIsSeparate) {
                            ropeLens.push_back(len);
                        } else {
                            fetchRope(len);
                        }
                    }
                    for (size_t len : ropeLens) {
                        fetchRope(len);
                    }
                }

                // parse the protobuf
                TRopeStream stream(iter, size);
                if (!ev->Record.ParseFromZeroCopyStream(&stream)) {
                    Y_ABORT("Failed to parse protobuf event type %" PRIu32 " class %s", TEventType, TypeName(ev->Record).data());
                }
            }
            ev->CachedByteSize = input->GetSize();
            return ev.Release();
        }

        size_t GetCachedByteSize() const {
            if (CachedByteSize == 0) {
                CachedByteSize = CalculateSerializedSize();
            }
            return CachedByteSize;
        }

        ui32 CalculateSerializedSizeCached() const override {
            return GetCachedByteSize();
        }

        void InvalidateCachedByteSize() {
            CachedByteSize = 0;
        }

        TEventSerializationInfo CreateSerializationInfo() const override {
            return CreateSerializationInfoImpl(0);
        }

        bool AllowExternalDataChannel() const {
            return TotalPayloadSize >= 4096;
        }

    public:
        void ReservePayload(size_t size) {
            Payload.reserve(size);
        }

        ui32 AddPayload(TRope&& rope) {
            const ui32 id = Payload.size();
            TotalPayloadSize += rope.size();
            Payload.push_back(std::move(rope));
            InvalidateCachedByteSize();
            return id;
        }

        const TRope& GetPayload(ui32 id) const {
            Y_ABORT_UNLESS(id < Payload.size());
            return Payload[id];
        }

        ui32 GetPayloadCount() const {
            return Payload.size();
        }

        void StripPayload() {
            Payload.clear();
            TotalPayloadSize = 0;
        }

    protected:
        TEventSerializationInfo CreateSerializationInfoImpl(size_t preserializedSize) const {
            TEventSerializationInfo info;
            info.IsExtendedFormat = static_cast<bool>(Payload);

            if (static_cast<const TEv&>(*this).AllowExternalDataChannel()) {
                if (Payload) {
                    char temp[MaxNumberBytes];
#if USE_EXTENDED_PAYLOAD_FORMAT
                    size_t headerLen = 1 + SerializeNumber(Payload.size(), temp);
                    for (const TRope& rope : Payload) {
                        headerLen += SerializeNumber(rope.size(), temp);
                    }
                    info.Sections.push_back(TEventSectionInfo{0, headerLen, 0, 0, true});
                    for (const TRope& rope : Payload) {
                        info.Sections.push_back(TEventSectionInfo{0, rope.size(), 0, 0, false});
                    }
#else
                    info.Sections.push_back(TEventSectionInfo{0, 1 + SerializeNumber(Payload.size(), temp), 0, 0, true}); // payload marker and rope count
                    for (const TRope& rope : Payload) {
                        const size_t ropeSize = rope.GetSize();
                        info.Sections.back().Size += SerializeNumber(ropeSize, temp);
                        info.Sections.push_back(TEventSectionInfo{0, ropeSize, 0, 0, false}); // data as a separate section
                    }
#endif
                }

                const size_t byteSize = Max<ssize_t>(0, Record.ByteSize()) + preserializedSize;
                info.Sections.push_back(TEventSectionInfo{0, byteSize, 0, 0, true}); // protobuf itself

#ifndef NDEBUG
                size_t total = 0;
                for (const auto& section : info.Sections) {
                    total += section.Size;
                }
                size_t serialized = CalculateSerializedSize();
                Y_ABORT_UNLESS(total == serialized, "total# %zu serialized# %zu byteSize# %zd Payload.size# %zu", total,
                    serialized, byteSize, Payload.size());
#endif
            }

            return info;
        }

        bool SerializeToArcadiaStreamImpl(TChunkSerializer* chunker, const TString& preserialized) const {
            // serialize payload first
            if (Payload) {
                void *data;
                int size = 0;
                auto append = [&](const char *p, size_t len) {
                    while (len) {
                        if (size) {
                            const size_t numBytesToCopy = std::min<size_t>(size, len);
                            memcpy(data, p, numBytesToCopy);
                            data = static_cast<char*>(data) + numBytesToCopy;
                            size -= numBytesToCopy;
                            p += numBytesToCopy;
                            len -= numBytesToCopy;
                        } else if (!chunker->Next(&data, &size)) {
                            return false;
                        }
                    }
                    return true;
                };
                auto appendNumber = [&](size_t number) {
                    char buf[MaxNumberBytes];
                    return append(buf, SerializeNumber(number, buf));
                };

#if USE_EXTENDED_PAYLOAD_FORMAT
                char marker = ExtendedPayloadMarker;
                append(&marker, 1);
                if (!appendNumber(Payload.size())) {
                    return false;
                }
                for (const TRope& rope : Payload) {
                    if (!appendNumber(rope.GetSize())) {
                        return false;
                    }
                }
                if (size) {
                    chunker->BackUp(std::exchange(size, 0));
                }
                for (const TRope& rope : Payload) {
                    if (!chunker->WriteRope(&rope)) {
                        return false;
                    }
                }
#else
                char marker = PayloadMarker;
                append(&marker, 1);
                if (!appendNumber(Payload.size())) {
                    return false;
                }
                for (const TRope& rope : Payload) {
                    if (!appendNumber(rope.GetSize())) {
                        return false;
                    }
                    if (rope) {
                        if (size) {
                            chunker->BackUp(std::exchange(size, 0));
                        }
                        if (!chunker->WriteRope(&rope)) {
                            return false;
                        }
                    }
                }
                if (size) {
                    chunker->BackUp(size);
                }
#endif
            }

            if (preserialized && !chunker->WriteString(&preserialized)) {
                return false;
            }

            return Record.SerializeToZeroCopyStream(chunker);
        }

    protected:
        mutable size_t CachedByteSize = 0;

        static constexpr char ExtendedPayloadMarker = 0x06;
        static constexpr char PayloadMarker = 0x07;
        static constexpr size_t MaxNumberBytes = (sizeof(size_t) * CHAR_BIT + 6) / 7;

        static size_t SerializeNumber(size_t num, char *buffer) {
            char *begin = buffer;
            do {
                *buffer++ = (num & 0x7F) | (num >= 128 ? 0x80 : 0x00);
                num >>= 7;
            } while (num);
            return buffer - begin;
        }

        static size_t DeserializeNumber(const char **ptr, const char *end) {
            const char *p = *ptr;
            size_t res = 0;
            size_t offset = 0;
            for (;;) {
                if (p == end) {
                    return Max<size_t>();
                }
                const char byte = *p++;
                res |= (static_cast<size_t>(byte) & 0x7F) << offset;
                offset += 7;
                if (!(byte & 0x80)) {
                    break;
                }
            }
            *ptr = p;
            return res;
        }

        static size_t DeserializeNumber(TRope::TConstIterator& iter, ui64& size) {
            size_t res = 0;
            size_t offset = 0;
            for (;;) {
                if (!iter.Valid()) {
                    return Max<size_t>();
                }
                const char byte = *iter.ContiguousData();
                iter += 1;
                --size;
                res |= (static_cast<size_t>(byte) & 0x7F) << offset;
                offset += 7;
                if (!(byte & 0x80)) {
                    break;
                }
            }
            return res;
        }
    };

    // Protobuf record not using arena
    template <typename TRecord>
    struct TRecordHolder {
        TRecord Record;

        TRecordHolder() = default;
        TRecordHolder(const TRecord& rec)
            : Record(rec)
        {}

        TRecordHolder(TRecord&& rec)
            : Record(std::move(rec))
        {}
    };

    // Protobuf arena and a record allocated on it
    template <typename TRecord, size_t InitialBlockSize, size_t MaxBlockSize>
    struct TArenaRecordHolder {
        TIntrusivePtr<TProtoArenaHolder> Arena;
        TRecord& Record;

        // Arena depends on block size to be a multiple of 8 for correctness
        // FIXME: uncomment these asserts when code is synchronized between repositories
        // static_assert((InitialBlockSize & 7) == 0, "Misaligned InitialBlockSize");
        // static_assert((MaxBlockSize & 7) == 0, "Misaligned MaxBlockSize");

        static const google::protobuf::ArenaOptions GetArenaOptions() {
            google::protobuf::ArenaOptions opts;
            opts.initial_block_size = InitialBlockSize;
            opts.max_block_size = MaxBlockSize;
            return opts;
        }

        TArenaRecordHolder()
            : Arena(MakeIntrusive<TProtoArenaHolder>(GetArenaOptions()))
            , Record(*Arena->Allocate<TRecord>())
        {};

        TArenaRecordHolder(const TRecord& rec)
            : TArenaRecordHolder()
        {
            Record.CopyFrom(rec);
        }

        // not allowed to move from another protobuf, it's a potenial copying
        TArenaRecordHolder(TRecord&& rec) = delete;

        TArenaRecordHolder(TIntrusivePtr<TProtoArenaHolder> arena)
            : Arena(std::move(arena))
            , Record(*Arena->Allocate<TRecord>())
        {};
    };

    template <typename TEv, typename TRecord, ui32 TEventType>
    class TEventPB : public TEventPBBase<TEv, TRecord, TEventType, TRecordHolder<TRecord> > {
        typedef TEventPBBase<TEv, TRecord, TEventType, TRecordHolder<TRecord> > TPbBase;
        // NOTE: No extra fields allowed: TEventPB must be a "template typedef"
    public:
        using TPbBase::TPbBase;
    };

    template <typename TEv, typename TRecord, ui32 TEventType, size_t InitialBlockSize = 512, size_t MaxBlockSize = 16*1024>
    using TEventPBWithArena = TEventPBBase<TEv, TRecord, TEventType, TArenaRecordHolder<TRecord, InitialBlockSize, MaxBlockSize> >;

    template <typename TEv, typename TRecord, ui32 TEventType>
    class TEventShortDebugPB: public TEventPB<TEv, TRecord, TEventType> {
    public:
        using TBase = TEventPB<TEv, TRecord, TEventType>;
        TEventShortDebugPB() = default;
        explicit TEventShortDebugPB(const TRecord& rec)
            : TBase(rec)
        {
        }
        explicit TEventShortDebugPB(TRecord&& rec)
            : TBase(std::move(rec))
        {
        }
        TString ToString() const override {
            return TypeName<TEv>() + " { " + TBase::Record.ShortDebugString() + " }";
        }
    };

    template <typename TEv, typename TRecord, ui32 TEventType>
    class TEventPreSerializedPB: public TEventPB<TEv, TRecord, TEventType> {
    protected:
        using TBase = TEventPB<TEv, TRecord, TEventType>;
        using TSelf = TEventPreSerializedPB<TEv, TRecord, TEventType>;
        using TBase::Record;

    public:
        TString PreSerializedData; // already serialized PB data (using message::SerializeToString)

        TEventPreSerializedPB() = default;

        explicit TEventPreSerializedPB(const TRecord& rec)
            : TBase(rec)
        {
        }

        explicit TEventPreSerializedPB(TRecord&& rec)
            : TBase(std::move(rec))
        {
        }

        // when remote event received locally this method will merge preserialized data
        const TRecord& GetRecord() {
            TRecord& base(TBase::Record);
            if (!PreSerializedData.empty()) {
                TRecord copy;
                Y_PROTOBUF_SUPPRESS_NODISCARD copy.ParseFromString(PreSerializedData);
                copy.MergeFrom(base);
                base.Swap(&copy);
                PreSerializedData.clear();
            }
            return TBase::Record;
        }

        const TRecord& GetRecord() const {
            return const_cast<TSelf*>(this)->GetRecord();
        }

        TRecord* MutableRecord() {
            GetRecord(); // Make sure PreSerializedData is parsed
            return &(TBase::Record);
        }

        TString ToString() const override {
            return GetRecord().ShortDebugString();
        }

        bool SerializeToArcadiaStream(TChunkSerializer* chunker) const override {
            return TBase::SerializeToArcadiaStreamImpl(chunker, PreSerializedData);
        }

        ui32 CalculateSerializedSize() const override {
            return PreSerializedData.size() + TBase::CalculateSerializedSize();
        }

        size_t GetCachedByteSize() const {
            return PreSerializedData.size() + TBase::GetCachedByteSize();
        }

        ui32 CalculateSerializedSizeCached() const override {
            return GetCachedByteSize();
        }

        TEventSerializationInfo CreateSerializationInfo() const override {
            return TBase::CreateSerializationInfoImpl(PreSerializedData.size());
        }
    };

    TActorId ActorIdFromProto(const NActorsProto::TActorId& actorId);
    void ActorIdToProto(const TActorId& src, NActorsProto::TActorId* dest);

}
