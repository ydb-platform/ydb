#pragma once

#include "event.h"
#include "event_load.h"

#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/arena.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <util/generic/bitops.h>
#include <util/generic/deque.h>
#include <util/system/context.h>
#include <util/system/filemap.h>
#include <util/string/builder.h>
#include <util/string/hex.h>
#include <util/thread/lfstack.h>
#include <array>
#include <span>

namespace NActorsProto {
    class TActorId;
} // NActorsProto

namespace NInterconnect::NRdma {
    class TMemRegion;
}

namespace google::protobuf {
    class Message;
    class MessageLite;
}

namespace NActors {
    TString EventPBBaseToString(const TString& header, const TString& dbgStr);

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
        virtual NProtoBuf::io::CodedOutputStream *GetCodedOutputStream() = 0;
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

        NProtoBuf::io::CodedOutputStream *GetCodedOutputStream() override {
            return nullptr;
        }

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
        struct TChunk {
            const char* Buf;
            size_t Size;
            const NInterconnect::NRdma::TMemRegion* MemRegion;
        };

        enum class EAliasedMode {
            PassThrough,
            CopyToBuffer,
        };

        TCoroutineChunkSerializer();
        ~TCoroutineChunkSerializer();

        void SetSerializingEvent(const IEventBase *event, bool withCachedSizes, bool withCord);
        void DiscardEvent() { Event = nullptr; };
        void Abort();
        std::span<TChunk> FeedBuf(void* data, size_t size,
            EAliasedMode aliasedMode = EAliasedMode::PassThrough);
        std::span<TChunk> FeedBuf(TMutableContiguousSpan *buffer, size_t totalSize,
            EAliasedMode aliasedMode = EAliasedMode::PassThrough);
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
        bool WriteCord(const y_absl::Cord& cord) override;

        NProtoBuf::io::CodedOutputStream *GetCodedOutputStream() override {
            if (!WithCachedSizes) {
                return nullptr;
            }
            if (!CodedOutputStream) {
                CodedOutputStream.reset(new NProtoBuf::io::CodedOutputStream(this, false));
            }
            return CodedOutputStream.get();
        }

        std::vector<y_absl::Cord>& GetCords() {
            return Cords;
        }

    protected:
        void DoRun() override;
        void Resume();
        void Produce(const void* data, ssize_t size,
            const NInterconnect::NRdma::TMemRegion* memRegion);
        bool WriteAliasedRawImpl(const void* data, int size,
            const NInterconnect::NRdma::TMemRegion* memRegion);

        i64 TotalSerializedDataSize;
        TMappedAllocation Stack;
        TContClosure SelfClosure;
        TContMachineContext InnerContext;
        TContMachineContext *BufFeedContext = nullptr;
        TMutableContiguousSpan Buffer;
        size_t TotalSizeRemain;
        std::vector<TChunk> Chunks;
        TChunk LastChunk{nullptr, 0, nullptr};
        EAliasedMode AliasedMode = EAliasedMode::PassThrough;
        const IEventBase *Event = nullptr;
        bool CancelFlag = false;
        bool AbortFlag;
        bool SerializationSuccess;
        bool Finished = false;
        bool WithCachedSizes = false;
        bool WithCord = false;
        std::unique_ptr<NProtoBuf::io::CodedOutputStream> CodedOutputStream;
        std::vector<y_absl::Cord> Cords;
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
    static constexpr char ExtendedPayloadMarker = 0x06;
    static constexpr char PayloadMarker = 0x07;
    static constexpr size_t MaxNumberBytes = (sizeof(size_t) * CHAR_BIT + 6) / 7;

    void ParseExtendedFormatPayload(TRope::TConstIterator &iter, size_t &size, TVector<TRope> &payload, size_t &totalPayloadSize);
    bool SerializeToArcadiaStreamImpl(TChunkSerializer* chunker, const TVector<TRope> &payload);
    ui32 CalculateSerializedHeaderSizeImpl(const TVector<TRope> &payload);
    std::optional<TRope> SerializeToRopeImpl(const google::protobuf::MessageLite& msg, const TVector<TRope>& payload, IRcBufAllocator* allocator);
    ui32 CalculateSerializedSizeImpl(const TVector<TRope> &payload, ssize_t recordSize);
    TEventSerializationInfo CreateSerializationInfoImpl(size_t preserializedSize, bool allowExternalDataChannel, const TVector<TRope> &payload, ssize_t recordSize, size_t payloadAlignment = 0, size_t payloadHeaderSize = 0);

    // Type-independent state of TEventPBBase: payload ropes attached to the record and the cached serialized size.
    class TEventPBPayloadBase {
    public:
        TEventPBPayloadBase() = default;
        TEventPBPayloadBase(const TEventPBPayloadBase&) = default;
        TEventPBPayloadBase(TEventPBPayloadBase&&) = default;
        TEventPBPayloadBase& operator=(const TEventPBPayloadBase&) = default;
        TEventPBPayloadBase& operator=(TEventPBPayloadBase&&) = default;
        ~TEventPBPayloadBase();

        ui32 AddPayload(TRope&& rope) {
            const ui32 id = Payload.size();
            TotalPayloadSize += rope.size();
            Payload.push_back(std::move(rope));
            InvalidateCachedByteSize();
            return id;
        }

        const TRope& GetPayload(ui32 id) const {
            Y_ENSURE(id < Payload.size());
            return Payload[id];
        }

        const TVector<TRope>& GetPayload() const {
            return Payload;
        }

        ui32 GetPayloadCount() const {
            return Payload.size();
        }

        void StripPayload() {
            Payload.clear();
            TotalPayloadSize = 0;
            InvalidateCachedByteSize();
        }

        void InvalidateCachedByteSize() {
            CachedByteSize = 0;
        }

        bool AllowExternalDataChannel() const {
            return TotalPayloadSize >= 4096;
        }

    protected:
        TRope& MutablePayload(ui32 id) {
            Y_ENSURE(id < Payload.size());
            return Payload[id];
        }

    private:
        friend void LoadEventPB(const TEventSerializedData* input, google::protobuf::MessageLite& record,
            TEventPBPayloadBase& payload, ui32 eventType);

        // a vector of data buffers referenced by record; if filled, then extended serialization mechanism applies
        TVector<TRope> Payload;
        size_t TotalPayloadSize = 0;

    protected:
        mutable size_t CachedByteSize = 0;
    };

    // Non-template implementations of the TEventPBBase / TEventPreSerializedPB methods.
    void LoadEventPB(const TEventSerializedData* input, google::protobuf::MessageLite& record,
        TEventPBPayloadBase& payload, ui32 eventType);
    bool SerializeEventPB(TChunkSerializer* chunker, const google::protobuf::MessageLite& record,
        const TVector<TRope>& payload, const TString* preSerializedData = nullptr);
    ui32 CalculateEventPBSize(const google::protobuf::MessageLite& record, const TVector<TRope>& payload);
    TEventSerializationInfo CreateEventPBSerializationInfo(size_t preserializedSize, bool allowExternalDataChannel,
        const google::protobuf::MessageLite& record, const TVector<TRope>& payload,
        size_t payloadAlignment, size_t payloadHeaderSize);
    // header is event.ToStringHeader()
    TString EventPBToString(const IEventBase& event, const google::protobuf::Message& record);
    // header is record.GetTypeName()
    TString EventPBToString(const google::protobuf::Message& record);

    template <typename TEv, typename TRecord /*protobuf record*/, ui32 TEventType, typename TRecHolder>
    class TEventPBBase: public TEventBase<TEv, TEventType> , public TRecHolder, public TEventPBPayloadBase {
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
            return EventPBToString(*this, Record);
        }

        bool IsSerializable() const override {
            return true;
        }

        bool SerializeToArcadiaStream(TChunkSerializer* chunker) const override {
            return SerializeEventPB(chunker, Record, GetPayload());
        }

        ui32 CalculateSerializedSize() const override {
            return CalculateEventPBSize(Record, GetPayload());
        }

        std::optional<TRope> SerializeToRope(IRcBufAllocator* allocator) const override {
            return NActors::SerializeToRopeImpl(Record, GetPayload(), allocator);
        }

        static TEv* Load(const TEventSerializedData *input) {
            THolder<TEv> holder(new TEv());
            TEventPBBase* ev = holder.Get();
            LoadEventPB(input, ev->Record, *ev, TEventType);
            return holder.Release();
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

        TEventSerializationInfo CreateSerializationInfo(bool allowExternalDataChannel) const override {
            constexpr size_t payloadAlignment = TEv::GetPayloadAlignment();
            constexpr size_t payloadHeaderSize = TEv::GetPayloadHeaderSize();
            static_assert(payloadAlignment == 0 || IsPowerOf2(payloadAlignment),
                "GetPayloadAlignment() must be zero or a power of two");
            static_assert(payloadAlignment == 0 || payloadHeaderSize % payloadAlignment == 0,
                "GetPayloadHeaderSize() must be a multiple of GetPayloadAlignment()");
            allowExternalDataChannel = allowExternalDataChannel && static_cast<const TEv&>(*this).AllowExternalDataChannel();
            return CreateEventPBSerializationInfo(0, allowExternalDataChannel, Record, GetPayload(),
                payloadAlignment, payloadHeaderSize);
        }

        static constexpr size_t GetPayloadAlignment() {
            return 0;
        }

        // Size of an optional header reserved in the payload buffer right before each payload (see GetPayloadWithHeader).
        // Must be a multiple of GetPayloadAlignment() when alignment is requested.
        static constexpr size_t GetPayloadHeaderSize() {
            return 0;
        }

    public:
        // Returns an owning TRcBuf covering [header | payload] in contiguous memory, where the leading
        // GetPayloadHeaderSize() bytes are reserved (uninitialized) header space, immediately followed by the
        // payload bytes. When alignment is requested, the header start is aligned to GetPayloadAlignment().
        // GetPayload(id) keeps returning the payload-only rope.
        //
        // Fast (zero-copy) path: taken when the payload is a single contiguous chunk that still has enough safe
        // headroom in front (>= GetPayloadHeaderSize()) at the required alignment - the case for an
        // interconnect-received payload (see CreateSerializationInfo). Safe headroom means the chunk either
        // privately owns the space or, for cookie-bearing backends, currently owns its front edge, so a
        // shared-but-front buffer can also take this path. Otherwise an aligned buffer is allocated and the
        // payload is copied after the header. Intended to be called at most once per payload id: the fast path
        // consumes the reserved headroom (via cookies); a second call on the same id takes the fallback path.
        // Write the header through the returned buffer's
        // UnsafeGetContiguousSpanMut()/UnsafeGetDataMut() (these bypass copy-on-write; the header region lies
        // outside every payload's bytes, so this is safe even when the backend is shared).
        TRcBuf GetPayloadWithHeader(ui32 id) {
            TRope& rope = MutablePayload(id);
            constexpr size_t headerSize = TEv::GetPayloadHeaderSize();
            constexpr size_t alignment = TEv::GetPayloadAlignment();
            const size_t payloadSize = rope.GetSize();

            if (payloadSize && rope.IsContiguous()) {
                const TRcBuf& chunk = rope.Begin().GetChunk();
                if (chunk.Headroom() >= headerSize) {
                    const char* headerStart = chunk.GetData() - headerSize;
                    const bool aligned = alignment == 0 ||
                        (reinterpret_cast<uintptr_t>(headerStart) & (alignment - 1)) == 0;
                    if (aligned) {
                        return chunk.ExpandFront(headerSize);
                    }
                }
            }

            TRcBuf buffer = TRcBuf::UninitializedAligned(headerSize + payloadSize, alignment);
            if (payloadSize) {
                rope.Begin().ExtractPlainDataAndAdvance(buffer.UnsafeGetDataMut() + headerSize, payloadSize);
            }
            return buffer;
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
    using TEventPB = TEventPBBase<TEv, TRecord, TEventType, TRecordHolder<TRecord> >;

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
                TBase::InvalidateCachedByteSize(); // cached size may be incorrect now
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
            return EventPBToString(GetRecord());
        }

        bool SerializeToArcadiaStream(TChunkSerializer* chunker) const override {
            return SerializeEventPB(chunker, Record, TBase::GetPayload(), &PreSerializedData);
        }

        ui32 CalculateSerializedSize() const override {
            return PreSerializedData.size() + TBase::CalculateSerializedSize();
        }

        TEventSerializationInfo CreateSerializationInfo(bool allowExternalDataChannel) const override {
            constexpr size_t payloadAlignment = TEv::GetPayloadAlignment();
            constexpr size_t payloadHeaderSize = TEv::GetPayloadHeaderSize();
            static_assert(payloadAlignment == 0 || IsPowerOf2(payloadAlignment),
                "GetPayloadAlignment() must be zero or a power of two");
            static_assert(payloadAlignment == 0 || payloadHeaderSize % payloadAlignment == 0,
                "GetPayloadHeaderSize() must be a multiple of GetPayloadAlignment()");
            allowExternalDataChannel = allowExternalDataChannel && static_cast<const TEv&>(*this).AllowExternalDataChannel();
            return CreateEventPBSerializationInfo(PreSerializedData.size(), allowExternalDataChannel, Record, TBase::GetPayload(),
                payloadAlignment, payloadHeaderSize);
        }
    };

    TActorId ActorIdFromProto(const NActorsProto::TActorId& actorId);
    void ActorIdToProto(const TActorId& src, NActorsProto::TActorId* dest);

}
