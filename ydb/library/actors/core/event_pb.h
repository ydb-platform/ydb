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
    static constexpr char ExtendedPayloadMarker = 0x06;
    static constexpr char PayloadMarker = 0x07;
    static constexpr size_t MaxNumberBytes = (sizeof(size_t) * CHAR_BIT + 6) / 7;

    void ParseExtendedFormatPayload(TRope::TConstIterator &iter, size_t &size, TVector<TRope> &payload, size_t &totalPayloadSize);
    bool SerializeToArcadiaStreamImpl(TChunkSerializer* chunker, const TVector<TRope> &payload);
    ui32 CalculateSerializedSizeImpl(const TVector<TRope> &payload, ssize_t recordSize);
    TEventSerializationInfo CreateSerializationInfoImpl(size_t preserializedSize, bool allowExternalDataChannel, const TVector<TRope> &payload, ssize_t recordSize);

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
            if (!SerializeToArcadiaStreamImpl(chunker, Payload)) {
                return false;
            }
            return Record.SerializeToZeroCopyStream(chunker);
        }

        ui32 CalculateSerializedSize() const override {
            return CalculateSerializedSizeImpl(Payload, Record.ByteSize());
        }

        static IEventBase* Load(TEventSerializedData *input) {
            THolder<TEventPBBase> ev(new TEv());
            if (!input->GetSize()) {
                Y_PROTOBUF_SUPPRESS_NODISCARD ev->Record.ParseFromString(TString());
            } else {
                TRope::TConstIterator iter = input->GetBeginIter();
                ui64 size = input->GetSize();

                if (const auto& info = input->GetSerializationInfo(); info.IsExtendedFormat) {
                    ParseExtendedFormatPayload(iter, size, ev->Payload, ev->TotalPayloadSize);
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
            return CreateSerializationInfoImpl(0, static_cast<const TEv&>(*this).AllowExternalDataChannel(), GetPayload(), Record.ByteSize());
        }

        bool AllowExternalDataChannel() const {
            return TotalPayloadSize >= 4096;
        }

    public:
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

        const TVector<TRope>& GetPayload() const {
            return Payload;
        }

        ui32 GetPayloadCount() const {
            return Payload.size();
        }

        void StripPayload() {
            Payload.clear();
            TotalPayloadSize = 0;
        }

    protected:
        mutable size_t CachedByteSize = 0;
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
            if (!SerializeToArcadiaStreamImpl(chunker, TBase::GetPayload())) {
                return false;
            }
            
            if (PreSerializedData && !chunker->WriteString(&PreSerializedData)) {
                return false;
            }

            return Record.SerializeToZeroCopyStream(chunker);
            
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
            return CreateSerializationInfoImpl(PreSerializedData.size(), static_cast<const TEv&>(*this).AllowExternalDataChannel(), TBase::GetPayload(), Record.ByteSize());
        }
    };

    TActorId ActorIdFromProto(const NActorsProto::TActorId& actorId);
    void ActorIdToProto(const TActorId& src, NActorsProto::TActorId* dest);

}
