#include "mlp_storage.h"

#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/library/actors/core/log.h>

#include <library/cpp/packedtypes/longs.h>

namespace NKikimr::NPQ::NMLP {

namespace {

struct TAddedMessage {
    union {
        struct {
            ui32 HasMessageGroupId: 1;
            ui32 MessageGroupIdHash: 31;
        } Fields;
        ui32 Value;

        static_assert(sizeof(Value) == sizeof(Fields));
    };
    ui32 WriteTimestampDelta;
};


struct TMessageChange {
    ui32 Status: 3;
    ui32 Reserve: 3; 
    ui32 ReceiveCount: 10;
    ui32 DeadlineDelta: 16;
};

static_assert(sizeof(TMessageChange) == sizeof(ui32));

void VarintSerialize(TString& buffer, ui64 value) {
    const auto outValue = static_cast<i64>(value);
    char varIntOut[sizeof(outValue) + 1];
    auto bytes = out_long(outValue, varIntOut);

    buffer.append(varIntOut, bytes);
}

void VarintDeserialize(const char*& data, ui64& value) {
    i64 tmp;
    data += in_long(tmp, data);
    value = tmp;
}

template<typename TMsg>
struct TItemSerializer {
    void Serialize(TString buffer, const TMsg& msg) {
        buffer.append(reinterpret_cast<const char*>(&msg), sizeof(TMsg));
    }
};

template<typename TMsg>
struct TItemDeserializer {
    bool Deserilize(const char*& data, const char* end, TMsg& msg) {
        if (data + sizeof(TMsg) > end) {
            return false;
        }

        memcpy(&msg, data, sizeof(TMsg));  // TODO BIGENDIAN/LOWENDIAN
        data += sizeof(TStorage::TMessage);

        return true;
    }
};

template<>
struct TItemSerializer<TAddedMessage> {
    ui64 LastWriteTimestampDelta;

    void Serialize(TString buffer, const TAddedMessage& msg) {
        buffer.append(reinterpret_cast<const char*>(&msg.Value), sizeof(msg.Value));
        VarintSerialize(buffer, static_cast<ui64>(msg.WriteTimestampDelta - LastWriteTimestampDelta));
        LastWriteTimestampDelta = msg.WriteTimestampDelta;
    }
};


template<typename TMsg>
struct TSerializer {
    TString Buffer;
    TItemSerializer<TMsg> ItemSerializer;

    void Reserve(size_t size) {
        Buffer.reserve(size * sizeof(TStorage::TMessage));
    }

    void Add(const TStorage::TMessage& message) {
        ItemSerializer.Serialize(Buffer, message);
    }
};

template<typename TMsg>
struct TDeserializer {
    const char* Data;
    const char* End;
    TItemDeserializer<TMsg> ItemDeserializer;

    TDeserializer(const TString& data)
        : Data(data.data())
        , End(data.data() + data.size())
    {
    }

    bool Next(TStorage::TMessage& message) {
        return ItemDeserializer.Deserilize(Data, End, message);
    }
};

template<typename TMsg>
struct TSerializerWithOffset {
    TString Buffer;
    ui64 LastOffset = 0;
    TItemSerializer<TMsg> ItemSerializer;

    void Reserve(size_t size) {
        Buffer.reserve(size * (sizeof(TMsg) + 1 /* offset delta */) + sizeof(ui64) /* first offset*/);
    }

    void Add(ui64 offset, const TMsg& message) {
        VarintSerialize(Buffer, offset - LastOffset);
        ItemSerializer.Serialize(Buffer, message);
        LastOffset = offset;
    }
};

template<typename TMsg>
struct TDeserializerWithOffset {
    const char* Data;
    const char* End;
    TItemDeserializer<TMsg> ItemDeserializer;
    ui64 LastOffset;

    TDeserializerWithOffset(const TString& data)
        : Data(data.data())
        , End(data.data() + data.size())
    {
    }

    bool Next(ui64& offset, TMsg& message) {
        ui64 delta;
        VarintDeserialize(Data, delta);
        LastOffset += delta;
        offset = LastOffset;

        return ItemDeserializer.Deserilize(Data, End, message);
    }
};

}

bool TStorage::Initialize(const NKikimrPQ::TMLPStorageSnapshot& snapshot) {
    AFL_ENSURE(snapshot.GetFormatVersion() == 1)("v", snapshot.GetFormatVersion());

    Messages.resize(snapshot.GetMessages().length() / sizeof(TMessage));

    auto& meta = snapshot.GetMeta();
    FirstOffset = meta.GetFirstOffset();
    FirstUncommittedOffset = FirstOffset;
    FirstUnlockedOffset = FirstOffset;
    BaseDeadline = TInstant::MilliSeconds(meta.GetBaseDeadlineMilliseconds());

    bool moveUnlockedOffset = true;
    bool moveUncommittedOffset = true;

    TDeserializer<TMessage> deserializer(snapshot.GetMessages());
    TMessage msg;
    size_t i = 0;
    while (deserializer.Next(msg)) {
        auto& message = Messages[i++] = msg;

        switch(message.Status) {
            case EMessageStatus::Locked:
                ++Metrics.LockedMessageCount;
                if (KeepMessageOrder && message.HasMessageGroupId) {
                    LockedMessageGroupsId.insert(message.MessageGroupIdHash);
                    ++Metrics.LockedMessageGroupCount;
                }
                moveUncommittedOffset = false;
                break;
            case EMessageStatus::Committed:
                ++Metrics.CommittedMessageCount;
                break;
            case EMessageStatus::Unprocessed:
                ++Metrics.UnprocessedMessageCount;
                moveUnlockedOffset = false;
                moveUncommittedOffset = false;
                break;
            case EMessageStatus::DLQ:
                moveUncommittedOffset = false;
                break;
        }
    
        if (moveUnlockedOffset) {
            ++FirstUnlockedOffset;
        }
        if (moveUncommittedOffset) {
            ++FirstUncommittedOffset;
        }
    }

    Metrics.InflyMessageCount = Messages.size();

    for (auto offset : snapshot.GetDLQ()) {
        DLQQueue.push_back(offset);
    }

    return true;
}

bool TStorage::ApplyWAL(NKikimrPQ::TMLPStorageWAL& wal) {
    AFL_ENSURE(wal.GetFormatVersion() == 1)("v", wal.GetFormatVersion());

    if (wal.HasAddedMessages()) {
        TDeserializerWithOffset<TAddedMessage> deserializer(wal.GetAddedMessages());

        ui64 offset;
        TAddedMessage msg;
        while(deserializer.Next(offset, msg)) {
            auto* message = GetMessageInt(offset);
            if (!message) {
                continue;
            }

            AddMessage(offset, msg.Fields.HasMessageGroupId, msg.Fields.MessageGroupIdHash);
        }
    }

    if (wal.HasChangedMessages()) {
        TDeserializerWithOffset<TMessageChange> deserializer(wal.GetChangedMessages());

        ui64 offset;
        TMessageChange msg;
        while(deserializer.Next(offset, msg)) {
            auto* message = GetMessageInt(offset);
            if (!message) {
                continue;
            }

            message->Status = msg.Status;
            message->DeadlineDelta = msg.DeadlineDelta;
            message->ReceiveCount = msg.ReceiveCount;

            // TODO metrics
        }
    }

    for (auto offset : wal.GetDLQ()) {
        DLQQueue.push_back(offset);
    }

    return wal.HasAddedMessages() || wal.HasChangedMessages();
}

bool TStorage::SerializeTo(NKikimrPQ::TMLPStorageSnapshot& snapshot) {
    auto* meta = snapshot.MutableMeta();
    meta->SetFirstOffset(FirstOffset);
    meta->SetFirstUncommittedOffset(FirstUncommittedOffset);
    meta->SetBaseDeadlineMilliseconds(BaseDeadline.MilliSeconds());

    snapshot.SetFormatVersion(1);

    TSerializer<TMessage> serializer;
    serializer.Reserve(Messages.size());
    for (auto& message : Messages) {
        serializer.Add(message);
    }
    snapshot.SetMessages(std::move(serializer.Buffer));

    for (auto offset : DLQQueue) {
        snapshot.AddDLQ(offset);
    }

    return true;
}

bool TStorage::TBatch::SerializeTo(NKikimrPQ::TMLPStorageWAL& wal) {
    wal.SetFormatVersion(1);
    wal.SetFirstOffset(Storage->FirstOffset);

    if (FirstNewMessage) {
        wal.SetFirstAddedOffset(FirstNewMessage.value());

        auto lastOffset = Storage->GetLastOffset();

        TSerializerWithOffset<TAddedMessage> serializer;
        serializer.Reserve(NewMessageCount);

        for (size_t offset = FirstNewMessage.value(); offset < lastOffset; ++offset) {
            auto* message = Storage->GetMessage(offset);
            if (message) {
                TAddedMessage msg;
                msg.Fields.HasMessageGroupId = message->HasMessageGroupId;
                msg.Fields.MessageGroupIdHash = message->MessageGroupIdHash;
                serializer.Add(offset, msg);
            }
        }

        wal.SetAddedMessages(std::move(serializer.Buffer));
    }

    if (!ChangedMessages.empty()) {
        TSerializerWithOffset<TMessageChange> serializer;
        serializer.Reserve(ChangedMessages.size());
        for (auto offset : ChangedMessages) {
            auto* message = Storage->GetMessage(offset);
            if (message) {
                TMessageChange msg;
                msg.Status = message->Status;
                msg.ReceiveCount = message->ReceiveCount;
                msg.DeadlineDelta = message->DeadlineDelta;
                serializer.Add(offset, msg);
            }
        }

        wal.SetChangedMessages(std::move(serializer.Buffer));
    }

    for (auto offset : DLQ) {
        wal.AddDLQ(offset);
    }

    return true;
}

} // namespace NKikimr::NPQ::NMLP
