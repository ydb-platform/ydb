#include "mlp_storage.h"

#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/library/actors/core/log.h>

#include <library/cpp/packedtypes/longs.h>

namespace NKikimr::NPQ::NMLP {

namespace {

struct TSnapshotMessage {
        union {
            struct {
                ui64 Status: 3;
                ui64 Reserve: 3;
                ui64 ReceiveCount: 10;
                ui64 DeadlineDelta: 16;
                ui64 HasMessageGroupId: 1;
                ui64 MessageGroupIdHash: 31;
            } Fields;
            ui64 Value;

            static_assert(sizeof(Value) == sizeof(Fields));
        } Common;
        ui32 WriteTimestampDelta;

};

struct TAddedMessage {
    union {
        struct {
            ui32 HasMessageGroupId: 1;
            ui32 MessageGroupIdHash: 31;
        } Fields;
        ui32 Value;

        static_assert(sizeof(Value) == sizeof(Fields));
    } MessageGroup;
    ui32 WriteTimestampDelta;
};


struct TMessageChange {
    union {
        struct {
            ui32 Status: 3;
            ui32 Reserve: 3;
            ui32 ReceiveCount: 10;
            ui32 DeadlineDelta: 16;
        } Fields;
        ui32 Value;

        static_assert(sizeof(Value) == sizeof(Fields));
    } Common;
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
    void Serialize(TString& buffer, const TMsg& msg) {
        buffer.append(reinterpret_cast<const char*>(&msg), sizeof(TMsg));
    }
};

template<typename TMsg>
struct TItemDeserializer {
    bool Deserialize(const char*& data, const char* end, TMsg& msg) {
        if (data + sizeof(TMsg) > end) {
            return false;
        }

        memcpy(&msg, data, sizeof(TMsg));  // TODO BIGENDIAN/LOWENDIAN
        data += sizeof(TMsg);

        return true;
    }
};

template<>
struct TItemSerializer<TSnapshotMessage> {
    ui64 LastWriteTimestampDelta = 0;

    void Serialize(TString& buffer, const TSnapshotMessage& msg) {
        buffer.append(reinterpret_cast<const char*>(&msg.Common.Value), sizeof(msg.Common.Value));
        VarintSerialize(buffer, static_cast<ui64>(msg.WriteTimestampDelta - LastWriteTimestampDelta));
        LastWriteTimestampDelta = msg.WriteTimestampDelta;
    }
};

template<>
struct TItemDeserializer<TSnapshotMessage> {
    ui64 LastWriteTimestampDelta = 0;

    bool Deserialize(const char*& data, const char* end, TSnapshotMessage& msg) {
        if (data + sizeof(msg.Common.Value) + 1 > end) {
            return false;
        }

        memcpy(&msg.Common.Value, data, sizeof(msg.Common.Value));  // TODO BIGENDIAN/LOWENDIAN
        data += sizeof(msg.Common.Value);

        ui64 delta;
        VarintDeserialize(data, delta);
        LastWriteTimestampDelta += delta;
        msg.WriteTimestampDelta = LastWriteTimestampDelta;

        return true;
    }
};

template<>
struct TItemSerializer<TAddedMessage> {
    ui64 LastWriteTimestampDelta = 0;

    void Serialize(TString& buffer, const TAddedMessage& msg) {
        buffer.append(reinterpret_cast<const char*>(&msg.MessageGroup.Value), sizeof(msg.MessageGroup.Value));
        VarintSerialize(buffer, static_cast<ui64>(msg.WriteTimestampDelta - LastWriteTimestampDelta));
        LastWriteTimestampDelta = msg.WriteTimestampDelta;
    }
};

template<>
struct TItemDeserializer<TAddedMessage> {
    ui64 LastWriteTimestampDelta = 0;

    bool Deserialize(const char*& data, const char* end, TAddedMessage& msg) {
        if (data + sizeof(msg.MessageGroup.Value) + 1 > end) {
            return false;
        }

        memcpy(&msg.MessageGroup.Value, data, sizeof(msg.MessageGroup.Value));  // TODO BIGENDIAN/LOWENDIAN
        data += sizeof(msg.MessageGroup.Value);

        ui64 delta;
        VarintDeserialize(data, delta);
        LastWriteTimestampDelta += delta;
        msg.WriteTimestampDelta = LastWriteTimestampDelta;

        return true;
    }
};


template<typename TMsg>
struct TSerializer {
    TString Buffer;
    TItemSerializer<TMsg> ItemSerializer;

    void Reserve(size_t size) {
        Buffer.reserve(size * sizeof(TMsg));
    }

    void Add(const TMsg& message) {
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

    bool Next(TMsg& message) {
        return ItemDeserializer.Deserialize(Data, End, message);
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
    ui64 LastOffset = 0;

    TDeserializerWithOffset(const TString& data)
        : Data(data.data())
        , End(data.data() + data.size())
    {
    }

    bool Next(ui64& offset, TMsg& message) {
        if (Data >= End) {
            return false;
        }

        ui64 delta;
        VarintDeserialize(Data, delta);
        LastOffset += delta;
        offset = LastOffset;

        return ItemDeserializer.Deserialize(Data, End, message);
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
    BaseDeadline = TInstant::Seconds(meta.GetBaseDeadlineSeconds());
    BaseWriteTimestamp = TInstant::Seconds(meta.GetBaseWriteTimestampSeconds());

    bool moveUnlockedOffset = true;
    bool moveUncommittedOffset = true;

    {
        TDeserializer<TMessage> deserializer(snapshot.GetMessages());
        TMessage message;
        size_t i = 0;
        while (deserializer.Next(message)) {
            Messages[i++] = message;

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
                    ++Metrics.DLQMessageCount;
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
    }

    {
        TDeserializerWithOffset<TMessage> deserializer(snapshot.GetSlowMessages());
        ui64 offset;
        TMessage message;
        while (deserializer.Next(offset, message)) {
            SlowMessages[offset] = message;

            switch(message.Status) {
                case EMessageStatus::Locked:
                    ++Metrics.LockedMessageCount;
                    if (KeepMessageOrder && message.HasMessageGroupId) {
                        LockedMessageGroupsId.insert(message.MessageGroupIdHash);
                        ++Metrics.LockedMessageGroupCount;
                    }
                    break;
                case EMessageStatus::Committed:
                    ++Metrics.CommittedMessageCount;
                    break;
                case EMessageStatus::Unprocessed:
                    ++Metrics.UnprocessedMessageCount;
                    break;
                case EMessageStatus::DLQ:
                    break;
            }
        }
    }

    Metrics.InflyMessageCount = Messages.size() + SlowMessages.size();

    for (auto offset : snapshot.GetDLQ()) {
        DLQQueue.push_back(offset);
    }

    return true;
}

bool TStorage::ApplyWAL(const NKikimrPQ::TMLPStorageWAL& wal) {
    AFL_ENSURE(wal.GetFormatVersion() == 1)("v", wal.GetFormatVersion());

    if (wal.HasBaseDeadlineSeconds() || wal.HasBaseWriteTimestampSeconds()) {
        auto newBaseDeadline = wal.HasBaseDeadlineSeconds() ? TInstant::Seconds(wal.GetBaseDeadlineSeconds()) : BaseDeadline;
        auto newBaseWriteTimestamp = wal.HasBaseWriteTimestampSeconds() ? TInstant::Seconds(wal.GetBaseWriteTimestampSeconds()) : BaseWriteTimestamp;
        MoveBaseDeadline(newBaseDeadline, newBaseWriteTimestamp);
    }

    std::unordered_map<ui64, TAddedMessage> newMessages;
    if (wal.HasAddedMessages()) {
        TDeserializerWithOffset<TAddedMessage> deserializer(wal.GetAddedMessages());

        ui64 offset;
        TAddedMessage msg;
        while (deserializer.Next(offset, msg)) {
            newMessages[offset] = msg;
        }
    }

    {
        ui64 offset = 0;
        for (auto diff : wal.GetMovedToSlowZone()) {
            offset += diff;

            auto [message, slowZone] = GetMessageInt(offset);
            if (message) {
                AFL_ENSURE(!slowZone)("o", offset);
                SlowMessages[offset] = *message;
                continue;
            }

            auto it = newMessages.find(offset);
            AFL_ENSURE(it != newMessages.end())("o", offset);
            auto& msg = it->second;
            SlowMessages[offset] = TMessage{
                .Status = EMessageStatus::Unprocessed,
                .ReceiveCount = 0,
                .DeadlineDelta = 0,
                .HasMessageGroupId = msg.MessageGroup.Fields.HasMessageGroupId,
                .MessageGroupIdHash = msg.MessageGroup.Fields.MessageGroupIdHash,
                .WriteTimestampDelta = msg.WriteTimestampDelta
            };

            ++Metrics.InflyMessageCount;
            ++Metrics.UnprocessedMessageCount;
        }
    }

    while (!Messages.empty() && FirstOffset < wal.GetFirstOffset()) {
        auto& message = Messages.front();
        if (!SlowMessages.contains(FirstOffset)) {
            RemoveMessage(message);
        }
        Messages.pop_front();
        ++FirstOffset;
    }

    FirstOffset = wal.GetFirstOffset();

    if (wal.HasAddedMessages()) {
        TDeserializerWithOffset<TAddedMessage> deserializer(wal.GetAddedMessages());

        ui64 offset;
        TAddedMessage msg;
        while (deserializer.Next(offset, msg)) {
            if (offset >= GetLastOffset()) {
                Messages.push_back({
                    .Status = EMessageStatus::Unprocessed,
                    .ReceiveCount = 0,
                    .DeadlineDelta = 0,
                    .HasMessageGroupId = msg.MessageGroup.Fields.HasMessageGroupId,
                    .MessageGroupIdHash = msg.MessageGroup.Fields.MessageGroupIdHash,
                    .WriteTimestampDelta = msg.WriteTimestampDelta
                });

                ++Metrics.InflyMessageCount;
                ++Metrics.UnprocessedMessageCount;
            }
        }
    }

    if (wal.HasChangedMessages()) {
        TDeserializerWithOffset<TMessageChange> deserializer(wal.GetChangedMessages());

        ui64 offset;
        TMessageChange msg;
        while (deserializer.Next(offset, msg)) {
            auto [message, _] = GetMessageInt(offset);
            if (!message) {
                continue;
            }

            auto statusChanged = message->Status != msg.Common.Fields.Status;
            if (statusChanged) {
                RemoveMessage(*message);
                ++Metrics.InflyMessageCount;
            }

            message->Status = msg.Common.Fields.Status;
            message->DeadlineDelta = msg.Common.Fields.DeadlineDelta;
            message->ReceiveCount = msg.Common.Fields.ReceiveCount;

            if (statusChanged) {
                switch(message->Status) {
                    case EMessageStatus::Locked:
                        ++Metrics.LockedMessageCount;
                        if (KeepMessageOrder && message->HasMessageGroupId) {
                            LockedMessageGroupsId.insert(message->MessageGroupIdHash);
                            ++Metrics.LockedMessageGroupCount;
                        }
                        break;
                    case EMessageStatus::Committed:
                        ++Metrics.CommittedMessageCount;
                        break;
                    case EMessageStatus::Unprocessed:
                        ++Metrics.UnprocessedMessageCount;
                        break;
                    case EMessageStatus::DLQ:
                        ++Metrics.DLQMessageCount;
                        break;
                }
            }
        }
    }

    {
        ui64 offset = 0;
        for (auto diff : wal.GetDeletedFromSlowZone()) {
            offset += diff;
            auto it = SlowMessages.find(offset);
            AFL_ENSURE(it != SlowMessages.end())("o", offset);
            auto& message = it->second;
            RemoveMessage(message);
            SlowMessages.erase(it);
        }
    }

    auto firstSlowOffset = wal.HasSlowFirstOffset() ? wal.GetSlowFirstOffset() : Max<ui64>();
    for (auto it = SlowMessages.begin(); it != SlowMessages.end(); ) {
        if (it->first >= firstSlowOffset) {
            break;
        }

        RemoveMessage(it->second);
        it = SlowMessages.erase(it);
    }

    {
        ui64 offset = 0;
        for (auto diff : wal.GetDLQ()) {
            offset += diff;
            DLQQueue.push_back(offset);
        }
    }

    FirstUncommittedOffset = std::max(FirstUncommittedOffset, FirstOffset);
    FirstUnlockedOffset = std::max(FirstUnlockedOffset, FirstOffset);

    // Reset changes
    Batch = { this };

    return true;
}

bool TStorage::SerializeTo(NKikimrPQ::TMLPStorageSnapshot& snapshot) {
    auto* meta = snapshot.MutableMeta();
    meta->SetFirstOffset(FirstOffset);
    meta->SetFirstUncommittedOffset(FirstUncommittedOffset);
    meta->SetBaseDeadlineSeconds(BaseDeadline.Seconds());
    meta->SetBaseWriteTimestampSeconds(BaseWriteTimestamp.Seconds());

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

    TSerializerWithOffset<TMessage> slowSerializer;
    slowSerializer.Reserve(SlowMessages.size());
    for (auto& message : SlowMessages) {
        slowSerializer.Add(message.first, message.second);
    }
    snapshot.SetSlowMessages(std::move(slowSerializer.Buffer));

    return true;
}

bool TStorage::TBatch::SerializeTo(NKikimrPQ::TMLPStorageWAL& wal) {
    wal.SetFormatVersion(1);
    wal.SetFirstOffset(Storage->FirstOffset);
    if (!Storage->SlowMessages.empty()) {
        wal.SetSlowFirstOffset(Storage->SlowMessages.begin()->first);
    }

    if (BaseDeadline) {
        wal.SetBaseDeadlineSeconds(BaseDeadline->Seconds());
    }
    if (BaseWriteTimestamp) {
        wal.SetBaseWriteTimestampSeconds(BaseWriteTimestamp->Seconds());
    }

    if (FirstNewMessage) {
        auto lastOffset = Storage->GetLastOffset();

        TSerializerWithOffset<TAddedMessage> serializer;
        serializer.Reserve(NewMessageCount);

        for (size_t offset = std::max(FirstNewMessage.value(), Storage->FirstOffset); offset < lastOffset; ++offset) {
            auto [message, _] = Storage->GetMessage(offset);
            if (message) {
                TAddedMessage msg;
                msg.MessageGroup.Fields.HasMessageGroupId = message->HasMessageGroupId;
                msg.MessageGroup.Fields.MessageGroupIdHash = message->MessageGroupIdHash;
                msg.WriteTimestampDelta = message->WriteTimestampDelta;
                serializer.Add(offset, msg);
            }
        }

        wal.SetAddedMessages(std::move(serializer.Buffer));
    }

    if (!ChangedMessages.empty()) {
        TSerializerWithOffset<TMessageChange> serializer;
        serializer.Reserve(ChangedMessages.size());
        std::sort(ChangedMessages.begin(), ChangedMessages.end());
        ui64 lastOffset = Max<ui64>();
        for (auto offset : ChangedMessages) {
            auto [message, _] = Storage->GetMessage(offset);
            if (lastOffset == offset) {
                continue;
            }
            lastOffset = offset;
            if (message) {
                TMessageChange msg;
                msg.Common.Fields.Status = message->Status;
                msg.Common.Fields.ReceiveCount = message->ReceiveCount;
                msg.Common.Fields.DeadlineDelta = message->DeadlineDelta;
                serializer.Add(offset, msg);
            }
        }

        wal.SetChangedMessages(std::move(serializer.Buffer));
    }

    {
        ui64 lastOffset = 0;
        for (auto offset : DLQ) {
            if (offset >= Storage->FirstOffset) {
                wal.AddDLQ(offset - lastOffset);
                lastOffset = offset;
            }
        }
    }

    {
        ui64 lastOffset = 0;
        for (auto offset : MovedToSlowZone) {
            wal.AddMovedToSlowZone(offset - lastOffset);
            lastOffset = offset;
        }
    }

    if (!Storage->SlowMessages.empty()) {
        ui64 lastOffset = 0;
        std::sort(DeletedFromSlowZone.begin(), DeletedFromSlowZone.end());
        for (auto offset : DeletedFromSlowZone) {
            if (offset >= Storage->SlowMessages.begin()->first) {
                wal.AddDeletedFromSlowZone(offset - lastOffset);
                lastOffset = offset;
            }
        }
    }

    return true;
}

} // namespace NKikimr::NPQ::NMLP
