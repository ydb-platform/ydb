#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/protos/base.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace {

    using TRead = TEvDataShard::TEvRead;

    struct TLegacyWireRead : NActors::TEventPB<TLegacyWireRead, NKikimrTxDataShard::TEvRead, TEvDataShard::EvRead> {};

    template <typename TEvent>
    TIntrusivePtr<NActors::TEventSerializedData> Serialize(const TEvent& event) {
        NActors::TAllocChunkSerializer serializer;
        UNIT_ASSERT(event.SerializeToArcadiaStream(&serializer));
        auto data = serializer.Release(event.CreateSerializationInfo(false));
        UNIT_ASSERT_VALUES_EQUAL(data->GetSize(), event.CalculateSerializedSize());
        return MakeIntrusive<NActors::TEventSerializedData>(data->GetString(), event.CreateSerializationInfo(false));
    }

    TString Key(ui64 value) {
        return TSerializedCellVec::Serialize({ TCell::Make(value) });
    }

} // namespace

Y_UNIT_TEST_SUITE(DataShardReadEvents) {
    Y_UNIT_TEST(SmallReadCompatibilityAndRepeatedSerialization) {
        TRead sender;
        sender.Record.SetReadId(42);
        sender.Keys.emplace_back(Key(1));
        sender.Keys.emplace_back(Key(2));
        sender.Keys.emplace_back(Key(3));
        auto data = Serialize(sender);
        for (size_t round = 0; round < 3; ++round) {
            THolder<TRead> read(TRead::Load(data.Get()));
            UNIT_ASSERT(!read->UsesCompactReadData());
            UNIT_ASSERT(read->PrepareReadData(1));
            UNIT_ASSERT_VALUES_EQUAL(read->GetKeysSize(), 3);
            UNIT_ASSERT_VALUES_EQUAL(read->Record.GetReadId(), 42);
            for (size_t i = 0; i < 3; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(read->GetKey(i).GetCells()[0].AsValue<ui64>(), i + 1);
            }
            const auto size = read->CalculateSerializedSize();
            UNIT_ASSERT_VALUES_EQUAL(read->CalculateSerializedSize(), size);
            data = Serialize(*read);
            THolder<TLegacyWireRead> legacy(TLegacyWireRead::Load(data.Get()));
            UNIT_ASSERT_VALUES_EQUAL(legacy->Record.KeysSize(), 3);
        }
    }

    Y_UNIT_TEST(CompactKeysOwnBuffersAndPadPrefixes) {
        const TString large(8193, 'x');
        TIntrusivePtr<NActors::TEventSerializedData> data;
        {
            TLegacyWireRead sender;
            sender.Record.SetReadId(77);
            sender.Record.MutableSnapshot()->SetStep(123);
            for (ui64 i = 0; i < 256; ++i) {
                sender.Record.AddKeys(
                    TSerializedCellVec::Serialize({ TCell::Make(i), TCell(large.data(), large.size()) }));
            }
            data = Serialize(sender);
        }
        THolder<TRead> read(TRead::Load(data.Get()));
        data.Reset();
        UNIT_ASSERT(read->UsesCompactReadData());
        UNIT_ASSERT(read->Keys.empty());
        UNIT_ASSERT_VALUES_EQUAL(read->GetKeysSize(), 256);
        UNIT_ASSERT(read->PrepareReadData(3));
        const auto first = read->GetKey(0).GetCells();
        UNIT_ASSERT_VALUES_EQUAL(first.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(read->GetKey(1).GetCells().data(), first.data() + 3);
        UNIT_ASSERT(read->PrepareReadData(3));
        UNIT_ASSERT_VALUES_EQUAL(read->GetKey(0).GetCells().data(), first.data());
        UNIT_ASSERT(!read->PrepareReadData(4));
        for (size_t i = 0; i < 256; ++i) {
            const auto cells = read->GetKey(i).GetCells();
            UNIT_ASSERT_VALUES_EQUAL(cells[0].AsValue<ui64>(), i);
            UNIT_ASSERT_VALUES_EQUAL(cells[1].AsBuf(), large);
            const auto lower = read->GetKeyLowerBound(i).GetCells();
            UNIT_ASSERT_VALUES_EQUAL(lower.size(), 3);
            UNIT_ASSERT(lower.back().IsNull());
            const auto& buffer = read->Record.GetKeys(i);
            UNIT_ASSERT(cells[1].Data() >= buffer.data());
            UNIT_ASSERT(cells[1].Data() + cells[1].Size() <= buffer.data() + buffer.size());
        }
        THolder<TLegacyWireRead> legacy(TLegacyWireRead::Load(Serialize(*read).Get()));
        UNIT_ASSERT_VALUES_EQUAL(legacy->Record.KeysSize(), 256);
        UNIT_ASSERT_VALUES_EQUAL(legacy->Record.GetSnapshot().GetStep(), 123);
    }

    Y_UNIT_TEST(CompactRangesNormalizeFlagsAndRoundTrip) {
        TLegacyWireRead sender;
        sender.Record.SetReadId(88);
        for (size_t i = 0; i < 256; ++i) {
            auto* range = sender.Record.AddRanges();
            range->SetFrom(i % 7 ? Key(i) : TString());
            range->SetTo(i % 11 ? Key(i + 1) : TString());
            range->SetFromInclusive(i & 1);
            range->SetToInclusive(i & 2);
        }
        THolder<TRead> read(TRead::Load(Serialize(sender).Get()));
        UNIT_ASSERT(read->UsesCompactReadData());
        UNIT_ASSERT(read->Ranges.empty());
        UNIT_ASSERT(read->PrepareReadData(3));
        for (size_t i = 0; i < 256; ++i) {
            const auto range = read->GetRange(i);
            const auto from = range.From.GetCells();
            const auto to = range.To.GetCells();
            const size_t rawFromSize = i % 7 ? 1 : 0;
            const size_t rawToSize = i % 11 ? 1 : 0;
            UNIT_ASSERT_VALUES_EQUAL(range.FromInclusive, bool(i & 1));
            UNIT_ASSERT_VALUES_EQUAL(range.ToInclusive, bool(i & 2));
            UNIT_ASSERT_VALUES_EQUAL(from.size(), range.FromInclusive ? 3 : rawFromSize);
            UNIT_ASSERT_VALUES_EQUAL(to.size(), !range.ToInclusive ? 3 : rawToSize);
            for (size_t col = rawFromSize; col < from.size(); ++col) {
                UNIT_ASSERT(from[col].IsNull());
            }
            for (size_t col = rawToSize; col < to.size(); ++col) {
                UNIT_ASSERT(to[col].IsNull());
            }
        }
        const auto size = read->CalculateSerializedSize();
        UNIT_ASSERT_VALUES_EQUAL(read->CalculateSerializedSize(), size);
        THolder<TLegacyWireRead> legacy(TLegacyWireRead::Load(Serialize(*read).Get()));
        UNIT_ASSERT_VALUES_EQUAL(legacy->Record.SerializeAsString(), sender.Record.SerializeAsString());
        THolder<TRead> reloaded(TRead::Load(Serialize(*read).Get()));
        UNIT_ASSERT(reloaded->PrepareReadData(3));
        UNIT_ASSERT_VALUES_EQUAL(reloaded->GetRangesSize(), 256);
    }

    Y_UNIT_TEST(CompactPreparationRejectsMalformedFrames) {
        TString truncated = Key(127);
        truncated.resize(truncated.size() - 1);
        for (const TString& invalid : { TString("\x01\0", 2), truncated }) {
            TLegacyWireRead sender;
            for (size_t i = 0; i < 128; ++i) {
                sender.Record.AddKeys(i == 127 ? invalid : Key(i));
            }
            THolder<TRead> read(TRead::Load(Serialize(sender).Get()));
            UNIT_ASSERT(read->UsesCompactReadData());
            UNIT_ASSERT(!read->PrepareReadData(1));
            UNIT_ASSERT_VALUES_EQUAL(read->GetKeysSize(), 128);
            UNIT_ASSERT(!read->PrepareReadData(1));
        }
    }

    Y_UNIT_TEST(CompactCountsPreserveMixedPayloadForValidation) {
        TLegacyWireRead sender;
        for (size_t i = 0; i < 128; ++i) {
            sender.Record.AddKeys(Key(i));
        }
        sender.Record.AddRanges()->SetFrom(Key(0));
        THolder<TRead> read(TRead::Load(Serialize(sender).Get()));
        UNIT_ASSERT_VALUES_EQUAL(read->GetKeysSize(), 128);
        UNIT_ASSERT_VALUES_EQUAL(read->GetRangesSize(), 1);
    }
}

} // namespace NKikimr
