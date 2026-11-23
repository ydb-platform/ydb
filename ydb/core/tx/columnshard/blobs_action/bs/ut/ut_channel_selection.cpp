#include <ydb/core/base/appdata.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/tx/columnshard/blobs_action/bs/blob_manager.h>

#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/guid.h>

namespace {

class TFixedRandomProvider: public IRandomProvider {
public:
    explicit TFixedRandomProvider(ui64 value)
        : Value(value)
    {
    }

    ui64 GenRand() noexcept override {
        return Value;
    }

    TGUID GenGuid() noexcept override {
        return {};
    }

    TGUID GenUuid4() noexcept override {
        return {};
    }

private:
    ui64 Value;
};

// GenRandReal1(x) = (x >> 11) / (2^53 - 1). This value is just under 0.5.
constexpr ui64 HalfReal1Rand() {
    return (9007199254740991ull / 2) << 11;
}

struct TRandomGuard {
    TIntrusivePtr<IRandomProvider> Previous;

    explicit TRandomGuard(ui64 value)
        : Previous(NKikimr::TAppData::RandomProvider)
    {
        NKikimr::TAppData::RandomProvider = new TFixedRandomProvider(value);
    }

    ~TRandomGuard() {
        NKikimr::TAppData::RandomProvider = Previous;
    }
};

TIntrusivePtr<NKikimr::TTabletStorageInfo> MakeTabletInfo(ui32 channelCount) {
    auto info = MakeIntrusive<NKikimr::TTabletStorageInfo>(ui64(1), NKikimr::TTabletTypes::ColumnShard);
    info->Channels.reserve(channelCount);
    for (ui32 channel = 0; channel < channelCount; ++channel) {
        NKikimr::TTabletChannelInfo channelInfo(channel, NKikimr::TBlobStorageGroupType::ErasureNone);
        channelInfo.History.emplace_back(/*fromGeneration*/ 1, /*groupId*/ 10 + channel);
        info->Channels.push_back(std::move(channelInfo));
    }
    return info;
}

ui32 ChannelOfNextBatch(NKikimr::NOlap::TBlobManager& manager) {
    auto batch = manager.StartBlobBatch();
    return batch.AllocateNextBlobId(TString(8, 'x')).Channel();
}

}   // namespace

Y_UNIT_TEST_SUITE(TBlobManagerDataChannel) {
    Y_UNIT_TEST(RoundRobinWhenFlagOff) {
        NKikimr::NOlap::TBlobManager manager(MakeTabletInfo(5), /*gen*/ 1, NKikimr::NOlap::TTabletId{ 1 }, false);

        // CurrentStep starts at 0 and is incremented before the pick: (step % 3) + 2.
        UNIT_ASSERT_VALUES_EQUAL(ChannelOfNextBatch(manager), 3u);
        UNIT_ASSERT_VALUES_EQUAL(ChannelOfNextBatch(manager), 4u);
        UNIT_ASSERT_VALUES_EQUAL(ChannelOfNextBatch(manager), 2u);
        UNIT_ASSERT_VALUES_EQUAL(ChannelOfNextBatch(manager), 3u);
    }

    Y_UNIT_TEST(DefaultFlagKeepsRoundRobin) {
        NKikimr::NOlap::TBlobManager manager(MakeTabletInfo(5), /*gen*/ 1, NKikimr::NOlap::TTabletId{ 1 });

        UNIT_ASSERT_VALUES_EQUAL(ChannelOfNextBatch(manager), 3u);
        UNIT_ASSERT_VALUES_EQUAL(ChannelOfNextBatch(manager), 4u);
        UNIT_ASSERT_VALUES_EQUAL(ChannelOfNextBatch(manager), 2u);
    }

    Y_UNIT_TEST(WeightedStaysOnDataChannels) {
        NKikimr::NOlap::TBlobManager manager(MakeTabletInfo(5), /*gen*/ 1, NKikimr::NOlap::TTabletId{ 1 }, true);

        {
            TRandomGuard guard(0);
            const ui32 channel = ChannelOfNextBatch(manager);
            UNIT_ASSERT(channel >= 2 && channel <= 4);
            UNIT_ASSERT_VALUES_EQUAL(channel, 2u);
        }
        {
            TRandomGuard guard(Max<ui64>());
            const ui32 channel = ChannelOfNextBatch(manager);
            UNIT_ASSERT(channel >= 2 && channel <= 4);
            UNIT_ASSERT_VALUES_EQUAL(channel, 4u);
        }
    }

    Y_UNIT_TEST(WeightedPrefersFreerChannel) {
        NKikimr::NOlap::TBlobManager manager(MakeTabletInfo(5), /*gen*/ 1, NKikimr::NOlap::TTabletId{ 1 }, true);
        manager.UpdateChannelApproximateFreeSpace(2, 0.01f);
        manager.UpdateChannelApproximateFreeSpace(3, 0.01f);
        manager.UpdateChannelApproximateFreeSpace(4, 1.0f);

        TRandomGuard guard(HalfReal1Rand());
        UNIT_ASSERT_VALUES_EQUAL(ChannelOfNextBatch(manager), 4u);
        UNIT_ASSERT_VALUES_EQUAL(ChannelOfNextBatch(manager), 4u);
    }

    Y_UNIT_TEST(SingleDataChannelIgnoresRandom) {
        NKikimr::NOlap::TBlobManager manager(MakeTabletInfo(3), /*gen*/ 1, NKikimr::NOlap::TTabletId{ 1 }, true);

        TRandomGuard guard(HalfReal1Rand());
        UNIT_ASSERT_VALUES_EQUAL(ChannelOfNextBatch(manager), 2u);
        UNIT_ASSERT_VALUES_EQUAL(ChannelOfNextBatch(manager), 2u);
    }

    Y_UNIT_TEST(RecordedSharesDoNotChangeRoundRobin) {
        NKikimr::NOlap::TBlobManager manager(MakeTabletInfo(5), /*gen*/ 1, NKikimr::NOlap::TTabletId{ 1 }, false);

        UNIT_ASSERT_VALUES_EQUAL(ChannelOfNextBatch(manager), 3u);
        manager.UpdateChannelApproximateFreeSpace(2, 1.0f);
        manager.UpdateChannelApproximateFreeSpace(3, 0.01f);
        manager.UpdateChannelApproximateFreeSpace(4, 0.01f);
        // Flag was off at construction. The next step is 2, so round-robin stays on channel 4.
        UNIT_ASSERT_VALUES_EQUAL(ChannelOfNextBatch(manager), 4u);
        UNIT_ASSERT_VALUES_EQUAL(ChannelOfNextBatch(manager), 2u);
    }
}
