#include "flat_dbase_sz_env.h"
#include "test/libs/table/test_writer.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NTable {
namespace {

    class TSizePageCollection : public NTest::TStorePageCollection {
    public:
        TSizePageCollection(TIntrusiveConstPtr<NTest::TStore> store, ui32 room, TLogoBlobID label)
            : TStorePageCollection(std::move(store), room)
            , Label_(label)
        {}

        const TLogoBlobID& Label() const noexcept override {
            return Label_;
        }

    private:
        const TLogoBlobID Label_;
    };

} // namespace

Y_UNIT_TEST_SUITE(TPageReadSize) {
    Y_UNIT_TEST(CountsEachCollectionPageOnce) {
        NTest::TLayoutCook layout;
        layout.Col(0, 0, NScheme::NTypeIds::Uint32).Col(0, 1, NScheme::NTypeIds::String).Key({ 0 });
        NPage::TConf conf;
        conf.Group(0).PageSize = 128;
        NTest::TPartCook cook(layout, conf);
        for (ui32 key : xrange(32)) {
            cook.AddN(key, TString(32, 'x'));
        }
        const auto source = cook.Finish().Lone();
        const ui32 slot = source->DataSlot({});
        TVector<TPageLocation> locations;
        TVector<TIntrusiveConstPtr<NPageCollection::IPageCollection>> descriptors;
        for (ui32 room : xrange(source->PageColls.size())) {
            descriptors.push_back(
                MakeIntrusive<TSizePageCollection>(source->Store, room, TLogoBlobID(1, 2, 3, 1, 0, room)));
        }
        for (ui32 id : xrange(source->Store->PageCollectionPagesCount(slot))) {
            if (source->Store->GetPageType(slot, id) == EPage::DataPage) {
                locations.push_back(source->Store->GetPageLocation(slot, id));
            }
        }
        UNIT_ASSERT(locations.size() >= 2);

        const auto makePart = [&](TConstArrayRef<TIntrusiveConstPtr<NPageCollection::IPageCollection>> collections) {
            TPart::TParams params{
                .Epoch = source->Epoch,
                .Scheme = source->Scheme,
                .IndexPages = source->IndexPages,
                .Blobs = source->Blobs,
                .ByKeyPrefixes = source->ByKeyPrefixes,
                .Large = source->Large,
                .Small = source->Small,
                .IndexesRawSize = source->IndexesRawSize,
                .MinRowVersion = source->MinRowVersion,
                .MaxRowVersion = source->MaxRowVersion,
                .GarbageStats = source->GarbageStats,
                .TxIdStats = source->TxIdStats,
                .Layout = source->Slots.GetLayout(),
            };
            auto part =
                MakeIntrusive<NTable::TPartStore>(collections.front()->Label(), std::move(params), source->Stat);
            for (const auto& descriptor : collections) {
                part->PageCollections.push_back(
                    MakeIntrusive<NTabletFlatExecutor::TPrivatePageCache::TPageCollection>(descriptor));
            }
            return part;
        };

        auto first = makePart(descriptors);
        auto second = makePart(descriptors);
        UNIT_ASSERT(first->PageCollections[slot].Get() != second->PageCollections[slot].Get());
        TSizeEnv env(nullptr);
        const auto& location = locations[0];
        UNIT_ASSERT(!env.TryGetPage(first.Get(), location, {}));
        UNIT_ASSERT(!env.TryGetPage(first.Get(), location, {}));
        UNIT_ASSERT_VALUES_EQUAL(env.GetSize(), location.Size);
        UNIT_ASSERT(!env.TryGetPage(second.Get(), location, {}));
        UNIT_ASSERT_VALUES_EQUAL(env.GetSize(), location.Size);

        descriptors[slot] = MakeIntrusive<TSizePageCollection>(source->Store, slot, descriptors[slot]->Label());
        auto reloaded = makePart(descriptors);
        UNIT_ASSERT(first->PageCollections[slot]->PageCollection.Get() != descriptors[slot].Get());
        UNIT_ASSERT(!env.TryGetPage(reloaded.Get(), location, {}));
        UNIT_ASSERT_VALUES_EQUAL(env.GetSize(), location.Size);

        const auto& next = locations[1];
        UNIT_ASSERT(!env.TryGetPage(reloaded.Get(), next, {}));
        UNIT_ASSERT_VALUES_EQUAL(env.GetSize(), location.Size + next.Size);

        descriptors[slot] = MakeIntrusive<TSizePageCollection>(source->Store, slot, TLogoBlobID(42, 2, 3, 1, 0, slot));
        auto different = makePart(descriptors);
        UNIT_ASSERT(!env.TryGetPage(different.Get(), location, {}));
        UNIT_ASSERT_VALUES_EQUAL(env.GetSize(), 2 * location.Size + next.Size);
    }
}

} // namespace NKikimr::NTable
