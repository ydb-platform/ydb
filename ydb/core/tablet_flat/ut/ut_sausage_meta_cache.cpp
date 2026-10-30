#include <flat_part_loader.h>
#include <flat_sausage_packet.h>
#include <flat_sausage_record.h>
#include <flat_store_hotdog.h>
#include <ydb/core/tablet_flat/protos/flat_executor.pb.h>
#include <shared_sausagecache_state.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPageCollection {
namespace {
    TSharedData MakeMeta() {
        TRecord record(0);
        record.Push(TLogoBlobID(1, 1, 1, 1, 16, 0));
        record.Push(ui32(NTable::NPage::EPage::DataPage), TString(16, 'x'));
        return record.Finish();
    }

    TLargeGlobId MetaId(const TSharedData& raw) {
        return { 0, TLogoBlobID(1, 1, 2, 1, raw.size(), 0), raw.size() };
    }
} // namespace

Y_UNIT_TEST_SUITE(TSharedMetaCore) {
    Y_UNIT_TEST(ComponentsUseSharedMetaForEpochAndSerialization) {
        NTable::NProto::TRoot root;
        root.SetEpoch(42);
        const auto body = root.SerializeAsString();
        TRecord record(0);
        record.Push(TLogoBlobID(1, 1, 1, 1, body.size(), 0));
        record.Push(ui32(NTable::NPage::EPage::Schem2), body);
        record.PushInplace(0, body);
        auto raw = record.Finish();

        NTable::TPartComponents parts{ .Epoch = NTable::TEpoch::Max() };
        auto& component = parts.PageCollectionComponents.emplace_back();
        component.LargeGlobId = MetaId(raw);
        component.SetMeta(raw);
        UNIT_ASSERT_VALUES_EQUAL(parts.GetEpoch().ToProto(), 42);
        NKikimrExecutorFlat::TBundle encoded;
        NTabletFlatExecutor::TPageCollectionProtoHelper(true).Do(&encoded, parts);
        UNIT_ASSERT_VALUES_EQUAL(encoded.GetPageCollections(0).GetMeta(), raw.ToString());
        encoded.ClearEpoch();
        auto decoded = NTabletFlatExecutor::TPageCollectionProtoHelper::MakePageCollectionComponents(encoded);
        UNIT_ASSERT(decoded.PageCollectionComponents[0].SharedMeta);
        UNIT_ASSERT_VALUES_EQUAL(decoded.GetEpoch().ToProto(), 42);
        UNIT_ASSERT_VALUES_EQUAL(decoded.PageCollectionComponents[0].SharedMeta->Raw.ToString(), raw.ToString());

        // An empty inline blob still represents metadata that must be loaded.
        encoded.MutablePageCollections(0)->SetMeta("");
        decoded = NTabletFlatExecutor::TPageCollectionProtoHelper::MakePageCollectionComponents(encoded);
        UNIT_ASSERT(!decoded.PageCollectionComponents[0].SharedMeta);
    }

    Y_UNIT_TEST(LoaderReleasesDuplicateRawAfterLookup) {
        auto raw = MakeMeta();
        const auto id = MetaId(raw);
        TAtomicSharedPtr<const TMeta> canonical = MakeAtomicShared<TMeta>(raw, id.Group);
        auto duplicate = TSharedData::Copy(raw.data(), raw.size());
        NTable::TPartComponents parts{ .Epoch = NTable::TEpoch::Max() };
        auto& component = parts.PageCollectionComponents.emplace_back();
        component.LargeGlobId = id;
        component.SetMeta(duplicate);
        NTable::TLoader loader(std::move(parts));
        auto fetch = loader.Run({});
        UNIT_ASSERT(fetch);
        UNIT_ASSERT(fetch.MetaOnly);
        UNIT_ASSERT(fetch.Pages.empty());
        UNIT_ASSERT(fetch.PageCollection->GetSharedMeta().Get() != canonical.Get());
        loader.Save({}, canonical);
        fetch.PageCollection.Reset();
        UNIT_ASSERT(duplicate.IsPrivate());
    }

    Y_UNIT_TEST(DescriptorsKeepPolicyAndShareCoreAccounting) {
        auto raw = MakeMeta();
        const auto id = MetaId(raw);
        TAtomicSharedPtr<const TMeta> meta = MakeAtomicShared<TMeta>(raw, id.Group);
        TIntrusiveConstPtr<TPageCollection> first = new TPageCollection(id, meta);
        TIntrusiveConstPtr<TPageCollection> second = new TPageCollection(id, meta);
        UNIT_ASSERT(&first->Meta == &second->Meta);
        first->SetSkipBTreeIndexV1Shadow(true);
        UNIT_ASSERT(!second->SkipBTreeIndexV1Shadow());

        NSharedCache::TCollections collections;
        auto& collection = collections[id.Lead];
        collection.Id = id.Lead;
        collection.SetPageCollection(first);
        NSharedCache::TRequest request;
        request.PageCollection = first;
        collection.AddMetadataRequest(request);
        UNIT_ASSERT(!collection.RetiredMetadata);
        collection.SetPageCollection(second);
        const i64 expected = 2 * sizeof(TPageCollection) + meta->GetMemoryUsage();
        UNIT_ASSERT_VALUES_EQUAL(collections.GetMetadataMemoryUsage(), expected);
        request.ReleaseMetadata();
        UNIT_ASSERT(!collection.RetiredMetadata);
        UNIT_ASSERT_VALUES_EQUAL(collections.GetMetadataMemoryUsage(), second->GetMetadataMemoryUsage());
        collection.SetPageCollection({});
        UNIT_ASSERT_VALUES_EQUAL(collections.GetMetadataMemoryUsage(), 0);

        // Independent cores can share Raw while retaining separate Steps allocations.
        TAtomicSharedPtr<const TMeta> separate = MakeAtomicShared<TMeta>(raw, id.Group);
        TIntrusiveConstPtr<TPageCollection> third = new TPageCollection(id, separate);
        collection.SetPageCollection(first);
        request.PageCollection = first;
        collection.AddMetadataRequest(request);
        collection.SetPageCollection(third);
        UNIT_ASSERT_VALUES_EQUAL(
            collections.GetMetadataMemoryUsage(), expected + separate->GetMemoryUsage() - raw.GetAllocationSize());
        request.ReleaseMetadata();
        collection.SetPageCollection({});
        UNIT_ASSERT_VALUES_EQUAL(collections.GetMetadataMemoryUsage(), 0);
    }

    Y_UNIT_TEST(RetiredDescriptorsCanBecomeCurrentAgain) {
        auto raw = MakeMeta();
        const auto id = MetaId(raw);
        TAtomicSharedPtr<const TMeta> meta = MakeAtomicShared<TMeta>(raw, id.Group);
        TIntrusiveConstPtr<TPageCollection> first = new TPageCollection(id, meta);
        TIntrusiveConstPtr<TPageCollection> second = new TPageCollection(id, meta);
        TIntrusiveConstPtr<TPageCollection> third = new TPageCollection(id, TSharedData::Copy(raw.data(), raw.size()));

        NSharedCache::TCollections collections;
        auto& collection = collections[id.Lead];
        collection.Id = id.Lead;
        collection.SetPageCollection(first);
        NSharedCache::TRequest firstRequest;
        firstRequest.PageCollection = first;
        collection.AddMetadataRequest(firstRequest);
        NSharedCache::TRequest anotherFirstRequest;
        anotherFirstRequest.PageCollection = first;
        collection.AddMetadataRequest(anotherFirstRequest);
        collection.SetPageCollection(first);
        UNIT_ASSERT(!collection.RetiredMetadata);
        UNIT_ASSERT_VALUES_EQUAL(collections.GetMetadataMemoryUsage(), first->GetMetadataMemoryUsage());

        collection.SetPageCollection(second);
        NSharedCache::TRequest secondRequest;
        secondRequest.PageCollection = second;
        collection.AddMetadataRequest(secondRequest);
        collection.SetPageCollection(third);
        NSharedCache::TRequest thirdRequest;
        thirdRequest.PageCollection = third;
        collection.AddMetadataRequest(thirdRequest);
        UNIT_ASSERT_VALUES_EQUAL(collection.RetiredMetadata->size(), 2);
        const i64 expected = 2 * sizeof(TPageCollection) + meta->GetMemoryUsage() + third->GetMetadataMemoryUsage();
        UNIT_ASSERT_VALUES_EQUAL(collections.GetMetadataMemoryUsage(), expected);

        collection.SetPageCollection(first);
        NSharedCache::TRequest newFirstRequest;
        newFirstRequest.PageCollection = first;
        collection.AddMetadataRequest(newFirstRequest);
        UNIT_ASSERT_VALUES_EQUAL(collection.RetiredMetadata->size(), 2);
        firstRequest.ReleaseMetadata();
        UNIT_ASSERT_VALUES_EQUAL(collections.GetMetadataMemoryUsage(), expected);
        secondRequest.ReleaseMetadata();
        UNIT_ASSERT_VALUES_EQUAL(collections.GetMetadataMemoryUsage(), expected - sizeof(TPageCollection));
        thirdRequest.ReleaseMetadata();
        UNIT_ASSERT(!collection.RetiredMetadata);
        UNIT_ASSERT_VALUES_EQUAL(collections.GetMetadataMemoryUsage(), first->GetMetadataMemoryUsage());

        collection.SetPageCollection({});
        UNIT_ASSERT_VALUES_EQUAL(collections.GetMetadataMemoryUsage(), first->GetMetadataMemoryUsage());
        collection.SetPageCollection(first);
        UNIT_ASSERT(!collection.RetiredMetadata);
        UNIT_ASSERT_VALUES_EQUAL(collections.GetMetadataMemoryUsage(), first->GetMetadataMemoryUsage());
        collection.SetPageCollection({});
        anotherFirstRequest.ReleaseMetadata();
        UNIT_ASSERT_VALUES_EQUAL(collections.GetMetadataMemoryUsage(), first->GetMetadataMemoryUsage());
        newFirstRequest.ReleaseMetadata();
        UNIT_ASSERT(!collection.RetiredMetadata);
        UNIT_ASSERT_VALUES_EQUAL(collections.GetMetadataMemoryUsage(), 0);
    }
}
} // namespace NKikimr::NPageCollection
