#include <ydb/core/tx/columnshard/blobs_action/tier/object_key.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/hash_set.h>
#include <util/string/split.h>

namespace NKikimr::NOlap::NBlobOperations::NTier {

namespace {

TLogoBlobID MakeBlobId(
    const ui32 channel, const ui64 tabletId = 72075186224037888, const ui32 generation = 3, const ui32 step = 17, const ui32 cookie = 5) {
    return TLogoBlobID(tabletId, generation, step, channel, 1024, cookie);
}

TVector<TString> SplitKey(const TString& key) {
    TVector<TString> parts;
    StringSplitter(key).Split('/').Collect(&parts);
    return parts;
}

}   // namespace

Y_UNIT_TEST_SUITE(TierObjectKey) {
    Y_UNIT_TEST(TreeLayout) {
        const TLogoBlobID blobId = MakeBlobId(TObjectKey::TreeLayoutChannel);
        const TString key = TObjectKey::Make(blobId);

        const TVector<TString> parts = SplitKey(key);
        UNIT_ASSERT_VALUES_EQUAL_C(parts.size(), 5, key);
        UNIT_ASSERT_VALUES_EQUAL(parts[0], ToString(blobId.TabletID()));
        UNIT_ASSERT_VALUES_EQUAL(parts[1], ToString(blobId.Generation()));
        UNIT_ASSERT_VALUES_EQUAL_C(parts[2].size(), 1, key);
        UNIT_ASSERT_VALUES_EQUAL_C(parts[3].size(), 1, key);
        UNIT_ASSERT_C(TString("0123456789abcdefghijklmnopqrstuvwxyz").Contains(parts[2]), key);
        UNIT_ASSERT_C(TString("0123456789abcdefghijklmnopqrstuvwxyz").Contains(parts[3]), key);
        UNIT_ASSERT_VALUES_EQUAL(parts[4], blobId.ToString());
    }

    Y_UNIT_TEST(FlatLayoutForLegacyBlobs) {
        const TLogoBlobID blobId = MakeBlobId(TObjectKey::FlatLayoutChannel);
        UNIT_ASSERT_VALUES_EQUAL(TObjectKey::Make(blobId), blobId.ToString());
    }

    Y_UNIT_TEST(ParseRoundTrip) {
        for (const ui32 channel : { TObjectKey::TreeLayoutChannel, TObjectKey::FlatLayoutChannel }) {
            const TLogoBlobID blobId = MakeBlobId(channel);
            TLogoBlobID parsed;
            TString error;
            UNIT_ASSERT_C(TObjectKey::Parse(TObjectKey::Make(blobId), parsed, error), error);
            UNIT_ASSERT_VALUES_EQUAL(parsed, blobId);
        }
    }

    Y_UNIT_TEST(ParseRejectsForeignKeys) {
        TLogoBlobID parsed;
        TString error;

        UNIT_ASSERT(!TObjectKey::Parse(MakeBlobId(TObjectKey::TreeLayoutChannel).ToString(), parsed, error));
        UNIT_ASSERT(!TObjectKey::Parse(TStringBuilder() << "1/2/a/b/" << MakeBlobId(TObjectKey::FlatLayoutChannel).ToString(), parsed, error));

        const TLogoBlobID blobId = MakeBlobId(TObjectKey::TreeLayoutChannel);
        const TVector<TString> parts = SplitKey(TObjectKey::Make(blobId));
        UNIT_ASSERT(!TObjectKey::Parse(TStringBuilder() << parts[0] << '/' << parts[1] << '/' << parts[2] << "/z/" << parts[4], parsed, error));
        UNIT_ASSERT(
            !TObjectKey::Parse(TStringBuilder() << "42/" << parts[1] << '/' << parts[2] << '/' << parts[3] << '/' << parts[4], parsed, error));

        UNIT_ASSERT(!TObjectKey::Parse("some/unrelated/object", parsed, error));
        UNIT_ASSERT(!TObjectKey::Parse("", parsed, error));
    }

    Y_UNIT_TEST(FanoutSpreadsBlobs) {
        THashSet<TString> prefixes;
        for (ui32 cookie = 0; cookie < 1000; ++cookie) {
            const TString key = TObjectKey::Make(MakeBlobId(TObjectKey::TreeLayoutChannel, 72075186224037888, 3, 17, cookie));
            const TVector<TString> parts = SplitKey(key);
            UNIT_ASSERT_VALUES_EQUAL_C(parts.size(), 5, key);
            prefixes.emplace(TStringBuilder() << parts[2] << '/' << parts[3]);
        }

        UNIT_ASSERT_GT(prefixes.size(), 100);
    }

    Y_UNIT_TEST(BlobsOfOneTabletShareTheirSubtree) {
        const ui64 tabletId = 72075186224037888;
        const TString otherTabletKey = TObjectKey::Make(MakeBlobId(TObjectKey::TreeLayoutChannel, tabletId + 1));
        const TString key = TObjectKey::Make(MakeBlobId(TObjectKey::TreeLayoutChannel, tabletId));
        UNIT_ASSERT(key.StartsWith(TStringBuilder() << tabletId << '/'));
        UNIT_ASSERT(!otherTabletKey.StartsWith(TStringBuilder() << tabletId << '/'));
    }
}

}   // namespace NKikimr::NOlap::NBlobOperations::NTier
