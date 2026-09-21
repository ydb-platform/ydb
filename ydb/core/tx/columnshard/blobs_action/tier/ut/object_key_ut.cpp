// TODO: Enable with the tiering tree object key implementation (PR #52993).
#if 0
// TODO: Include <ydb/core/tx/columnshard/blobs_action/tier/object_key.h> with the implementation (PR #52993).

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/hash_set.h>
#include <util/string/split.h>

namespace NKikimr::NOlap::NBlobOperations::NTier {

namespace {

TLogoBlobID MakeBlobId(
    const ui32 channel, const ui64 tabletId = 72075186224037888, const ui32 generation = 3, const ui32 step = 17, const ui32 cookie = 5) {
    return TLogoBlobID(tabletId, generation, step, channel, 1024, cookie);
}

TObjectKey MakeObjectKey(const TString& prefix = {}) {
    return TObjectKey(NColumnShard::NTiers::TExternalStorageId("/Root/tier", prefix).ToString());
}

TVector<TString> SplitKey(const TString& key) {
    TVector<TString> parts;
    StringSplitter(key).Split('/').Collect(&parts);
    return parts;
}

}

Y_UNIT_TEST_SUITE(TierObjectKey) {

    Y_UNIT_TEST(PrefixAndLayoutSurviveSerialization) {
        using NColumnShard::NTiers::TExternalStorageId;
        for (const std::optional<TString>& prefix : {std::optional<TString>(), std::make_optional(TString()),
                std::make_optional(TString("archive//2026:09/"))}) {
            const TExternalStorageId original("/Root/tier:with.dots", prefix);
            const auto restored = TExternalStorageId::FromString(original.ToString());
            UNIT_ASSERT_VALUES_EQUAL(restored, original);
            UNIT_ASSERT_VALUES_EQUAL(restored.GetConfigPath(), "/Root/tier:with.dots");
            UNIT_ASSERT(restored.GetObjectKeyPrefix() == prefix);
            const TObjectKey keys(restored.ToString());
            UNIT_ASSERT_VALUES_EQUAL(keys.GetChannelForWriting(),
                prefix ? TObjectKey::TreeLayoutChannel : TObjectKey::FlatLayoutChannel);
            const auto blob = MakeBlobId(keys.GetChannelForWriting());
            const auto key = keys.Make(blob);
            TLogoBlobID parsed;
            TString error;
            UNIT_ASSERT_C(keys.Parse(key, parsed, error), error);
            UNIT_ASSERT_VALUES_EQUAL(parsed, blob);
            if (prefix && !prefix->empty()) {
                UNIT_ASSERT_C(key.StartsWith(*prefix), key);
                UNIT_ASSERT(!MakeObjectKey("other").Parse(key, parsed, error));
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(TExternalStorageId("/Root/tier:legacy").ToString(), "/Root/tier:legacy");
        UNIT_ASSERT_VALUES_EQUAL(TExternalStorageId("s3:legacy").ToString(), "/s3:legacy");
        UNIT_ASSERT_VALUES_EQUAL(TExternalStorageId("/Root/tier", TString("archive")).ToString(), "s3:10:/Root/tierarchive");
        UNIT_ASSERT(TExternalStorageId("/Root/tier") != TExternalStorageId("/Root/tier", TString()));
        UNIT_ASSERT(TExternalStorageId("/Root/tier", TString("a")) != TExternalStorageId("/Root/tier", TString("b")));
    }

    Y_UNIT_TEST(LegacyKeysDoNotDependOnPrefix) {
        const auto blob = MakeBlobId(TObjectKey::FlatLayoutChannel);
        const auto keys = MakeObjectKey("archive/data");
        UNIT_ASSERT_VALUES_EQUAL(keys.Make(blob), blob.ToString());
        TLogoBlobID parsed;
        TString error;
        UNIT_ASSERT_C(keys.Parse(blob.ToString(), parsed, error), error);
        UNIT_ASSERT_VALUES_EQUAL(parsed, blob);
    }

    Y_UNIT_TEST(PrefixIsKeptVerbatim) {
        const auto blob = MakeBlobId(TObjectKey::TreeLayoutChannel);
        UNIT_ASSERT_VALUES_EQUAL(MakeObjectKey("archive/").Make(blob),
            "archive//" + MakeObjectKey().Make(blob));
    }

    Y_UNIT_TEST(PersistedKeyIsStable) {
        UNIT_ASSERT_VALUES_EQUAL(MakeObjectKey("archive").Make(MakeBlobId(TObjectKey::TreeLayoutChannel)),
            "archive/72075186224037888/3/t/q/[72075186224037888:3:17:254:5:1024:0]");
    }

    Y_UNIT_TEST(TreeLayout) {
        const TLogoBlobID blobId = MakeBlobId(TObjectKey::TreeLayoutChannel);
        const TString key = MakeObjectKey().Make(blobId);

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
        UNIT_ASSERT_VALUES_EQUAL(MakeObjectKey().Make(blobId), blobId.ToString());
    }

    Y_UNIT_TEST(ParseRoundTrip) {
        for (const ui32 channel : { TObjectKey::TreeLayoutChannel, TObjectKey::FlatLayoutChannel }) {
            const TLogoBlobID blobId = MakeBlobId(channel);
            TLogoBlobID parsed;
            TString error;
            UNIT_ASSERT_C(MakeObjectKey().Parse(MakeObjectKey().Make(blobId), parsed, error), error);
            UNIT_ASSERT_VALUES_EQUAL(parsed, blobId);
        }
    }

    Y_UNIT_TEST(ParseRejectsForeignKeys) {
        TLogoBlobID parsed;
        TString error;

        UNIT_ASSERT(!MakeObjectKey().Parse(MakeBlobId(TObjectKey::TreeLayoutChannel).ToString(), parsed, error));
        UNIT_ASSERT(!MakeObjectKey().Parse(TStringBuilder() << "1/2/a/b/" << MakeBlobId(TObjectKey::FlatLayoutChannel).ToString(), parsed, error));

        const TLogoBlobID blobId = MakeBlobId(TObjectKey::TreeLayoutChannel);
        const TVector<TString> parts = SplitKey(MakeObjectKey().Make(blobId));
        UNIT_ASSERT(!MakeObjectKey().Parse(TStringBuilder() << parts[0] << '/' << parts[1] << '/' << parts[2] << "/z/" << parts[4], parsed, error));
        UNIT_ASSERT(
            !MakeObjectKey().Parse(TStringBuilder() << "42/" << parts[1] << '/' << parts[2] << '/' << parts[3] << '/' << parts[4], parsed, error));

        UNIT_ASSERT(!MakeObjectKey().Parse("some/unrelated/object", parsed, error));
        UNIT_ASSERT(!MakeObjectKey().Parse("", parsed, error));
    }

    Y_UNIT_TEST(FanoutSpreadsBlobs) {
        THashSet<TString> prefixes;
        for (ui32 cookie = 0; cookie < 1000; ++cookie) {
            const TString key = MakeObjectKey().Make(MakeBlobId(TObjectKey::TreeLayoutChannel, 72075186224037888, 3, 17, cookie));
            const TVector<TString> parts = SplitKey(key);
            UNIT_ASSERT_VALUES_EQUAL_C(parts.size(), 5, key);
            prefixes.emplace(TStringBuilder() << parts[2] << '/' << parts[3]);
        }

        UNIT_ASSERT_GT(prefixes.size(), 100);
    }

    Y_UNIT_TEST(BlobsOfOneTabletShareTheirSubtree) {
        const ui64 tabletId = 72075186224037888;
        const TString otherTabletKey = MakeObjectKey().Make(MakeBlobId(TObjectKey::TreeLayoutChannel, tabletId + 1));
        const TString key = MakeObjectKey().Make(MakeBlobId(TObjectKey::TreeLayoutChannel, tabletId));
        UNIT_ASSERT(key.StartsWith(TStringBuilder() << tabletId << '/'));
        UNIT_ASSERT(!otherTabletKey.StartsWith(TStringBuilder() << tabletId << '/'));
    }
}

}
#endif
