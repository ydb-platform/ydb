#include <ydb/core/mind/bscontroller/impl.h>
#include <ydb/core/tablet_flat/test/libs/table/test_dummy.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NBsController {
namespace {

// Frozen stable-26-3-1 definition. Do not inherit the current schema: this reader
// must keep constructing six-column lower bounds after the schema is upgraded.
struct TLegacySchema : NIceDb::Schema {
    struct BoxStoragePoolPDiskFilter : Table<122> {
        struct BoxId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct StoragePoolId : Column<2, NScheme::NTypeIds::Uint64> {};
        struct TypeCol : Column<3, NScheme::NTypeIds::Int32> {
            static TString GetColumnName(const TString&) { return "Type"; }
        };
        struct SharedWithOs : Column<4, NScheme::NTypeIds::Bool> {};
        struct ReadCentric : Column<5, NScheme::NTypeIds::Bool> {};
        struct Kind : Column<6, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<BoxId, StoragePoolId, TypeCol, SharedWithOs, ReadCentric, Kind>;
        using TColumns = TableColumns<BoxId, StoragePoolId, TypeCol, SharedWithOs, ReadCentric, Kind>;
    };

    using TTables = SchemaTables<BoxStoragePoolPDiskFilter>;
};

// Released V2 readers decode the key directly into a protobuf and reserialize it when deleting.
struct TReleasedV2Schema : NIceDb::Schema {
    struct BoxStoragePoolPDiskFilterV2 : Table<136> {
        struct BoxId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct StoragePoolId : Column<2, NScheme::NTypeIds::Uint64> {};
        struct Filter : Column<3, NScheme::NTypeIds::String> { using Type = NKikimrBlobStorage::TPDiskFilter; };
        using TKey = TableKey<BoxId, StoragePoolId, Filter>;
        using TColumns = TableColumns<BoxId, StoragePoolId, Filter>;
    };
    using TTables = SchemaTables<BoxStoragePoolPDiskFilterV2>;
};

using TPool = TBlobStorageController::TStoragePoolInfo;
using TFilter = TPool::TPDiskFilter;
using TFilters = TPool::TPDiskFilters;
using TLegacyTable = TLegacySchema::BoxStoragePoolPDiskFilter;

struct TFixture {
    NTable::TDatabase Database;
    NTable::TDummyEnv Env;
    NIceDb::TNiceDb Db{Database};
    ui64 Stamp = 0;

    template<typename TCallback>
    void Transaction(TCallback&& callback) {
        Database.Begin(++Stamp, Env);
        callback();
        Database.Commit(Stamp, true);
    }

    TFilters Load(ui64 boxId, ui64 poolId) {
        TPool pool;
        Transaction([&] {
            UNIT_ASSERT(pool.LoadPDiskFilters(Db, {boxId, poolId}));
        });
        return std::move(pool.PDiskFilters);
    }

    void Update(const TFilters& previous, const TFilters& current) {
        Transaction([&] {
            TPool::UpdatePDiskFilters(Database, &previous, &current);
        });
    }

    void InsertV2(const TString& key) {
        Transaction([&] {
            Db.Table<Schema::BoxStoragePoolPDiskFilterV2>().Key(7, 11, key).Update();
        });
    }

    TSet<TString> V2Keys() {
        TSet<TString> keys;
        Transaction([&] {
            using T = Schema::BoxStoragePoolPDiskFilterV2;
            auto rows = Db.Table<T>().Range(7, 11).Select();
            UNIT_ASSERT(rows.IsReady());
            while (rows.IsValid()) {
                keys.insert(rows.GetValue<T::Filter>());
                UNIT_ASSERT(rows.Next());
            }
        });
        return keys;
    }

    template<typename T>
    size_t Count(ui64 boxId, ui64 poolId) {
        size_t count = 0;
        Transaction([&] {
            auto rows = Db.Table<T>().Range(boxId, poolId).Select();
            UNIT_ASSERT(rows.IsReady());
            while (rows.IsValid()) {
                ++count;
                UNIT_ASSERT(rows.Next());
            }
        });
        return count;
    }
};

TFilter MakeFilter(NKikimrBlobStorage::EPDiskType type, TMaybe<TString> dataCenter = {}) {
    NKikimrBlobStorage::TPDiskFilter proto;
    proto.AddProperty()->SetType(type);
    if (dataCenter) {
        proto.AddProperty()->MutableLocation()->SetDataCenter(*dataCenter);
    }
    return TFilter::Parse(7, 11, proto);
}

void CheckFilters(const TFilters& actual, const TFilters& expected) {
    UNIT_ASSERT_VALUES_EQUAL(actual.size(), expected.size());
    auto a = actual.begin();
    auto e = expected.begin();
    for (; a != actual.end(); ++a, ++e) {
        UNIT_ASSERT(a->GetKey() == e->GetKey());
        UNIT_ASSERT_VALUES_EQUAL(bool(a->RequiredLocation), bool(a->Location));
    }
}

} // namespace

Y_UNIT_TEST_SUITE(BsControllerPDiskFilterCompatibility) {
    Y_UNIT_TEST(KnownV2FiltersCanStillBeReadAndDeletedByReleasedCode) {
        TFixture f;
        f.Transaction([&] { f.Db.Materialize<Schema>(); });
        f.Update({}, {MakeFilter(NKikimrBlobStorage::SSD, TString("dc1"))});
        f.Transaction([&] {
            f.Db.Materialize<TReleasedV2Schema>();
            using T = TReleasedV2Schema::BoxStoragePoolPDiskFilterV2;
            auto rows = f.Db.Table<T>().Range(7, 11).Select();
            UNIT_ASSERT(rows.IsReady() && rows.IsValid());
            const auto proto = rows.GetValue<T::Filter>();
            // Reconstruct only the fields known to the released reader.
            NKikimrBlobStorage::TPDiskFilter key;
            key.AddProperty()->SetType(proto.GetProperty(0).GetType());
            *key.AddProperty()->MutableLocation() = proto.GetProperty(1).GetLocation();
            const auto values = std::make_tuple(ui64(7), ui64(11), key.SerializeAsString());
            auto keyTuple = NTableAdapter::PrepareKeyTuple<T>(&values);
            TStackVec<TRawTypeValue, std::tuple_size_v<decltype(keyTuple)>> rawKey;
            NTableAdapter::MapKey<T>(&keyTuple, rawKey);
            f.Database.Update(T::TableId, NTable::ERowOp::Erase, rawKey, {});
        });
        UNIT_ASSERT(f.V2Keys().empty());
    }

    Y_UNIT_TEST(UnknownMetadataKeepsExactKeysThroughRewriteAndDelete) {
        TFixture f;
        f.Transaction([&] { f.Db.Materialize<Schema>(); });
        NKikimrBlobStorage::TPDiskFilter known;
        known.AddProperty()->SetType(NKikimrBlobStorage::ROT);
        TSet<TString> originals;
        for (const char* name : {"first", "second"}) {
            NKikimrBlobStorage::TPDiskFilter metadata;
            metadata.GetReflection()->MutableUnknownFields(&metadata)->AddLengthDelimited(100, name);
            // Unknown fields precede known ones on the wire. Protobuf reserialization changes these bytes.
            const TString raw = metadata.SerializeAsString() + known.SerializeAsString();
            NKikimrBlobStorage::TPDiskFilter parsed;
            UNIT_ASSERT(parsed.ParseFromString(raw));
            UNIT_ASSERT(parsed.SerializeAsString() != raw);
            originals.insert(raw);
            f.InsertV2(raw);
        }
        auto loaded = f.Load(7, 11);
        UNIT_ASSERT_VALUES_EQUAL(loaded.size(), 2); // Equal known fields must not hide different future metadata.
        UNIT_ASSERT(f.V2Keys() == originals);
        TFilters rewritten;
        for (const auto& filter : loaded) {
            UNIT_ASSERT(filter.SerializedFilter);
            UNIT_ASSERT(originals.contains(*filter.SerializedFilter));
            UNIT_ASSERT(filter.MatchPDisk(TPDiskCategory(NPDisk::DEVICE_TYPE_ROT, 0).GetRaw(), {}, {}, {}));
            NKikimrBlobStorage::TPDiskFilter proto;
            TBlobStorageController::Serialize(&proto, filter);
            UNIT_ASSERT_VALUES_EQUAL(proto.GetReflection()->GetUnknownFields(proto).field_count(), 1);
            rewritten.insert(TFilter::Parse(7, 11, proto));
        }
        f.Update(loaded, rewritten);
        UNIT_ASSERT_VALUES_EQUAL(f.V2Keys().size(), 2);
        for (const auto& key : f.V2Keys()) {
            UNIT_ASSERT(!originals.contains(key)); // The old keys must actually be erased.
        }
        CheckFilters(f.Load(7, 11), rewritten);

        f.Update(f.Load(7, 11), {});
        UNIT_ASSERT(f.V2Keys().empty());
    }

    Y_UNIT_TEST(UnknownConstraintsSurviveWithoutMatchingDisks) {
        for (ui32 variant = 0; variant < 4; ++variant) {
            TFixture f;
            f.Transaction([&] { f.Db.Materialize<Schema>(); });
            NKikimrBlobStorage::TPDiskFilter proto;
            if (variant != 3) {
                proto.AddProperty()->SetType(NKikimrBlobStorage::ROT);
            }
            if (variant == 0 || variant == 3) {
                auto* property = proto.AddProperty();
                property->GetReflection()->MutableUnknownFields(property)->AddVarint(100, 7);
            } else if (variant == 1) {
                // A future oneof alternative can override a known field in the same nested message.
                auto* property = proto.MutableProperty(0);
                property->GetReflection()->MutableUnknownFields(property)->AddVarint(100, 7);
            } else {
                auto* location = proto.AddProperty()->MutableLocation();
                location->SetDataCenter("dc1");
                location->mutable_unknown_fields()->AddVarint(100, 7);
            }
            const auto bytes = proto.SerializeAsString();
            f.InsertV2(bytes);
            const auto loaded = f.Load(7, 11);
            UNIT_ASSERT_VALUES_EQUAL(loaded.size(), 1);
            const auto& filter = *loaded.begin();
            UNIT_ASSERT(filter.HasUnknownRequiredProperty);
            UNIT_ASSERT(!filter.MatchPDisk(TPDiskCategory(NPDisk::DEVICE_TYPE_ROT, 0).GetRaw(), {}, {}, {}));
            NKikimrBlobStorage::TPDiskFilter returned;
            TBlobStorageController::Serialize(&returned, filter);
            UNIT_ASSERT_VALUES_EQUAL(returned.SerializeAsString(), bytes);
            f.Update(loaded, {});
            UNIT_ASSERT(f.V2Keys().empty());
        }
    }

    Y_UNIT_TEST(AllEncodingsOfTheSameKnownFilterAreDeleted) {
        TFixture f;
        f.Transaction([&] { f.Db.Materialize<Schema>(); });
        NKikimrBlobStorage::TPDiskFilter proto;
        proto.AddProperty()->SetType(NKikimrBlobStorage::ROT);
        proto.AddProperty()->SetKind(7);
        f.Update({}, {TFilter::Parse(7, 11, proto)}); // Legacy table.
        f.InsertV2(proto.SerializeAsString()); // V2 without Location still has to be deleted from V2.
        proto.MutableProperty()->SwapElements(0, 1);
        f.InsertV2(proto.SerializeAsString());
        const auto loaded = f.Load(7, 11);
        UNIT_ASSERT_VALUES_EQUAL(loaded.size(), 3);
        f.Update(loaded, {});
        UNIT_ASSERT_VALUES_EQUAL(f.Count<TLegacyTable>(7, 11), 0);
        UNIT_ASSERT(f.V2Keys().empty());
    }

    Y_UNIT_TEST(LegacyReadAndWriteAfterUpgrade) {
        TFixture f;
        f.Transaction([&] {
            f.Db.Materialize<TLegacySchema>();
            f.Db.Table<TLegacyTable>().Key(7, 11, i32(NKikimrBlobStorage::ROT), false, true, 3).Update();
        });
        f.Transaction([&] {
            f.Db.Materialize<Schema>();
        });
        UNIT_ASSERT_VALUES_EQUAL(f.Database.GetScheme().GetTableInfo(122)->KeyColumns.size(), 6);

        auto original = MakeFilter(NKikimrBlobStorage::ROT);
        original.SharedWithOs = false;
        original.ReadCentric = true;
        original.Kind = 3;
        const TFilters oldFilters{original};
        CheckFilters(f.Load(7, 11), oldFilters);

        // A current controller writes a filter with nullable key fields. The old
        // reader must see it, and its prefix scan must not abort after upgrade.
        const auto added = MakeFilter(NKikimrBlobStorage::SSD);
        const TFilters newFilters{original, added};
        f.Update(oldFilters, newFilters);
        f.Transaction([&] {
            f.Db.Materialize<TLegacySchema>();
        });
        UNIT_ASSERT_VALUES_EQUAL(f.Count<TLegacyTable>(7, 11), 2);
        UNIT_ASSERT_VALUES_EQUAL(f.Count<Schema::BoxStoragePoolPDiskFilterV2>(7, 11), 0);

        // Stable can change the filters after downgrade; a subsequent upgrade
        // must read those changes, rather than resurrecting a separate copy.
        f.Transaction([&] {
            f.Db.Table<TLegacyTable>().Key(7, 11, i32(NKikimrBlobStorage::ROT), false, true, 3).Delete();
            f.Db.Table<TLegacyTable>().Key(7, 11, i32(NKikimrBlobStorage::ROT), true, false, 5).Update();
        });
        original.SharedWithOs = true;
        original.ReadCentric = false;
        original.Kind = 5;
        f.Transaction([&] {
            f.Db.Materialize<Schema>();
        });
        CheckFilters(f.Load(7, 11), {original, added});
    }

    Y_UNIT_TEST(LocationFiltersPersistAndSwitchTables) {
        TFixture f;
        f.Transaction([&] {
            f.Db.Materialize<Schema>();
        });
        UNIT_ASSERT_VALUES_EQUAL(f.Database.GetScheme().GetTableInfo(136)->KeyColumns.size(), 3);
        const auto unrestricted = MakeFilter(NKikimrBlobStorage::ROT);
        const auto dc1 = MakeFilter(NKikimrBlobStorage::ROT, TString("dc1"));
        const auto dc2 = MakeFilter(NKikimrBlobStorage::ROT, TString("dc2"));
        const auto emptyDataCenter = MakeFilter(NKikimrBlobStorage::ROT, TString());
        const TFilters all{unrestricted, dc1, dc2, emptyDataCenter};
        f.Update({}, all);
        CheckFilters(f.Load(7, 11), all);
        UNIT_ASSERT_VALUES_EQUAL(f.Count<TLegacyTable>(7, 11), 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Count<Schema::BoxStoragePoolPDiskFilterV2>(7, 11), 3);

        const TFilters located{dc1, dc2};
        f.Update(all, located);
        CheckFilters(f.Load(7, 11), located);
        UNIT_ASSERT_VALUES_EQUAL(f.Count<TLegacyTable>(7, 11), 0);

        const TFilters plain{unrestricted};
        f.Update(f.Load(7, 11), plain);
        CheckFilters(f.Load(7, 11), plain);
        UNIT_ASSERT_VALUES_EQUAL(f.Count<TLegacyTable>(7, 11), 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Count<Schema::BoxStoragePoolPDiskFilterV2>(7, 11), 0);

        f.Update(plain, located);
        CheckFilters(f.Load(7, 11), located);

        // MergeBoxes changes both ids carried in filter keys.
        TFilters moved;
        for (auto filter : located) {
            filter.BoxId = 8;
            filter.StoragePoolId = 12;
            moved.insert(std::move(filter));
        }
        f.Update(located, moved);
        UNIT_ASSERT(f.Load(7, 11).empty());
        CheckFilters(f.Load(8, 12), moved);
        f.Update(moved, {});
        UNIT_ASSERT(f.Load(8, 12).empty());
        UNIT_ASSERT_VALUES_EQUAL(f.Count<TLegacyTable>(7, 11), 0);
        UNIT_ASSERT_VALUES_EQUAL(f.Count<Schema::BoxStoragePoolPDiskFilterV2>(7, 11), 0);
    }

    Y_UNIT_TEST(SerializedFilterKeyDoesNotDependOnPropertyOrder) {
        TFixture f;
        f.Transaction([&] {
            f.Db.Materialize<Schema>();
        });
        NKikimrBlobStorage::TPDiskFilter first;
        first.AddProperty()->SetType(NKikimrBlobStorage::ROT);
        first.AddProperty()->SetKind(9);
        first.AddProperty()->SetSharedWithOs(false);
        first.AddProperty()->MutableLocation()->SetDataCenter("dc1");

        NKikimrBlobStorage::TPDiskFilter reversed;
        for (int i = first.PropertySize(); i > 0; --i) {
            *reversed.AddProperty() = first.GetProperty(i - 1);
        }
        UNIT_ASSERT(first.SerializeAsString() != reversed.SerializeAsString());
        const TFilters a{TFilter::Parse(7, 11, first)};
        const TFilters b{TFilter::Parse(7, 11, reversed)};
        f.Update({}, a);
        f.Update({}, b);
        UNIT_ASSERT_VALUES_EQUAL(f.Count<Schema::BoxStoragePoolPDiskFilterV2>(7, 11), 1);
        CheckFilters(f.Load(7, 11), b);
        f.Update(b, {});
        UNIT_ASSERT_VALUES_EQUAL(f.Count<Schema::BoxStoragePoolPDiskFilterV2>(7, 11), 0);
    }
}

} // namespace NKikimr::NBsController
