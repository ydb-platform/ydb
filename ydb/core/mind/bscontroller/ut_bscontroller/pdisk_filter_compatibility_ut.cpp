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
    Y_UNIT_TEST(UnknownMetadataKeepsExactKeysThroughEditAndDelete) {
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
        NKikimrBlobStorage::TDefineStoragePool command;
        for (const auto& filter : loaded) {
            UNIT_ASSERT(filter.SerializedFilter);
            UNIT_ASSERT(originals.contains(*filter.SerializedFilter));
            UNIT_ASSERT(filter.MatchPDisk(TPDiskCategory(NPDisk::DEVICE_TYPE_ROT, 0).GetRaw(), {}, {}, {}));
            NKikimrBlobStorage::TPDiskFilter proto;
            TBlobStorageController::Serialize(&proto, filter);
            UNIT_ASSERT_VALUES_EQUAL(proto.GetReflection()->GetUnknownFields(proto).field_count(), 1);
            *command.AddPDiskFilter() = proto;
        }
        const auto edited = TPool::ParsePDiskFilters(7, 11, command.GetPDiskFilter(), &loaded);
        f.Update(loaded, edited);
        UNIT_ASSERT(f.V2Keys() == originals);
        CheckFilters(f.Load(7, 11), loaded);

        f.Update(f.Load(7, 11), {});
        UNIT_ASSERT(f.V2Keys().empty());
    }

    Y_UNIT_TEST(OpaqueOneofKeepsWireOrderAndRejectsAmbiguousEdits) {
        TFixture f;
        f.Transaction([&] { f.Db.Materialize<Schema>(); });
        NKikimrBlobStorage::TPDiskFilter known;
        known.AddProperty()->SetType(NKikimrBlobStorage::SSD);
        // Property 100 = 7 followed by Kind = 1. A future reader selects Kind (last oneof member wins).
        const TString raw = known.SerializeAsString() + TString("\x0a\x05\xa0\x06\x07\x20\x01", 7);
        f.InsertV2(raw);
        auto loaded = f.Load(7, 11);
        NKikimrBlobStorage::TDefineStoragePool command;
        TBlobStorageController::Serialize(command.AddPDiskFilter(), *loaded.begin());
        const TString reordered = command.GetPDiskFilter(0).SerializeAsString();
        UNIT_ASSERT(raw != reordered); // A future reader would select property 100 after this reserialization.
        UNIT_ASSERT_EXCEPTION_CONTAINS(TPool::ParsePDiskFilters(7, 11, command.GetPDiskFilter(), nullptr),
            yexception, "unsupported PDisk filter");
        f.Update(loaded, TPool::ParsePDiskFilters(7, 11, command.GetPDiskFilter(), &loaded));
        UNIT_ASSERT(f.V2Keys() == TSet<TString>{raw});

        f.InsertV2(reordered); // Two distinct meanings now have identical responses to this old reader.
        loaded = f.Load(7, 11);
        UNIT_ASSERT_EXCEPTION_CONTAINS(TPool::ParsePDiskFilters(7, 11, command.GetPDiskFilter(), &loaded),
            yexception, "Ambiguous PDisk filter edit");
        *command.AddPDiskFilter() = command.GetPDiskFilter(0);
        CheckFilters(TPool::ParsePDiskFilters(7, 11, command.GetPDiskFilter(), &loaded), loaded);
        command.ClearPDiskFilter();
        f.Update(loaded, TPool::ParsePDiskFilters(7, 11, command.GetPDiskFilter(), &loaded));
        UNIT_ASSERT(f.V2Keys().empty());
    }

    Y_UNIT_TEST(InvalidStoredFiltersRemainInactiveAndRemovable) {
        TFixture f;
        f.Transaction([&] { f.Db.Materialize<Schema>(); });
        NKikimrBlobStorage::TDefineStoragePool command;
        command.AddPDiskFilter()->AddProperty()->SetKind(1); // No Type.
        auto* duplicate = command.AddPDiskFilter();
        duplicate->AddProperty()->SetType(NKikimrBlobStorage::ROT);
        duplicate->AddProperty()->SetType(NKikimrBlobStorage::SSD);
        for (const auto& proto : command.GetPDiskFilter()) {
            UNIT_ASSERT_EXCEPTION(TFilter::Parse(7, 11, proto), yexception);
            f.InsertV2(proto.SerializeAsString());
        }
        const auto loaded = f.Load(7, 11);
        UNIT_ASSERT_VALUES_EQUAL(loaded.size(), 2);
        for (const auto& filter : loaded) {
            UNIT_ASSERT(filter.IsUnsupported);
            UNIT_ASSERT(!filter.MatchPDisk(TPDiskCategory(NPDisk::DEVICE_TYPE_ROT, 1).GetRaw(), {}, {}, {}));
        }
        CheckFilters(TPool::ParsePDiskFilters(7, 11, command.GetPDiskFilter(), &loaded), loaded);
        f.Update(loaded, {});
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
            UNIT_ASSERT(filter.IsUnsupported);
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
        f.Transaction([&] { f.Db.Materialize<Schema>(); });
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
        CheckFilters(a, b);
        f.Update({}, a);
        f.Update({}, b);
        UNIT_ASSERT_VALUES_EQUAL(f.Count<Schema::BoxStoragePoolPDiskFilterV2>(7, 11), 1);
        const auto loaded = f.Load(7, 11);
        CheckFilters(loaded, b);
        // A redefinition in either order edits the same row instead of writing a second encoding.
        for (const auto* request : {&first, &reversed}) {
            NKikimrBlobStorage::TDefineStoragePool command;
            *command.AddPDiskFilter() = *request;
            CheckFilters(TPool::ParsePDiskFilters(7, 11, command.GetPDiskFilter(), &loaded), loaded);
        }
        f.Update(loaded, {});
        UNIT_ASSERT_VALUES_EQUAL(f.Count<Schema::BoxStoragePoolPDiskFilterV2>(7, 11), 0);
    }

    Y_UNIT_TEST(GeometryOnlyPileOverrideKeepsInactiveStoredFilters) {
        TFixture f;
        f.Transaction([&] { f.Db.Materialize<Schema>(); });
        NKikimrBlobStorage::TPDiskFilter invalid;
        invalid.AddProperty()->SetKind(1); // No Type: Parse rejects it, load keeps it inactive.
        f.InsertV2(invalid.SerializeAsString());

        TPool pool;
        pool.Name = "pool";
        pool.Kind = "kind";
        pool.ErasureSpecies = TBlobStorageGroupType::ErasureNone;
        pool.VDiskKind = NKikimrBlobStorage::TVDiskKind::Default;
        pool.NumGroups = 0;
        pool.RandomizeGroupMapping = false;
        pool.DefaultGroupSizeInUnits = 0;
        f.Transaction([&] { UNIT_ASSERT(pool.LoadPDiskFilters(f.Db, {7, 11})); });
        UNIT_ASSERT_VALUES_EQUAL(pool.PDiskFilters.size(), 1);
        UNIT_ASSERT(pool.PDiskFilters.begin()->IsUnsupported);

        TBridgeInfo bridgeInfo;
        auto& pile = bridgeInfo.Piles.emplace_back();
        pile.BridgePileId = TBridgePileId::FromPileIndex(0);
        pile.Name = "pile";
        TStoragePoolPileOverrideRules rules;
        auto* rule = rules.Add();
        rule->AddPileName("pile");
        rule->AddKind("kind");
        rule->SetErasureSpecies("block-4-2");
        rule->MutableGeometry();

        // The override changes only erasure and geometry, so the stored filters pass through as loaded. Layout
        // calculation takes this path at startup; a throwing reparse here would restart the tablet.
        const auto resolved = TBlobStorageController::ResolveStoragePoolForPile({7, 11}, pool, pile.BridgePileId,
            &bridgeInfo, &rules);
        UNIT_ASSERT(resolved.ErasureSpecies == TBlobStorageGroupType::Erasure4Plus2Block);
        CheckFilters(resolved.PDiskFilters, pool.PDiskFilters);
        UNIT_ASSERT(resolved.PDiskFilters.begin()->IsUnsupported);

        // An override with its own filters replaces the stored set.
        rule->MutablePDiskFilters()->AddPDiskFilter()->AddProperty()->SetType(NKikimrBlobStorage::SSD);
        const auto replaced = TBlobStorageController::ResolveStoragePoolForPile({7, 11}, pool, pile.BridgePileId,
            &bridgeInfo, &rules);
        UNIT_ASSERT_VALUES_EQUAL(replaced.PDiskFilters.size(), 1);
        UNIT_ASSERT(!replaced.PDiskFilters.begin()->IsUnsupported);
        UNIT_ASSERT(replaced.PDiskFilters.begin()->Type == NKikimrBlobStorage::SSD);
    }
}

} // namespace NKikimr::NBsController
