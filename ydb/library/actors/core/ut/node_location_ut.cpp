#include <ydb/library/actors/core/interconnect.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/digest/murmur.h>

#include <cstring>

using namespace NActors;

namespace {
    NActorsInterconnect::TNodeLocation LegacyProto(ui32 room = 12, ui32 rack = 34, ui32 body = 56) {
        NActorsInterconnect::TNodeLocation pb;
        ui32 dc = 0;
        memcpy(&dc, "dc", 2);
        pb.SetDataCenterNum(dc);
        pb.SetRoomNum(room);
        pb.SetRackNum(rack);
        pb.SetBodyNum(body);
        return pb;
    }

    void AssertLegacy(const TNodeLocation::TLegacyValue& value, ui32 dc, ui32 room, ui32 rack, ui32 body) {
        UNIT_ASSERT_VALUES_EQUAL(value.DataCenter, dc);
        UNIT_ASSERT_VALUES_EQUAL(value.Room, room);
        UNIT_ASSERT_VALUES_EQUAL(value.Rack, rack);
        UNIT_ASSERT_VALUES_EQUAL(value.Body, body);
    }
}

Y_UNIT_TEST_SUITE(TNodeLocationContract) {
    Y_UNIT_TEST(ModernRoundTripAndHierarchy) {
        const TNodeLocation location("dc", "12", "34", "56");
        UNIT_ASSERT_VALUES_EQUAL(location.GetDataCenterId(), "dc");
        UNIT_ASSERT_VALUES_EQUAL(location.GetModuleId(), "DC=dc/12");
        UNIT_ASSERT_VALUES_EQUAL(location.GetRackId(), "DC=dc/M=12/34");
        UNIT_ASSERT_VALUES_EQUAL(location.GetUnitId(), "DC=dc/M=12/R=34/56");
        UNIT_ASSERT_VALUES_EQUAL(location.ToString(), "DC=dc/M=12/R=34/U=56/");
        const auto serialized = location.GetSerializedLocation();
        const auto pb = TNodeLocation::ParseLocation(serialized);
        UNIT_ASSERT(!pb.HasDataCenterNum());
        UNIT_ASSERT(!pb.HasBodyNum());
        UNIT_ASSERT(TNodeLocation(pb).GetItems() == location.GetItems());
        UNIT_ASSERT(TNodeLocation(TNodeLocation::FromSerialized, serialized) == location);
        UNIT_ASSERT_VALUES_EQUAL(location.GetItems().size(), 4);
        UNIT_ASSERT(location.HasKey(TNodeLocation::TKeys::Module));
        UNIT_ASSERT(!location.HasKey(TNodeLocation::TKeys::BridgePileName));
        UNIT_ASSERT(!location.GetBridgePileName());
    }

    Y_UNIT_TEST(LegacyConversionAndInputImmutability) {
        const auto pb = LegacyProto();
        const auto before = pb.SerializeAsString();
        const TNodeLocation location(pb);
        UNIT_ASSERT_VALUES_EQUAL(pb.SerializeAsString(), before);
        UNIT_ASSERT(location.GetItems() == TNodeLocation("dc", "12", "34", "56").GetItems());
        AssertLegacy(location.GetLegacyValue(), pb.GetDataCenterNum(), 12, 34, 56);
        NActorsInterconnect::TNodeLocation output;
        location.Serialize(&output, true);
        UNIT_ASSERT_VALUES_EQUAL(output.GetDataCenterNum(), pb.GetDataCenterNum());
        UNIT_ASSERT_VALUES_EQUAL(output.GetRoomNum(), 12);
        UNIT_ASSERT_VALUES_EQUAL(output.GetRackNum(), 34);
        UNIT_ASSERT_VALUES_EQUAL(output.GetBodyNum(), 56);
        UNIT_ASSERT(TNodeLocation(output) == location);
        const auto modern = TNodeLocation::ParseLocation(location.GetSerializedLocation());
        UNIT_ASSERT(!modern.HasRoomNum());
        UNIT_ASSERT(TNodeLocation(modern).GetItems() == location.GetItems());
    }

    Y_UNIT_TEST(PartialLegacyUsesZeroDefaults) {
        NActorsInterconnect::TNodeLocation pb;
        pb.SetRoomNum(7);
        const TNodeLocation location(pb);
        AssertLegacy(location.GetLegacyValue(), 0, 7, 0, 0);
        UNIT_ASSERT(location.HasKey(TNodeLocation::TKeys::DataCenter));
        UNIT_ASSERT_VALUES_EQUAL(location.GetItems()[1].second, "7");
        UNIT_ASSERT_VALUES_EQUAL(location.GetItems()[2].second, "0");
        UNIT_ASSERT_VALUES_EQUAL(location.GetItems()[3].second, "0");
        UNIT_ASSERT_VALUES_EQUAL(location.GetItems().size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(location.GetDataCenterId(), "");
        UNIT_ASSERT_VALUES_EQUAL(location.GetUnitId(), "DC=/M=7/R=0/0");
    }

    Y_UNIT_TEST(MixedFormatKeepsModernItemsAndLegacyValue) {
        auto pb = LegacyProto();
        pb.SetRack("modern-rack");
        const auto before = pb.SerializeAsString();
        const TNodeLocation location(pb);
        UNIT_ASSERT_VALUES_EQUAL(pb.SerializeAsString(), before);
        UNIT_ASSERT(location.GetItems() == TNodeLocation("", "", "modern-rack").GetItems());
        AssertLegacy(location.GetLegacyValue(), pb.GetDataCenterNum(), 12, 34, 56);
        NActorsInterconnect::TNodeLocation modern;
        location.Serialize(&modern, false);
        UNIT_ASSERT_VALUES_EQUAL(modern.GetRack(), "modern-rack");
        UNIT_ASSERT(!modern.HasDataCenter());
        UNIT_ASSERT(!modern.HasRackNum());
        NActorsInterconnect::TNodeLocation compatible;
        location.Serialize(&compatible, true);
        UNIT_ASSERT_VALUES_EQUAL(compatible.GetRackNum(), 34);
        UNIT_ASSERT_VALUES_EQUAL(compatible.GetRack(), "modern-rack");
    }

    Y_UNIT_TEST(BodyOverridesUnitWithoutMutatingInput) {
        for (bool legacy : {false, true}) {
            auto pb = legacy ? LegacyProto() : NActorsInterconnect::TNodeLocation();
            pb.SetUnit("99");
            pb.SetBody(42);
            const auto before = pb.SerializeAsString();
            const TNodeLocation location(pb);
            UNIT_ASSERT_VALUES_EQUAL(pb.SerializeAsString(), before);
            UNIT_ASSERT(location.GetItems() == TNodeLocation("", "", "", "42").GetItems());
            const auto output = TNodeLocation::ParseLocation(location.GetSerializedLocation());
            UNIT_ASSERT(!output.HasBody());
            UNIT_ASSERT_VALUES_EQUAL(output.GetUnit(), "42");
            UNIT_ASSERT_VALUES_EQUAL(location.GetLegacyValue().Body, legacy ? 56 : 42);
        }
    }

    Y_UNIT_TEST(UnknownStringsAreSortedAndRoundTrip) {
        NActorsInterconnect::TNodeLocation pb;
        pb.SetRack("rack");
        pb.SetBridgePileName("pile");
        pb.SetDataCenter("dc");
        pb.mutable_unknown_fields()->AddLengthDelimited(60, "last");
        pb.mutable_unknown_fields()->AddLengthDelimited(50, "z");
        pb.mutable_unknown_fields()->AddLengthDelimited(50, "a");
        const auto before = pb.SerializeAsString();
        const TNodeLocation location(pb);
        UNIT_ASSERT_VALUES_EQUAL(pb.SerializeAsString(), before);
        const auto& items = location.GetItems();
        UNIT_ASSERT_VALUES_EQUAL(items.size(), 6);
        const int keys[] = {5, 10, 30, 50, 50, 60};
        const TString values[] = {"pile", "dc", "rack", "a", "z", "last"};
        for (size_t i = 0; i < items.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(int(items[i].first), keys[i]);
            UNIT_ASSERT_VALUES_EQUAL(items[i].second, values[i]);
        }
        UNIT_ASSERT_VALUES_EQUAL(*location.GetBridgePileName(), "pile");
        UNIT_ASSERT(location.HasKey(TNodeLocation::TKeys::E(50)));
        UNIT_ASSERT(!location.HasKey(TNodeLocation::TKeys::E(55)));
        UNIT_ASSERT_VALUES_EQUAL(location.ToStringUpTo(TNodeLocation::TKeys::E(50)), "P=pile/DC=dc/R=rack/az");
        UNIT_ASSERT_VALUES_EQUAL(location.ToString(), "P=pile/DC=dc/R=rack/50=a/50=z/60=last/");
        const auto output = TNodeLocation::ParseLocation(location.GetSerializedLocation());
        UNIT_ASSERT_VALUES_EQUAL(output.unknown_fields().field_count(), 3);
        UNIT_ASSERT(TNodeLocation(output).GetItems() == items);
        UNIT_ASSERT(TNodeLocation(TNodeLocation::FromSerialized, location.GetSerializedLocation()).GetItems() == items);
    }

    Y_UNIT_TEST(NumericAndNamedLegacyIds) {
        ui32 dc = 0;
        memcpy(&dc, "abcd", 4);
        const TNodeLocation numeric("abcdef", "12", "34", "56");
        AssertLegacy(numeric.GetLegacyValue(), dc, 12, 34, 56);
        const TNodeLocation named("abcd", "module", "rack", "56");
        AssertLegacy(named.GetLegacyValue(), dc,
            MurmurHash<ui32>("module", 6), MurmurHash<ui32>("rack", 4), 56);
        NActorsInterconnect::TNodeLocation pb;
        numeric.Serialize(&pb, true);
        UNIT_ASSERT(pb.HasDataCenterNum() && pb.HasRoomNum() && pb.HasRackNum() && pb.HasBodyNum());
        AssertLegacy(TNodeLocation(pb).GetLegacyValue(), dc, 12, 34, 56);
        UNIT_ASSERT_VALUES_EQUAL(pb.GetDataCenter(), "abcdef");
        AssertLegacy(TNodeLocation().GetLegacyValue(), 0, 0, 0, 0);
    }

    Y_UNIT_TEST(BridgePileDoesNotAffectLegacyIds) {
        NActorsInterconnect::TNodeLocation pb;
        pb.SetBridgePileName("pile");
        const TNodeLocation pileOnly(pb);
        AssertLegacy(pileOnly.GetLegacyValue(), 0, 0, 0, 0);
        UNIT_ASSERT_VALUES_EQUAL(pileOnly.GetDataCenterId(), "P=pile/");
        pb = LegacyProto();
        pb.SetBridgePileName("pile");
        const TNodeLocation mixed(pb);
        UNIT_ASSERT_VALUES_EQUAL(mixed.GetItems().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(*mixed.GetBridgePileName(), "pile");
        AssertLegacy(mixed.GetLegacyValue(), pb.GetDataCenterNum(), 12, 34, 56);
    }

    Y_UNIT_TEST(ComparisonsAndInheritedLegacyValue) {
        const TNodeLocation low("dc", "12", "34", "55");
        const TNodeLocation high("dc", "12", "34", "56");
        UNIT_ASSERT(low < high && low <= high && low != high);
        UNIT_ASSERT(high > low && high >= low);
        UNIT_ASSERT(high == high && high <= high && high >= high);
        const TNodeLocation legacy(LegacyProto());
        UNIT_ASSERT(legacy == high && high == legacy);
        UNIT_ASSERT(low < legacy && legacy > low);
        UNIT_ASSERT(TNodeLocation(LegacyProto(12, 34, 57)) > legacy);
        TNodeLocation inherited("other", "module", "rack", "not-numeric");
        const auto items = inherited.GetItems();
        inherited.InheritLegacyValue(legacy);
        UNIT_ASSERT(inherited.GetItems() == items);
        UNIT_ASSERT(inherited == legacy && inherited == high);
        AssertLegacy(inherited.GetLegacyValue(), LegacyProto().GetDataCenterNum(), 12, 34, 56);
        NActorsInterconnect::TNodeLocation pb;
        inherited.Serialize(&pb, true);
        UNIT_ASSERT_VALUES_EQUAL(pb.GetUnit(), "not-numeric");
        UNIT_ASSERT_VALUES_EQUAL(pb.GetBodyNum(), 56);
        inherited.InheritLegacyValue(low);
        UNIT_ASSERT(inherited == low && inherited < legacy);
        UNIT_ASSERT(inherited.GetItems() == items);
    }

    Y_UNIT_TEST(HierarchyWithMissingFields) {
        const TNodeLocation empty;
        UNIT_ASSERT(empty.GetItems().empty());
        UNIT_ASSERT_VALUES_EQUAL(empty.GetDataCenterId(), "");
        UNIT_ASSERT_VALUES_EQUAL(empty.GetModuleId(), "");
        UNIT_ASSERT_VALUES_EQUAL(empty.GetRackId(), "");
        UNIT_ASSERT_VALUES_EQUAL(empty.GetUnitId(), "");
        UNIT_ASSERT(!empty.HasKey(TNodeLocation::TKeys::DataCenter));
        UNIT_ASSERT(!empty.GetBridgePileName());
        const TNodeLocation sparse("dc", "", "rack", "");
        UNIT_ASSERT_VALUES_EQUAL(sparse.GetModuleId(), "DC=dc/");
        UNIT_ASSERT_VALUES_EQUAL(sparse.GetRackId(), "DC=dc/rack");
        UNIT_ASSERT_VALUES_EQUAL(sparse.GetUnitId(), "DC=dc/R=rack/");
        const TNodeLocation unitOnly("", "", "", "7");
        UNIT_ASSERT_VALUES_EQUAL(unitOnly.GetRackId(), "");
        UNIT_ASSERT_VALUES_EQUAL(unitOnly.GetUnitId(), "7");
        NActorsInterconnect::TNodeLocation pb;
        pb.SetModule("");
        const TNodeLocation explicitEmpty(pb);
        UNIT_ASSERT(explicitEmpty.HasKey(TNodeLocation::TKeys::Module));
        UNIT_ASSERT(explicitEmpty != empty);
        UNIT_ASSERT_VALUES_EQUAL(explicitEmpty.GetModuleId(), "");
        UNIT_ASSERT_VALUES_EQUAL(explicitEmpty.ToString(), "M=/");
    }
}
