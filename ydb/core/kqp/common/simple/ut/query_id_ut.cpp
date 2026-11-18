#include <ydb/core/kqp/common/simple/query_id.h>

#include <library/cpp/testing/unittest/registar.h>

#include <google/protobuf/util/message_differencer.h>

namespace NKikimr::NKqp {

namespace {

using TTypes = TKqpQueryId::TParameterTypes;

Ydb::Type Primitive(Ydb::Type::PrimitiveTypeId id) {
    Ydb::Type type;
    type.set_type_id(id);
    return type;
}

std::shared_ptr<TTypes> Types(std::initializer_list<TTypes::value_type> types) {
    return std::make_shared<TTypes>(types);
}

TKqpQueryId MakeId(const TString& text, std::shared_ptr<TTypes> types, const TString& userSid = "user",
    const TGUCSettings& gUCSettings = {})
{
    return TKqpQueryId("db", "/Root", "id", userSid, text, TKqpQuerySettings(NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY),
        std::move(types), gUCSettings);
}

void CheckEqual(const TKqpQueryId& a, const TKqpQueryId& b) {
    UNIT_ASSERT(a == b);
    UNIT_ASSERT_VALUES_EQUAL(a.GetHash(), b.GetHash());
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(KqpQueryId) {
    Y_UNIT_TEST(Equality) {
        const auto types = Types({{"$a", Primitive(Ydb::Type::UINT64)}, {"$b", Primitive(Ydb::Type::UTF8)}});
        const auto id = MakeId("SELECT 1", types);

        // Fields are read back from the key buffer.
        UNIT_ASSERT_VALUES_EQUAL(id.Cluster(), "db");
        UNIT_ASSERT_VALUES_EQUAL(id.Database(), "/Root");
        UNIT_ASSERT_VALUES_EQUAL(id.DatabaseId(), "id");
        UNIT_ASSERT_VALUES_EQUAL(id.UserSid(), "user");
        UNIT_ASSERT_VALUES_EQUAL(id.Text(), "SELECT 1");

        // An independently built key with equal contents.
        CheckEqual(id, MakeId("SELECT 1", Types({{"$b", Primitive(Ydb::Type::UTF8)}, {"$a", Primitive(Ydb::Type::UINT64)}})));

        UNIT_ASSERT(!(id == MakeId("SELECT 2", types)));
        UNIT_ASSERT(!(id == MakeId("SELECT 1", types, "other")));
        UNIT_ASSERT(!(id == MakeId("SELECT 1", nullptr)));
        UNIT_ASSERT(!(MakeId("SELECT 1", nullptr) == MakeId("SELECT 1", Types({}))));
        UNIT_ASSERT(!(id == MakeId("SELECT 1", Types({{"$a", Primitive(Ydb::Type::UINT64)}, {"$b", Primitive(Ydb::Type::STRING)}}))));
        UNIT_ASSERT(!(id == MakeId("SELECT 1", Types({{"$a", Primitive(Ydb::Type::UINT64)}, {"$c", Primitive(Ydb::Type::UTF8)}}))));

        // Field boundaries take part in the comparison, not only the concatenated bytes.
        const TGUCSettings guc;
        const TKqpQuerySettings settings(NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY);
        UNIT_ASSERT(!(TKqpQueryId("ab", "c", "", "", "", settings, nullptr, guc) == TKqpQueryId("a", "bc", "", "", "", settings, nullptr, guc)));

        // A set but empty nested message differs from an absent one.
        Ydb::Type absentItem;
        absentItem.mutable_optional_type();
        Ydb::Type emptyItem = absentItem;
        emptyItem.mutable_optional_type()->mutable_item();
        UNIT_ASSERT(!(MakeId("SELECT 1", Types({{"$a", absentItem}})) == MakeId("SELECT 1", Types({{"$a", emptyItem}}))));

        TGUCSettings changedGuc;
        changedGuc.Set("search_path", "public");
        UNIT_ASSERT(!(id == MakeId("SELECT 1", types, "user", changedGuc)));
        CheckEqual(MakeId("SELECT 1", types, "user", changedGuc), MakeId("SELECT 1", types, "user", changedGuc));
    }

    Y_UNIT_TEST(UnknownFields) {
        auto withUnknown = [](ui64 value) {
            Ydb::Type type = Primitive(Ydb::Type::UINT64);
            type.GetReflection()->MutableUnknownFields(&type)->AddVarint(100500, value);
            return MakeId("SELECT 1", Types({{"$a", type}}));
        };

        CheckEqual(withUnknown(1), withUnknown(1));
        UNIT_ASSERT(!(withUnknown(1) == withUnknown(2)));
        UNIT_ASSERT(!(withUnknown(1) == MakeId("SELECT 1", Types({{"$a", Primitive(Ydb::Type::UINT64)}}))));

        // The same two unknown fields in another order are equal as messages and give different keys:
        // types are compared by serialized bytes, and unknown fields are written in the order they arrived.
        auto withTwoUnknown = [](bool reversed) {
            Ydb::Type type = Primitive(Ydb::Type::UINT64);
            auto* unknown = type.GetReflection()->MutableUnknownFields(&type);
            unknown->AddVarint(reversed ? 100501 : 100500, reversed ? 2 : 1);
            unknown->AddVarint(reversed ? 100500 : 100501, reversed ? 1 : 2);
            return type;
        };
        UNIT_ASSERT(google::protobuf::util::MessageDifferencer::Equals(withTwoUnknown(false), withTwoUnknown(true)));
        UNIT_ASSERT(!(MakeId("SELECT 1", Types({{"$a", withTwoUnknown(false)}})) == MakeId("SELECT 1", Types({{"$a", withTwoUnknown(true)}}))));
        CheckEqual(MakeId("SELECT 1", Types({{"$a", withTwoUnknown(true)}})), MakeId("SELECT 1", Types({{"$a", withTwoUnknown(true)}})));
    }

    Y_UNIT_TEST(MatchesMessageComparison) {
        auto addUnknown = [](google::protobuf::Message* message) {
            message->GetReflection()->MutableUnknownFields(message)->AddVarint(100500, 1);
        };

        TVector<Ydb::Type> types;
        auto add = [&](auto fill) {
            fill(types.emplace_back());
        };
        add([](Ydb::Type& t) { t.set_type_id(Ydb::Type::UINT64); });
        add([](Ydb::Type& t) { t.mutable_decimal_type()->set_precision(22); t.mutable_decimal_type()->set_scale(9); });
        add([](Ydb::Type& t) { t.mutable_decimal_type()->set_precision(22); t.mutable_decimal_type()->set_scale(8); });
        add([](Ydb::Type& t) { t.mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UINT64); });
        add([](Ydb::Type& t) { t.mutable_list_type()->mutable_item()->set_type_id(Ydb::Type::UINT64); });
        add([](Ydb::Type& t) { t.mutable_list_type(); });
        add([](Ydb::Type& t) {
            t.mutable_tuple_type()->add_elements()->set_type_id(Ydb::Type::UINT64);
            t.mutable_tuple_type()->add_elements()->set_type_id(Ydb::Type::UTF8);
        });
        add([](Ydb::Type& t) {
            t.mutable_tuple_type()->add_elements()->set_type_id(Ydb::Type::UTF8);
            t.mutable_tuple_type()->add_elements()->set_type_id(Ydb::Type::UINT64);
        });
        for (TString name : {"a", "b"}) {
            add([&](Ydb::Type& t) {
                auto* member = t.mutable_struct_type()->add_members();
                member->set_name(name);
                member->mutable_type()->set_type_id(Ydb::Type::UINT64);
            });
        }
        add([](Ydb::Type& t) { t.mutable_struct_type()->add_members()->set_name("a"); });
        add([](Ydb::Type& t) {
            t.mutable_dict_type()->mutable_key()->set_type_id(Ydb::Type::UTF8);
            t.mutable_dict_type()->mutable_payload()->set_type_id(Ydb::Type::UINT64);
        });
        add([](Ydb::Type& t) { t.mutable_dict_type()->mutable_key()->set_type_id(Ydb::Type::UTF8); });
        add([](Ydb::Type& t) { t.mutable_variant_type()->mutable_tuple_items()->add_elements()->set_type_id(Ydb::Type::UINT64); });
        add([](Ydb::Type& t) {
            auto* member = t.mutable_variant_type()->mutable_struct_items()->add_members();
            member->set_name("a");
            member->mutable_type()->set_type_id(Ydb::Type::UINT64);
        });
        for (TString tag : {"x", "y"}) {
            add([&](Ydb::Type& t) {
                t.mutable_tagged_type()->set_tag(tag);
                t.mutable_tagged_type()->mutable_type()->set_type_id(Ydb::Type::UINT64);
            });
        }
        add([](Ydb::Type& t) { t.set_void_type(google::protobuf::NULL_VALUE); });
        add([](Ydb::Type& t) { t.set_null_type(google::protobuf::NULL_VALUE); });
        add([](Ydb::Type& t) { t.set_empty_list_type(google::protobuf::NULL_VALUE); });
        add([](Ydb::Type& t) { t.set_empty_dict_type(google::protobuf::NULL_VALUE); });
        add([](Ydb::Type& t) { t.mutable_pg_type()->set_type_name("int8"); t.mutable_pg_type()->set_oid(20); t.mutable_pg_type()->set_typlen(8); });
        add([](Ydb::Type& t) { t.mutable_pg_type()->set_type_name("int8"); t.mutable_pg_type()->set_oid(20); t.mutable_pg_type()->set_typlen(8); t.mutable_pg_type()->set_typmod(-1); });
        add([](Ydb::Type& t) { t.mutable_pg_type()->set_type_name("numeric"); t.mutable_pg_type()->set_type_modifier("10,2"); });
        add([](Ydb::Type& t) { t.mutable_pg_type()->set_type_name("numeric"); t.mutable_pg_type()->set_type_modifier("10,3"); });

        // Unknown fields deep inside a type: List<Struct<a: Uint64>> with the field on the member and on its type.
        Ydb::Type nested;
        auto* nestedMember = nested.mutable_list_type()->mutable_item()->mutable_struct_type()->add_members();
        nestedMember->set_name("a");
        nestedMember->mutable_type()->set_type_id(Ydb::Type::UINT64);
        types.push_back(nested);
        types.push_back(nested);
        addUnknown(types.back().mutable_list_type()->mutable_item()->mutable_struct_type()->mutable_members(0));
        types.push_back(nested);
        addUnknown(types.back().mutable_list_type()->mutable_item()->mutable_struct_type()->mutable_members(0)->mutable_type());

        for (const auto& a : types) {
            for (const auto& b : types) {
                const auto idA = MakeId("SELECT 1", Types({{"$a", a}}));
                const auto idB = MakeId("SELECT 1", Types({{"$a", b}}));
                const bool expected = google::protobuf::util::MessageDifferencer::Equals(a, b);
                UNIT_ASSERT_VALUES_EQUAL_C(idA == idB, expected, a.ShortDebugString() << " vs " << b.ShortDebugString());
                if (expected) {
                    UNIT_ASSERT_VALUES_EQUAL(idA.GetHash(), idB.GetHash());
                }
            }
        }
    }

    Y_UNIT_TEST(Rebuild) {
        const auto types = Types({{"$a", Primitive(Ydb::Type::UINT64)}});
        TGUCSettings guc;
        guc.Set("search_path", "public");

        auto id = MakeId("SELECT 1", nullptr, "", guc);
        id.SetUserSid("user");
        CheckEqual(id, MakeId("SELECT 1", nullptr, "user", guc));

        id.SetQueryParameterTypes(types);
        CheckEqual(id, MakeId("SELECT 1", types, "user", guc));

        // Cached GUC bytes follow the settings: equal settings reuse them, a change re-encodes.
        TKqpEncodedGUCSettings encoded;
        const TString before(encoded.Get(guc));
        UNIT_ASSERT_VALUES_EQUAL(encoded.Get(guc), before);
        guc.Set("search_path", "other");
        UNIT_ASSERT_VALUES_UNEQUAL(encoded.Get(guc), before);
        UNIT_ASSERT_VALUES_EQUAL(encoded.Get(guc), TKqpQueryId::EncodeGUCSettings(guc));
        guc.Set("search_path", "public");
        UNIT_ASSERT_VALUES_EQUAL(encoded.Get(guc), before);

        // A key built from GUC settings encoded in advance is the same key.
        CheckEqual(id, TKqpQueryId("db", "/Root", "id", "user", "SELECT 1", TKqpQuerySettings(NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY),
            types, TKqpQueryId::EncodeGUCSettings(guc)));
    }
}

} // namespace NKikimr::NKqp
