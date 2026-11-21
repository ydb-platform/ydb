#include <ydb/core/kqp/common/simple/query_id.h>

#include <library/cpp/testing/unittest/registar.h>

#include <google/protobuf/text_format.h>
#include <google/protobuf/util/message_differencer.h>

namespace NKikimr::NKqp {

namespace {

using TTypes = TKqpQueryId::TParameterTypes;

Ydb::Type ParseType(const TString& text) {
    Ydb::Type type;
    UNIT_ASSERT_C(google::protobuf::TextFormat::ParseFromString(text, &type), text);
    return type;
}

void AddUnknown(google::protobuf::Message* message, int field = 100500, ui64 value = 1) {
    message->GetReflection()->MutableUnknownFields(message)->AddVarint(field, value);
}

std::shared_ptr<TTypes> Types(std::initializer_list<std::pair<TString, TString>> types) {
    auto result = std::make_shared<TTypes>();
    for (const auto& [name, type] : types) {
        result->emplace(name, ParseType(type));
    }
    return result;
}

std::shared_ptr<TTypes> OneType(const Ydb::Type& type) {
    return std::make_shared<TTypes>(TTypes{{"$a", type}});
}

const TKqpQuerySettings DefaultSettings(NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY);

TKqpQueryId MakeId(const TString& text, std::shared_ptr<TTypes> types, const TString& userSid = "user",
    const TGUCSettings& gUCSettings = {}, const TKqpQuerySettings& settings = DefaultSettings)
{
    return TKqpQueryId("db", "/Root", "id", userSid, text, settings, std::move(types), gUCSettings);
}

void CheckEqual(const TKqpQueryId& a, const TKqpQueryId& b) {
    UNIT_ASSERT(a == b);
    UNIT_ASSERT_VALUES_EQUAL(a.GetHash(), b.GetHash());
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(KqpQueryId) {
    Y_UNIT_TEST(Key) {
        const auto types = Types({{"$a", "type_id: UINT64"}, {"$b", "type_id: UTF8"}});
        const auto id = MakeId("SELECT 1", types);

        UNIT_ASSERT_VALUES_EQUAL(id.Cluster(), "db");
        UNIT_ASSERT_VALUES_EQUAL(id.Database(), "/Root");
        UNIT_ASSERT_VALUES_EQUAL(id.DatabaseId(), "id");
        UNIT_ASSERT_VALUES_EQUAL(id.UserSid(), "user");
        UNIT_ASSERT_VALUES_EQUAL(id.Text(), "SELECT 1");
        UNIT_ASSERT(id.Settings() == DefaultSettings);

        CheckEqual(id, MakeId("SELECT 1", Types({{"$a", "type_id: UINT64"}, {"$b", "type_id: UTF8"}})));
        UNIT_ASSERT(!(id == MakeId("SELECT 2", types)));
        UNIT_ASSERT(!(id == MakeId("SELECT 1", types, "other")));
        UNIT_ASSERT(!(id == MakeId("SELECT 1", nullptr)));
        UNIT_ASSERT(!(MakeId("SELECT 1", nullptr) == MakeId("SELECT 1", Types({}))));
        UNIT_ASSERT(!(id == MakeId("SELECT 1", Types({{"$a", "type_id: UINT64"}, {"$b", "type_id: STRING"}}))));
        UNIT_ASSERT(!(id == MakeId("SELECT 1", Types({{"$a", "type_id: UINT64"}, {"$c", "type_id: UTF8"}}))));

        auto internalCall = DefaultSettings;
        internalCall.IsInternalCall = true;
        UNIT_ASSERT(!(id == MakeId("SELECT 1", types, "user", {}, internalCall)));

        // Field boundaries take part in the comparison, not only the concatenated bytes.
        UNIT_ASSERT(!(TKqpQueryId("ab", "c", "", "", "", DefaultSettings, nullptr, TGUCSettings()) ==
            TKqpQueryId("a", "bc", "", "", "", DefaultSettings, nullptr, TGUCSettings())));

        // Keys of GUC settings are equal exactly when the settings are, whatever order they were set in.
        TVector<TGUCSettings> gucs(5);
        for (int i = 0; i < 8; ++i) {
            gucs[1].Set("name" + std::to_string(i), "value");
            gucs[2].Set("name" + std::to_string(7 - i), "value");
        }
        gucs[3] = gucs[1];
        gucs[3].Set("name0", "other");
        gucs[4].Set("name0", "value", /* isLocal */ true);
        for (const auto& a : gucs) {
            for (const auto& b : gucs) {
                UNIT_ASSERT_VALUES_EQUAL(MakeId("SELECT 1", types, "user", a) == MakeId("SELECT 1", types, "user", b), a == b);
            }
        }

        // Setters rebuild the key, and a key from GUC settings encoded in advance is the same key.
        auto rebuilt = MakeId("SELECT 1", nullptr, "", gucs[1]);
        rebuilt.SetUserSid("user");
        rebuilt.SetQueryParameterTypes(types);
        CheckEqual(rebuilt, MakeId("SELECT 1", types, "user", gucs[1]));
        CheckEqual(rebuilt, TKqpQueryId("db", "/Root", "id", "user", "SELECT 1", DefaultSettings, types,
            TKqpQueryId::EncodeGUCSettings(gucs[1])));

        // Cached GUC bytes follow the settings: equal settings reuse them, a change re-encodes.
        TKqpEncodedGUCSettings encoded;
        auto guc = gucs[1];
        const TString before(encoded.Get(guc));
        guc.Set("name0", "other");
        UNIT_ASSERT_VALUES_EQUAL(encoded.Get(guc), TKqpQueryId::EncodeGUCSettings(gucs[3]));
        guc.Set("name0", "value");
        UNIT_ASSERT_VALUES_EQUAL(encoded.Get(guc), before);
    }

    // Comparing serialized types must agree with comparing the messages.
    Y_UNIT_TEST(Types) {
        TVector<Ydb::Type> types;
        for (const TString text : {
            "type_id: UINT64",
            "decimal_type { precision: 22 scale: 9 }",
            "decimal_type { precision: 22 scale: 8 }",
            "optional_type { }",
            "optional_type { item { } }",
            "optional_type { item { type_id: UINT64 } }",
            "list_type { item { type_id: UINT64 } }",
            "list_type { }",
            "tuple_type { elements { type_id: UINT64 } elements { type_id: UTF8 } }",
            "tuple_type { elements { type_id: UTF8 } elements { type_id: UINT64 } }",
            "struct_type { members { name: 'a' type { type_id: UINT64 } } }",
            "struct_type { members { name: 'b' type { type_id: UINT64 } } }",
            "struct_type { members { name: 'a' } }",
            "dict_type { key { type_id: UTF8 } payload { type_id: UINT64 } }",
            "dict_type { key { type_id: UTF8 } }",
            "variant_type { tuple_items { elements { type_id: UINT64 } } }",
            "variant_type { struct_items { members { name: 'a' type { type_id: UINT64 } } } }",
            "tagged_type { tag: 'x' type { type_id: UINT64 } }",
            "tagged_type { tag: 'y' type { type_id: UINT64 } }",
            "void_type: NULL_VALUE",
            "null_type: NULL_VALUE",
            "empty_list_type: NULL_VALUE",
            "empty_dict_type: NULL_VALUE",
            "pg_type { type_name: 'int8' oid: 20 typlen: 8 }",
            "pg_type { type_name: 'int8' oid: 20 typlen: 8 typmod: -1 }",
            "pg_type { type_name: 'numeric' type_modifier: '10,2' }",
            "pg_type { type_name: 'numeric' type_modifier: '10,3' }",
        }) {
            types.push_back(ParseType(text));
        }

        // Unknown fields: on the type, deep inside it, and with another value.
        for (ui64 value : {1, 2}) {
            AddUnknown(&types.emplace_back(ParseType("type_id: UINT64")), 100500, value);
        }
        const auto nested = ParseType("list_type { item { struct_type { members { name: 'a' type { type_id: UINT64 } } } } }");
        AddUnknown(types.emplace_back(nested).mutable_list_type()->mutable_item()->mutable_struct_type()->mutable_members(0));
        AddUnknown(types.emplace_back(nested).mutable_list_type()->mutable_item()->mutable_struct_type()->mutable_members(0)->mutable_type());
        types.push_back(nested);

        for (const auto& a : types) {
            for (const auto& b : types) {
                const auto idA = MakeId("SELECT 1", OneType(a));
                const auto idB = MakeId("SELECT 1", OneType(b));
                const bool expected = google::protobuf::util::MessageDifferencer::Equals(a, b);
                UNIT_ASSERT_VALUES_EQUAL_C(idA == idB, expected, a.ShortDebugString() << " vs " << b.ShortDebugString());
                if (expected) {
                    UNIT_ASSERT_VALUES_EQUAL(idA.GetHash(), idB.GetHash());
                }
            }
        }

        // The one known disagreement: the same unknown fields in another order are equal as messages
        // and give different keys, since unknown fields are written in the order they arrived.
        Ydb::Type forward = ParseType("type_id: UINT64");
        Ydb::Type reversed = forward;
        AddUnknown(&forward, 100500, 1);
        AddUnknown(&forward, 100501, 2);
        AddUnknown(&reversed, 100501, 2);
        AddUnknown(&reversed, 100500, 1);
        UNIT_ASSERT(google::protobuf::util::MessageDifferencer::Equals(forward, reversed));
        UNIT_ASSERT(!(MakeId("SELECT 1", OneType(forward)) == MakeId("SELECT 1", OneType(reversed))));
    }
}

} // namespace NKikimr::NKqp
