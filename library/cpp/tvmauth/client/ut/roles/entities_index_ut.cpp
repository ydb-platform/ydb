#include <library/cpp/tvmauth/client/ut/common.h>

#include <library/cpp/tvmauth/client/misc/roles/entities_index.h>

#include <library/cpp/testing/unittest/registar.h>

#include <array>

using namespace NTvmAuth::NRoles;

Y_UNIT_TEST_SUITE(RolesEntitiesIndex) {
    Y_UNIT_TEST(Stage) {
        TEntitiesIndex::TStage stage({
            "key#1",
            "key#2",
            "key#3",
            "key#4",
        });

        const std::vector<std::vector<TString>> results = {
            {"key#1"},
            {"key#2"},
            {"key#1", "key#2"},
            {"key#3"},
            {"key#1", "key#3"},
            {"key#2", "key#3"},
            {"key#1", "key#2", "key#3"},
            {"key#4"},
            {"key#1", "key#4"},
            {"key#2", "key#4"},
            {"key#1", "key#2", "key#4"},
            {"key#3", "key#4"},
            {"key#1", "key#3", "key#4"},
            {"key#2", "key#3", "key#4"},
            {"key#1", "key#2", "key#3", "key#4"},
        };

        std::vector<TString> keys;
        for (const std::vector<TString>& res : results) {
            UNIT_ASSERT(stage.GetNextKeySet(keys));
            UNIT_ASSERT_VALUES_EQUAL(keys, res);
        }

        UNIT_ASSERT_C(!stage.GetNextKeySet(keys), keys);
    }

    Y_UNIT_TEST(GetUniqueSortedKeys) {
        std::vector<TEntityPtr> entities;

        UNIT_ASSERT_VALUES_EQUAL(std::set<TString>(),
                                 TEntitiesIndex::GetUniqueSortedKeys(entities));

        entities = {
            std::make_shared<TEntity>(),
        };
        UNIT_ASSERT_VALUES_EQUAL(std::set<TString>(),
                                 TEntitiesIndex::GetUniqueSortedKeys(entities));

        entities = {
            std::make_shared<TEntity>(TEntity{
                {"key#1", "value#1"},
            }),
        };
        UNIT_ASSERT_VALUES_EQUAL(std::set<TString>({
                                     "key#1",
                                 }),
                                 TEntitiesIndex::GetUniqueSortedKeys(entities));

        entities = {
            std::make_shared<TEntity>(TEntity{
                {"key#1", "value#1"},
            }),
            std::make_shared<TEntity>(TEntity{
                {"key#1", "value#11"},
                {"key#2", "value#22"},
            }),
        };
        UNIT_ASSERT_VALUES_EQUAL(std::set<TString>({
                                     "key#1",
                                     "key#2",
                                 }),
                                 TEntitiesIndex::GetUniqueSortedKeys(entities));
    }

    Y_UNIT_TEST(MakeUnique) {
        const TEntityPtr entityA = std::make_shared<TEntity>(TEntity{{"key#1", "aaaa"}});
        const TEntityPtr entityA2 = std::make_shared<TEntity>(TEntity{{"key#1", "aaaa"}});
        const TEntityPtr entityB = std::make_shared<TEntity>(TEntity{{"key#1", "bbbb"}});

        TEntitiesIndex::TSubTree idx = {
            std::vector<TEntityPtr>{
                entityA,
                entityA,
            },
            TEntitiesIndex::TIdxByAttrs{
                {
                    TKeyValue{"key#1", "value#11"},
                    TEntitiesIndex::TSubTree{
                        std::vector<TEntityPtr>{
                            entityA,
                            entityB,
                            entityA,
                        },
                        TEntitiesIndex::TIdxByAttrs{
                            {
                                TKeyValue{"key#2", "value#21"},
                                TEntitiesIndex::TSubTree{
                                    std::vector<TEntityPtr>{
                                        entityA,
                                        entityB,
                                        entityA,
                                    },
                                    TEntitiesIndex::TIdxByAttrs{},
                                },
                            },
                        },
                    },
                },
                {
                    TKeyValue{"key#1", "value#12"},
                    TEntitiesIndex::TSubTree{
                        std::vector<TEntityPtr>{
                            entityA,
                            entityB,
                            entityA2,
                        },
                        TEntitiesIndex::TIdxByAttrs{},
                    },
                },
            },
        };

        TEntitiesIndex::MakeUnique(idx);

        UNIT_ASSERT_VALUES_EQUAL(idx.Entities.size(), 1);

        auto it = idx.SubTree.find(TKeyValue{"key#1", "value#12"});
        UNIT_ASSERT(it != idx.SubTree.end());
        UNIT_ASSERT_VALUES_EQUAL(it->second.Entities.size(), 2);

        it = idx.SubTree.find(TKeyValue{"key#1", "value#11"});
        UNIT_ASSERT(it != idx.SubTree.end());
        UNIT_ASSERT_VALUES_EQUAL(it->second.Entities.size(), 2);

        it = it->second.SubTree.find(TKeyValue{"key#2", "value#21"});
        UNIT_ASSERT(it != it->second.SubTree.end());
        UNIT_ASSERT_VALUES_EQUAL(it->second.Entities.size(), 2);
    }

    Y_UNIT_TEST(GetByAttrs) {
        const TEntitiesIndex index = CreateEntitiesIndex();

        UNIT_ASSERT_STRINGS_EQUAL(
            index.PrintDebugString(),
            R"(
"key#1/value#11"
    "key#2/value#22"
        "key#3/value#33"
    "key#2/value#23"
        "key#3/value#33"
    "key#3/value#33"
"key#1/value#13"
    "key#3/value#33"
"key#2/value#22"
    "key#3/value#33"
"key#2/value#23"
    "key#3/value#33"
"key#3/value#33"
)");

        struct TCase {
            TEntity AttrsToFind;
            std::vector<TEntity> Result;
        };

        std::vector<TCase> cases = {
            {
                TEntity{},
                std::vector<TEntity>{
                    TEntity{
                        {"key#1", "value#11"},
                    },
                    TEntity{
                        {"key#1", "value#11"},
                        {"key#2", "value#22"},
                        {"key#3", "value#33"},
                    },
                    TEntity{
                        {"key#1", "value#11"},
                        {"key#2", "value#23"},
                        {"key#3", "value#33"},
                    },
                    TEntity{
                        {"key#1", "value#13"},
                        {"key#3", "value#33"},
                    },
                },
            },
            {
                TEntity{
                    {"key#1", "value#11"},
                },
                std::vector<TEntity>{
                    TEntity{
                        {"key#1", "value#11"},
                    },
                    TEntity{
                        {"key#1", "value#11"},
                        {"key#2", "value#22"},
                        {"key#3", "value#33"},
                    },
                    TEntity{
                        {"key#1", "value#11"},
                        {"key#2", "value#23"},
                        {"key#3", "value#33"},
                    },
                },
            },
            {
                TEntity{
                    {"key#1", "value#13"},
                },
                std::vector<TEntity>{
                    TEntity{
                        {"key#1", "value#13"},
                        {"key#3", "value#33"},
                    },
                },
            },
            {
                TEntity{
                    {"key#1", "value#14"},
                },
                std::vector<TEntity>{},
            },
            {
                TEntity{
                    {"key#2", "value#22"},
                },
                std::vector<TEntity>{
                    TEntity{
                        {"key#1", "value#11"},
                        {"key#2", "value#22"},
                        {"key#3", "value#33"},
                    },
                },
            },
            {
                TEntity{
                    {"key#3", "value#33"},
                },
                std::vector<TEntity>{
                    TEntity{
                        {"key#1", "value#11"},
                        {"key#2", "value#22"},
                        {"key#3", "value#33"},
                    },
                    TEntity{
                        {"key#1", "value#11"},
                        {"key#2", "value#23"},
                        {"key#3", "value#33"},
                    },
                    TEntity{
                        {"key#1", "value#13"},
                        {"key#3", "value#33"},
                    },
                },
            },
        };

        for (const TCase& c : cases) {
            std::vector<TEntityPtr> expected;
            for (const TEntity& e : c.Result) {
                expected.push_back(std::make_shared<TEntity>(e));
            }

            UNIT_ASSERT_VALUES_EQUAL_C(
                index.GetEntitiesWithAttrs(c.AttrsToFind.begin(), c.AttrsToFind.end()),
                expected,
                "'" << c.AttrsToFind << "'");
        }
    }

    Y_UNIT_TEST(Contains) {
        const TEntitiesIndex index = CreateEntitiesIndex();

        struct TCase {
            TEntity Exact;
            bool Result = false;
        };

        std::vector<TCase> cases = {
            {
                TEntity{},
                false,
            },
            {
                TEntity{
                    {"key#1", "value#11"},
                },
                true,
            },
            {
                TEntity{
                    {"key#1", "value#13"},
                },
                false,
            },
            {
                TEntity{
                    {"key#1", "value#13"},
                    {"key#3", "value#33"},
                },
                true,
            },
        };

        for (const TCase& c : cases) {
            UNIT_ASSERT_VALUES_EQUAL_C(
                index.ContainsExactEntity(c.Exact.begin(), c.Exact.end()),
                c.Result,
                "'" << c.Exact << "'");
        }
    }
}

template <>
void Out<std::vector<TString>>(IOutputStream& o, const std::vector<TString>& s) {
    for (const auto& key : s) {
        o << key << ",";
    }
}

template <>
void Out<std::set<TString>>(IOutputStream& o, const std::set<TString>& s) {
    for (const auto& key : s) {
        o << key << ",";
    }
}

template <>
void Out<std::vector<TEntityPtr>>(IOutputStream& o, const std::vector<TEntityPtr>& v) {
    for (const TEntityPtr& p : v) {
        o << *p << Endl;
    }
}

template <>
void Out<TEntityPtr>(IOutputStream& o, const TEntityPtr& v) {
    o << *v;
}

template <>
void Out<TEntity>(IOutputStream& o, const TEntity& v) {
    for (const auto& [key, value] : v) {
        o << key << "->" << value << Endl;
    }
}
