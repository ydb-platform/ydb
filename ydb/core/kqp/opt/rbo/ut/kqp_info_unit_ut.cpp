#include <ydb/core/kqp/opt/rbo/kqp_info_unit.h>

#include <library/cpp/testing/unittest/registar.h>

#include <algorithm>
#include <array>
#include <random>
#include <set>

namespace NKikimr::NKqp {
namespace {

using TReference = std::set<TInfoUnitId>;

void CheckSet(const TUnorderedIUs& actual, const TReference& expected) {
    UNIT_ASSERT_VALUES_EQUAL(actual.Size(), expected.size());
    UNIT_ASSERT_VALUES_EQUAL(actual.Empty(), expected.empty());
    UNIT_ASSERT(std::ranges::equal(actual, expected));
    for (const auto id : expected) {
        UNIT_ASSERT(actual.Contains(id));
    }
    UNIT_ASSERT(!actual.Contains(TUnorderedIUs::InvalidBit));
}

void CheckRelations(const TUnorderedIUs& left, const TReference& a, const TUnorderedIUs& right, const TReference& b) {
    const bool overlap = std::ranges::any_of(a, [&](const auto id) { return b.contains(id); });
    UNIT_ASSERT_VALUES_EQUAL(left == right, a == b);
    UNIT_ASSERT_VALUES_EQUAL(left.IsSubsetOf(right), std::ranges::includes(b, a));
    UNIT_ASSERT_VALUES_EQUAL(right.IsSubsetOf(left), std::ranges::includes(a, b));
    UNIT_ASSERT_VALUES_EQUAL(left.HasAny(right), overlap);
    UNIT_ASSERT_VALUES_EQUAL(right.HasAny(left), overlap);
}

static_assert(TMappedIUs<TUnionInputRow>::MutableValues);
static_assert(!TUnionAllIUs::MutableValues);
static_assert(std::is_same_v<decltype(std::declval<TUnionAllIUs&>().At(0)), const TUnionInputRow&>);

struct TDependencies {
    auto operator()(const std::pair<TInfoUnitId, TInfoUnitId>& pair) const {
        return std::array{pair.first, pair.second};
    }
};

} // namespace

Y_UNIT_TEST_SUITE(KqpInfoUnitCollections) {
    Y_UNIT_TEST(EveryRepresentationPairSupportsSetAlgebra) {
        static_assert(sizeof(TUnorderedIUs) == 32);
        const TVector<TVector<TInfoUnitId>> examples{
            {}, {0, 63, 64, 191}, {6400, 6527}, {1, 1024, 32769},
            {0, 64, 128, 192, 256, 320}, {0, 1024, 65536, 1000000},
            {0, (1u << 24) - 1, (1u << 24)}, {TUnorderedIUs::InvalidBit - 2, TUnorderedIUs::InvalidBit - 1}
        };
        using EKind = TUnorderedIUs::EStorageKind;
        std::set<EKind> kinds;
        for (const auto& leftIds : examples) {
            TUnorderedIUs left;
            left.Assign(leftIds);
            kinds.insert(left.StorageKind());
            const TReference a(leftIds.begin(), leftIds.end());
            CheckSet(left, a);
            for (const auto& rightIds : examples) {
                TUnorderedIUs right;
                right.Assign(rightIds);
                const TReference b(rightIds.begin(), rightIds.end());
                TReference joined = a, intersection, difference;
                joined.insert(b.begin(), b.end());
                std::set_intersection(a.begin(), a.end(), b.begin(), b.end(), std::inserter(intersection, intersection.end()));
                std::set_difference(a.begin(), a.end(), b.begin(), b.end(), std::inserter(difference, difference.end()));
                auto value = left;
                UNIT_ASSERT_VALUES_EQUAL(value.UnionWith(right), joined != a);
                CheckSet(value, joined);
                value = left;
                UNIT_ASSERT_VALUES_EQUAL(value.IntersectWith(right), intersection != a);
                CheckSet(value, intersection);
                value = left;
                UNIT_ASSERT_VALUES_EQUAL(value.Subtract(right), difference != a);
                CheckSet(value, difference);
                UNIT_ASSERT_VALUES_EQUAL(left.HasAny(right), !intersection.empty());
                UNIT_ASSERT_VALUES_EQUAL(left.IsSubsetOf(right), intersection == a);
                UNIT_ASSERT_VALUES_EQUAL(left == right, a == b);
                CheckSet(left, a);
                CheckSet(right, b);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(kinds.size(), 4);
    }

    Y_UNIT_TEST(RandomMutationsAndRepresentationTransitions) {
        std::mt19937 random(5719);
        std::array<TUnorderedIUs, 4> sets;
        std::array<TReference, 4> refs;
        // Phases alternate between one compact window, where heap sets stay
        // dense, and scattered IDs, where they stay sparse.
        bool clustered = false;
        std::set<TUnorderedIUs::EStorageKind> kinds;
        auto nextId = [&]() -> TInfoUnitId {
            if (clustered) {
                return 6400 + random() % 1024;
            }
            switch (random() % 5) {
                case 0: return random() % 256;
                case 1: return 6400 + random() % 1024;
                case 2: {
                    const TInfoUnitId base = (random() % 5) * 4096;
                    return base + random() % 64;
                }
                case 3: return random() % (1u << 30);
                default: return TUnorderedIUs::InvalidBit - 1 - random() % 256;
            }
        };
        for (size_t step = 0; step < 20000; ++step) {
            clustered = step / 1000 % 2;
            const auto index = random() % sets.size(), other = random() % sets.size();
            auto& value = sets[index];
            auto& ref = refs[index];
            const auto id = nextId();
            switch (random() % 10) {
                case 0: case 1:
                    UNIT_ASSERT_VALUES_EQUAL(value.Add(id), ref.insert(id).second);
                    break;
                case 2: {
                    // Mostly remove members, so that heap words are cleared in place.
                    auto removed = id;
                    if (!ref.empty() && random() % 4) {
                        removed = *std::next(ref.begin(), random() % ref.size());
                    }
                    UNIT_ASSERT_VALUES_EQUAL(value.Remove(removed), ref.erase(removed) != 0);
                    break;
                }
                case 3: {
                    const auto before = ref;
                    ref.insert(refs[other].begin(), refs[other].end());
                    UNIT_ASSERT_VALUES_EQUAL(value.UnionWith(sets[other]), before != ref);
                    break;
                }
                case 4: case 5: {
                    const bool subtract = random() % 2;
                    auto result = ref;
                    for (auto it = result.begin(); it != result.end();) {
                        if (refs[other].contains(*it) == subtract) {
                            it = result.erase(it);
                        } else {
                            ++it;
                        }
                    }
                    const bool changed = subtract ? value.Subtract(sets[other]) : value.IntersectWith(sets[other]);
                    UNIT_ASSERT_VALUES_EQUAL(changed, result != ref);
                    ref = std::move(result);
                    break;
                }
                case 6: {
                    TVector<TInfoUnitId> ids;
                    const size_t count = random() % 100;
                    for (size_t i = 0; i < count; ++i) {
                        ids.push_back(nextId());
                    }
                    value.Assign(ids);
                    ref = TReference(ids.begin(), ids.end());
                    break;
                }
                case 7:
                    value.Assign(value);
                    break;
                case 8: {
                    // Equal copies diverge later, so relations see both outcomes.
                    auto copy = sets[other];
                    TUnorderedIUs moved(std::move(copy));
                    UNIT_ASSERT(copy.Empty());
                    value = std::move(moved);
                    UNIT_ASSERT(moved.Empty());
                    ref = refs[other];
                    break;
                }
                default:
                    value.Clear();
                    ref.clear();
            }
            kinds.insert(value.StorageKind());
            for (size_t i = 0; i < sets.size(); ++i) {
                CheckSet(sets[i], refs[i]);
                CheckRelations(value, ref, sets[i], refs[i]);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(kinds.size(), 4);
        TUnorderedIUs boundaries{0, 63, 64, 191, 192, (1u << 24), TUnorderedIUs::InvalidBit - 1};
        for (const auto id : TVector<TInfoUnitId>(boundaries.begin(), boundaries.end())) {
            UNIT_ASSERT(boundaries.Remove(id));
            UNIT_ASSERT(!boundaries.Remove(id));
        }
        UNIT_ASSERT(boundaries.Empty());
        UNIT_ASSERT_EXCEPTION(boundaries.Add(TUnorderedIUs::InvalidBit), yexception);
    }

    Y_UNIT_TEST(OrderedMembershipRetainsRepeatedPositionsAndMetadata) {
        TOrderedIUs<TString> values{{7, "first"}, {2, "second"}, {7, "again"}};
        UNIT_ASSERT(values.Unordered() == (TUnorderedIUs{2, 7}));
        values.EraseAt(0);
        UNIT_ASSERT(values.Unordered() == (TUnorderedIUs{2, 7}));
        values.ReplaceAt(1, 9, "changed");
        UNIT_ASSERT(values.Unordered() == (TUnorderedIUs{2, 9}));
        values.InsertAt(0, 9, "leading");
        UNIT_ASSERT_VALUES_EQUAL(values.Items()[0].second, "leading");
        UNIT_ASSERT_VALUES_EQUAL(values.Items()[2].second, "changed");
        auto copy = values;
        values.Clear();
        UNIT_ASSERT(values.Unordered().Empty());
        UNIT_ASSERT(copy.Unordered() == (TUnorderedIUs{2, 9}));
        TOrderedIUs<TString> moved(std::move(copy));
        UNIT_ASSERT(copy.Unordered().Empty());
        UNIT_ASSERT_VALUES_EQUAL(moved.Items().size(), 3);
    }

    Y_UNIT_TEST(MappedValuesInvalidateDependencyUnionWithoutLosingSharedInputs) {
        TMappedIUs<std::pair<TInfoUnitId, TInfoUnitId>, TDependencies> values;
        values.Add(0, std::pair{7u, 8u});
        values.Add(1, std::pair{8u, 9u});
        UNIT_ASSERT(values.Keys() == (TUnorderedIUs{0, 1}));
        UNIT_ASSERT(values.MappedIUs() == (TUnorderedIUs{7, 8, 9}));
        values.Replace(0, std::pair{9u, 10u});
        UNIT_ASSERT(values.MappedIUs() == (TUnorderedIUs{8, 9, 10}));
        UNIT_ASSERT_EXCEPTION(values.Add(1, std::pair{100u, 101u}), yexception);
        UNIT_ASSERT(values.MappedIUs() == (TUnorderedIUs{8, 9, 10}));
        UNIT_ASSERT(values.RetainKeys(TUnorderedIUs{0}));
        UNIT_ASSERT(values.Keys() == TUnorderedIUs{0});
        UNIT_ASSERT(values.MappedIUs() == (TUnorderedIUs{9, 10}));
        auto copy = values;
        values.Clear();
        UNIT_ASSERT(values.Keys().Empty());
        UNIT_ASSERT(values.MappedIUs().Empty());
        UNIT_ASSERT(copy.MappedIUs() == (TUnorderedIUs{9, 10}));
    }

    Y_UNIT_TEST(SubstitutionsAndAppendMissing) {
        const TSubstitutions substitutions{{1, 5}, {2, 1}};
        UNIT_ASSERT_VALUES_EQUAL(substitutions.At(2), 1);
        UNIT_ASSERT_EXCEPTION(substitutions.At(5), yexception);
        // Simultaneous: 2 becomes 1, not 5.
        UNIT_ASSERT_VALUES_EQUAL(Substitute(1, substitutions), 5);
        UNIT_ASSERT_VALUES_EQUAL(Substitute(2, substitutions), 1);
        UNIT_ASSERT_VALUES_EQUAL(Substitute(3, substitutions), 3);
        UNIT_ASSERT_EXCEPTION((TSubstitutions{{1, 5}, {1, 6}}), yexception);

        TOrderedIUs<> keys{4, 2};
        UNIT_ASSERT(!keys.AppendMissing(2));
        UNIT_ASSERT(keys.AppendMissing(9));
        keys.AppendMissing(TVector<TInfoUnitId>{2, 1, 1, 4});
        UNIT_ASSERT(keys == (TOrderedIUs<>{4, 2, 9, 1}));
        UNIT_ASSERT(keys.Unordered() == (TUnorderedIUs{1, 2, 4, 9}));
    }

    Y_UNIT_TEST(PairMembershipAndUnionRowsKeepTheirDifferentContracts) {
        TPairedIUs pairs{{1, 7}, {1, 8}, {2, 8}, {1, 7}};
        UNIT_ASSERT_VALUES_EQUAL(pairs.Items().size(), 3);
        UNIT_ASSERT(pairs.Left() == (TUnorderedIUs{1, 2}));
        UNIT_ASSERT(pairs.Right() == (TUnorderedIUs{7, 8}));
        UNIT_ASSERT(pairs.Remove(1, 8));
        UNIT_ASSERT(pairs.Left() == (TUnorderedIUs{1, 2}));
        UNIT_ASSERT(pairs.Right() == (TUnorderedIUs{7, 8}));
        TUnionAllIUs rows(TUnionInputPolicy{3});
        rows.Add(0, TUnionInputRow{{1, 7, 1}});
        UNIT_ASSERT_EXCEPTION(rows.Add(2, TUnionInputRow{{1, 7}}), yexception);
        UNIT_ASSERT(rows.Keys() == TUnorderedIUs{0});
        UNIT_ASSERT_VALUES_EQUAL(rows.Find(0)->Inputs[2], 1);
        UNIT_ASSERT_EXCEPTION(rows.Replace(0, TUnionInputRow{{1}}), yexception);
        UNIT_ASSERT_VALUES_EQUAL(rows.Find(0)->Inputs.size(), 3);
    }

    Y_UNIT_TEST(TemporariesHandOutViewsByValue) {
        static_assert(std::is_same_v<decltype(std::declval<TOrderedIUs<>&>().Items()), const TOrderedIUs<>::TEntries&>);
        static_assert(std::is_same_v<decltype(std::declval<TOrderedIUs<>>().Items()), TOrderedIUs<>::TEntries>);
        static_assert(std::is_same_v<decltype(std::declval<TPairedIUs>().Left()), TUnorderedIUs>);
        static_assert(std::is_same_v<decltype(std::declval<TSubstitutions>().Keys()), TUnorderedIUs>);

        // Range-for keeps only the view alive, so it must not refer into the temporary.
        auto makeOrdered = [] { return TOrderedIUs<>{3, 1, 3}; };
        TVector<TInfoUnitId> ids;
        for (const auto id : makeOrdered().Items()) {
            ids.push_back(id);
        }
        UNIT_ASSERT(ids == (TVector<TInfoUnitId>{3, 1, 3}));
        CheckSet(makeOrdered().Unordered(), {1, 3});

        const TPairedIUs pairs{{1, 7}, {2, 8}};
        UNIT_ASSERT_VALUES_EQUAL(TPairedIUs(pairs).Items().size(), 2);
        CheckSet(TPairedIUs(pairs).Left(), {1, 2});
        CheckSet(TPairedIUs(pairs).Right(), {7, 8});

        TMappedIUs<std::pair<TInfoUnitId, TInfoUnitId>, TDependencies> dependencies{{5, {1, 2}}};
        CheckSet(std::move(dependencies).MappedIUs(), {1, 2});
        CheckSet(dependencies.MappedIUs(), {1, 2});

        // Taking a cache leaves the source intact; taking its storage leaves it empty.
        auto ordered = makeOrdered();
        CheckSet(std::move(ordered).Unordered(), {1, 3});
        CheckSet(ordered.Unordered(), {1, 3});
        UNIT_ASSERT_VALUES_EQUAL(std::move(ordered).Items().size(), 3);
        UNIT_ASSERT(ordered.Items().empty());
        CheckSet(ordered.Unordered(), {});

        TSubstitutions substitutions{{1, 10}, {2, 20}};
        CheckSet(TSubstitutions(substitutions).Keys(), {1, 2});
        UNIT_ASSERT_VALUES_EQUAL(std::move(substitutions).Items().size(), 2);
        CheckSet(substitutions.Keys(), {});
        UNIT_ASSERT(!substitutions.Find(1));
    }

    Y_UNIT_TEST(RegistryIdentityAndPhysicalNames) {
        {
            TInfoUnitRegistry registry;
            const auto first = registry.Add(TInfoUnit("t", "a"));
            const auto second = registry.Add(TInfoUnit("t", "a"));
            const auto generated = registry.AddGenerated("intermediate_agg");
            UNIT_ASSERT_VALUES_EQUAL(first, 0);
            UNIT_ASSERT_VALUES_EQUAL(second, 1);
            UNIT_ASSERT_VALUES_EQUAL(generated, 2);
            UNIT_ASSERT_VALUES_EQUAL(registry.Get(first).GetFullName(), "t.a");
            UNIT_ASSERT_VALUES_EQUAL(registry.Get(second).GetFullName(), "t.a");
            UNIT_ASSERT(registry.IsGenerated(generated));
            UNIT_ASSERT(!registry.IsGenerated(first));
            UNIT_ASSERT_VALUES_EQUAL(registry.GetDisplayName(second), "t.a_1");
            UNIT_ASSERT_VALUES_EQUAL(registry.GetDisplayName(generated), "intermediate_agg1_2");
            // A copy is a fresh binding: a temporary keeps its annotation under the new ID.
            UNIT_ASSERT_VALUES_EQUAL(registry.GetDisplayName(registry.AddCopy(generated)), "intermediate_agg2_3");
            UNIT_ASSERT_VALUES_EQUAL(registry.GetDisplayName(registry.AddCopy(second)), "t.a_4");
            UNIT_ASSERT_VALUES_EQUAL(registry.GetDisplayName(registry.AddGenerated()), "tmp1_5");
            UNIT_ASSERT_VALUES_EQUAL(registry.GetDebugName(first), "%0[t.a]");
            UNIT_ASSERT_VALUES_EQUAL(registry.GetDebugName(generated), "%2{intermediate_agg1}");
            const TString prefix = "intermediate_agg";
            UNIT_ASSERT_VALUES_EQUAL(registry.GetDebugName(registry.AddGenerated(prefix)), "%6{intermediate_agg3}");
        }
        {
            TInfoUnitRegistry registry;
            const auto first = registry.Add(TInfoUnit("table", "column"));
            const auto second = registry.Add(TInfoUnit("table", "column"));
            const auto suffix = registry.Add(TInfoUnit("table", "column_0"));
            const TPhysicalNames names(registry);
            UNIT_ASSERT_VALUES_EQUAL(names.Get(first), "table.column_0");
            UNIT_ASSERT_VALUES_EQUAL(names.Get(second), "table.column_1");
            UNIT_ASSERT_VALUES_EQUAL(names.Get(suffix), "table.column_0_2");

            const auto later = registry.Add(TInfoUnit("later"));
            UNIT_ASSERT_EXCEPTION_CONTAINS(names.Get(later), yexception, "not frozen for lowering");

            const auto escapedSuffix = registry.Add(TInfoUnit("table", "column_0_"));
            const auto unique = registry.Add(TInfoUnit("column0"));
            const auto deadCopy = registry.AddCopy(unique);
            const auto generated = registry.AddGenerated("agg");
            const auto temporary = registry.Add(TInfoUnit("__kqp_win_acc_0_"));
            const auto temporarySuffix = registry.Add(TInfoUnit("__kqp_win_acc_0__"));
            registry.FinalizeDisplayNames({first, second, suffix, escapedSuffix, unique, generated, temporary, temporarySuffix});
            const TPhysicalNames finalNames(registry);
            UNIT_ASSERT_VALUES_EQUAL(finalNames.Get(first), "table.column_0__");
            UNIT_ASSERT_VALUES_EQUAL(finalNames.Get(second), "table.column_1");
            UNIT_ASSERT_VALUES_EQUAL(finalNames.Get(suffix), "table.column_0");
            UNIT_ASSERT_VALUES_EQUAL(finalNames.Get(escapedSuffix), "table.column_0_");
            UNIT_ASSERT_VALUES_EQUAL(finalNames.Get(unique), "column0");
            UNIT_ASSERT(finalNames.Get(deadCopy) != "column0");
            UNIT_ASSERT_VALUES_EQUAL(finalNames.Get(generated), "agg1");
            UNIT_ASSERT_VALUES_EQUAL(registry.GetDebugName(unique), "%5[column0]");
            UNIT_ASSERT_VALUES_EQUAL(registry.GetDebugName(generated), "%7{agg1}");

            THashSet<TString> spellings;
            for (TInfoUnitId id = 0; id < registry.Size(); ++id) {
                UNIT_ASSERT(spellings.insert(finalNames.Get(id)).second);
                UNIT_ASSERT_VALUES_EQUAL(finalNames.Get(id), registry.GetDisplayName(id));
            }
            const auto temporaryName = finalNames.GetTemporaryName("__kqp_win_acc_0_");
            UNIT_ASSERT(!spellings.contains(temporaryName));
            UNIT_ASSERT_VALUES_EQUAL(finalNames.GetTemporaryName("__kqp_win_acc_0_"), temporaryName);
            UNIT_ASSERT(finalNames.GetTemporaryName("__kqp_win_acc_1_") != temporaryName);
            UNIT_ASSERT_EXCEPTION_CONTAINS(registry.Add(TInfoUnit("late")), yexception, "finalized");
        }
    }
}
} // namespace NKikimr::NKqp

