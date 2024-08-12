#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NMiniKQL {

Y_UNIT_TEST_SUITE(TKeyTypeContanerHelper) {
    struct TSetup {
        TSetup()
            : Alloc(__LOCATION__)
            , TypeEnv(Alloc)
            , TypeBuilder(TypeEnv)
            , MemUsage("TKeyTypeContanerHelperTest")
            , HolderFactory(Alloc.Ref(), MemUsage)
        {}
        TScopedAlloc Alloc;
        TTypeEnvironment TypeEnv;
        TTypeBuilder TypeBuilder;
        TMemoryUsageInfo MemUsage;
        THolderFactory HolderFactory;
    };

    struct TTestedSets {
        TTestedSets() = default;
        TTestedSets(const TType* type)
            : HashSetHelper(type)
            , HashSet(0, HashSetHelper.GetValueHash(), HashSetHelper.GetValueEqual())
            , CmpSetHelper(type)
            , CmpSet(CmpSetHelper.GetValueLess())
        {}

        void MoveFromHashToCmpSet() {
            const auto size = HashSet.size();
            while(!HashSet.empty()) {
                CmpSet.insert(std::move(*HashSet.begin()));
                HashSet.erase(HashSet.begin());
            }
            UNIT_ASSERT_EQUAL(size, CmpSet.size());
        }

        void Reset() {
            HashSetHelper = TKeyTypeContanerHelper<true, true, false>{};
            HashSet = std::unordered_set<NUdf::TUnboxedValue, TValueHasher, TValueEqual, TMKQLAllocator<NUdf::TUnboxedValue>>{
                0,
                HashSetHelper.GetValueHash(),
                HashSetHelper.GetValueEqual(),
            };
            CmpSetHelper = TKeyTypeContanerHelper<false, false, true>{};
            CmpSet = std::set<NUdf::TUnboxedValue, TValueLess, TMKQLAllocator<NUdf::TUnboxedValue>>{CmpSetHelper.GetValueLess()};
        }

        TKeyTypeContanerHelper<true, true, false> HashSetHelper;
        std::unordered_set<NUdf::TUnboxedValue, TValueHasher, TValueEqual, TMKQLAllocator<NUdf::TUnboxedValue>> HashSet;
        TKeyTypeContanerHelper<false, false, true> CmpSetHelper;
        std::set<NUdf::TUnboxedValue, TValueLess, TMKQLAllocator<NUdf::TUnboxedValue>> CmpSet;
    };

    Y_UNIT_TEST(StoreInts) {
        TSetup setup;
        TTestedSets sets{setup.TypeBuilder.NewDataType(NUdf::EDataSlot::Int32)};
        const size_t N = 100;
        for (size_t i = 0; i != N; ++i) {
            sets.HashSet.insert(NUdf::TUnboxedValuePod{(ui32)i});
        }
        UNIT_ASSERT_EQUAL(N, sets.HashSet.size());
        sets.MoveFromHashToCmpSet();
        for (size_t i = 0; i != N; ++i) {
            UNIT_ASSERT_EQUAL(i, sets.CmpSet.begin()->Get<ui32>());
            sets.CmpSet.erase(sets.CmpSet.begin());
        }
    }

    Y_UNIT_TEST(StoreStrings) {
        TSetup setup;
        TTestedSets sets{setup.TypeBuilder.NewDataType(NUdf::EDataSlot::String)};
        const size_t N = 100; //be aware of O(n^2) complexity in data generation
        for (size_t i = 0; i != N; ++i) {
            sets.HashSet.insert(NMiniKQL::MakeString(std::string(i, 'x')));
        }
        UNIT_ASSERT_EQUAL(N, sets.HashSet.size());
        sets.MoveFromHashToCmpSet();
        for (size_t i = 0; i != N; ++i) {
            UNIT_ASSERT_EQUAL(i, sets.CmpSet.begin()->AsStringRef().Size());
            sets.CmpSet.erase(sets.CmpSet.begin());
        }
    }

    Y_UNIT_TEST(StoreTuples) {
        TSetup setup;
        TType* elems[] = {
            setup.TypeBuilder.NewDataType(NUdf::EDataSlot::Uint32),
            setup.TypeBuilder.NewDataType(NUdf::EDataSlot::String)
        };
        TTestedSets sets{setup.TypeBuilder.NewTupleType(elems)};
        const size_t N = 100; //be aware of O(n^2) complexity in data generation
        for (size_t i = 0; i != N; ++i) {
            NUdf::TUnboxedValue* items;
            NUdf::TUnboxedValue v = setup.HolderFactory.CreateDirectArrayHolder(2, items);
            items[0] = NUdf::TUnboxedValuePod{(ui32)i};
            items[1] = NMiniKQL::MakeString(std::string(i, 'x'));
            sets.HashSet.insert(std::move(v));
        }
        UNIT_ASSERT_EQUAL(N, sets.HashSet.size());
        sets.MoveFromHashToCmpSet();
        for (size_t i = 0; i != N; ++i) {
            UNIT_ASSERT_EQUAL(i, sets.CmpSet.begin()->GetElement(0).Get<ui32>());
            NUdf::TUnboxedValue s = sets.CmpSet.begin()->GetElement(1);
            UNIT_ASSERT_EQUAL(i, s.AsStringRef().Size());
            sets.CmpSet.erase(sets.CmpSet.begin());
        }
    }

    Y_UNIT_TEST(StoreStructs) {
        TSetup setup;
        std::pair<std::string_view, TType*> elems[] = {
            std::pair<std::string_view, TType*>{"i", setup.TypeBuilder.NewDataType(NUdf::EDataSlot::Uint32)},
            std::pair<std::string_view, TType*>{"s", setup.TypeBuilder.NewDataType(NUdf::EDataSlot::String)}
        };
        TTestedSets sets{setup.TypeBuilder.NewStructType(elems)};
        const size_t N = 100; //be aware of O(n^2) complexity in data generation
        for (size_t i = 0; i != N; ++i) {
            NUdf::TUnboxedValue* items;
            NUdf::TUnboxedValue v = setup.HolderFactory.CreateDirectArrayHolder(2, items);
            items[0] = NUdf::TUnboxedValuePod{(ui32)i};
            items[1] = NMiniKQL::MakeString(std::string(i, 'x'));
            sets.HashSet.insert(std::move(v));
        }
        UNIT_ASSERT_EQUAL(N, sets.HashSet.size());
        sets.MoveFromHashToCmpSet();
        for (size_t i = 0; i != N; ++i) {
            UNIT_ASSERT_EQUAL(i, sets.CmpSet.begin()->GetElement(0).Get<ui32>());
            NUdf::TUnboxedValue s = sets.CmpSet.begin()->GetElement(1);
            UNIT_ASSERT_EQUAL(i, s.AsStringRef().Size());
            sets.CmpSet.erase(sets.CmpSet.begin());
        }
    }

    Y_UNIT_TEST(ReleaseAllResources) {
        auto setup = std::make_shared<TSetup>();
        auto guard = Guard(setup->Alloc);

        TTestedSets intSets{setup->TypeBuilder.NewDataType(NUdf::EDataSlot::Int32)};

        TTestedSets stringSets{setup->TypeBuilder.NewDataType(NUdf::EDataSlot::String)};

        TType* tupleElems[] = {
            setup->TypeBuilder.NewDataType(NUdf::EDataSlot::Uint32),
            setup->TypeBuilder.NewDataType(NUdf::EDataSlot::String)
        };
        TTestedSets tupleSets{setup->TypeBuilder.NewTupleType(tupleElems)};

        std::pair<std::string_view, TType*> structElems[] = {
            std::pair<std::string_view, TType*>{"i", setup->TypeBuilder.NewDataType(NUdf::EDataSlot::Uint32)},
            std::pair<std::string_view, TType*>{"s", setup->TypeBuilder.NewDataType(NUdf::EDataSlot::String)}
        };
        TTestedSets structSets{setup->TypeBuilder.NewStructType(structElems)};

        intSets.Reset();
        stringSets.Reset();
        tupleSets.Reset();
        structSets.Reset();

        guard.Release();
        setup.reset();
    }
}

} //namespace namespace NKikimr::NMiniKQL
