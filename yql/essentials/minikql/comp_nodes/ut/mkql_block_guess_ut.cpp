#include <yql/essentials/minikql/comp_nodes/mkql_block_guess.h>

#include <yql/essentials/minikql/comp_nodes/ut/mkql_block_test_helper.h>
#include <yql/essentials/minikql/comp_nodes/ut/mkql_computation_node_ut.h>

namespace NKikimr::NMiniKQL {

using namespace NTest;

namespace {

template <typename TInputData, typename TExpectedData>
void TestBlockGuess(const TVector<TInputData>& data, const TVector<TExpectedData>& expected, ui32 tupleIndex) {
    TBlockHelper().TestKernelFuzzied(data, expected, [tupleIndex](TSetup<false>& setup, TRuntimeNode variantValue) {
        return setup.PgmBuilder->BlockGuess(variantValue, tupleIndex);
    });
}

} // namespace

Y_UNIT_TEST_SUITE(TMiniKQLBlockGuessTest) {

Y_UNIT_TEST(TupleVariant_Ui32Ui64_Index0) {
    using TVariant = std::variant<ui32, ui64>;
    TVector<TVariant> data = {TVariant{ui32{1}}, TVariant{ui64{2}}, TVariant{ui32{3}}};
    TVector<TMaybe<ui32>> expected = {ui32{1}, Nothing(), ui32{3}};
    TestBlockGuess(data, expected, 0u);
}

Y_UNIT_TEST(TupleVariant_Ui32Ui64_Index1) {
    using TVariant = std::variant<ui32, ui64>;
    TVector<TVariant> data = {TVariant{ui32{1}}, TVariant{ui64{2}}, TVariant{ui32{3}}};
    TVector<TMaybe<ui64>> expected = {Nothing(), ui64{2}, Nothing()};
    TestBlockGuess(data, expected, 1u);
}

Y_UNIT_TEST(TupleVariant_AllSameAlternative) {
    using TVariant = std::variant<ui32, ui64>;
    TVector<TVariant> data = {TVariant{ui32{10}}, TVariant{ui32{20}}, TVariant{ui32{30}}};
    TVector<TMaybe<ui32>> expected = {ui32{10}, ui32{20}, ui32{30}};
    TestBlockGuess(data, expected, 0u);
}

Y_UNIT_TEST(TupleVariant_NoneMatchAlternative) {
    using TVariant = std::variant<ui32, ui64>;
    TVector<TVariant> data = {TVariant{ui32{1}}, TVariant{ui32{2}}, TVariant{ui32{3}}};
    TVector<TMaybe<ui64>> expected = {Nothing(), Nothing(), Nothing()};
    TestBlockGuess(data, expected, 1u);
}

Y_UNIT_TEST(TupleVariant_StringAlternative) {
    using TVariant = std::variant<ui32, TString>;
    TVector<TVariant> data = {TVariant{ui32{42}}, TVariant{TString{"hello"}}, TVariant{ui32{7}}};
    TVector<TMaybe<TString>> expected = {Nothing(), TMaybe<TString>{"hello"}, Nothing()};
    TestBlockGuess(data, expected, 1u);
}

Y_UNIT_TEST(TupleVariant_TupleAlternative) {
    using TTupleAlternative = std::tuple<ui32, TString>;
    using TVariant = std::variant<TTupleAlternative, ui64>;
    TVector<TVariant> data = {
        TVariant{TTupleAlternative{ui32{1}, TString{"a"}}},
        TVariant{ui64{2}},
        TVariant{TTupleAlternative{ui32{3}, TString{"b"}}},
    };
    TVector<TMaybe<TTupleAlternative>> expected = {
        TMaybe<TTupleAlternative>{TTupleAlternative{ui32{1}, TString{"a"}}},
        Nothing(),
        TMaybe<TTupleAlternative>{TTupleAlternative{ui32{3}, TString{"b"}}},
    };
    TestBlockGuess(data, expected, 0u);
}

Y_UNIT_TEST(TupleVariant_PgIntAlternative) {
    using TVariant = std::variant<TPgInt, ui64>;
    TVector<TVariant> data = {TVariant{TPgInt{10}}, TVariant{ui64{20}}, TVariant{TPgInt{30}}};
    TVector<TMaybe<TPgInt>> expected = {TMaybe<TPgInt>{TPgInt{10}}, Nothing(), TMaybe<TPgInt>{TPgInt{30}}};
    TestBlockGuess(data, expected, 0u);
}

Y_UNIT_TEST(TupleVariant_OptionalAlternative_DoubleOptional) {
    using TVariant = std::variant<TMaybe<ui32>, ui64>;
    TVector<TVariant> data = {
        TVariant{TMaybe<ui32>{10u}},
        TVariant{ui64{20u}},
        TVariant{TMaybe<ui32>{}},
    };
    TVector<TMaybe<TMaybe<ui32>>> expected = {
        TMaybe<TMaybe<ui32>>{TMaybe<ui32>{10u}},
        TMaybe<TMaybe<ui32>>{},
        TMaybe<TMaybe<ui32>>{TMaybe<ui32>{}},
    };
    TestBlockGuess(data, expected, 0u);
}

Y_UNIT_TEST(OptionalTupleVariant_NullRows) {
    using TVariant = std::variant<ui32, ui64>;
    TVector<TMaybe<TVariant>> data = {
        TMaybe<TVariant>{TVariant{ui32{1}}},
        TMaybe<TVariant>{},
        TMaybe<TVariant>{TVariant{ui32{3}}},
    };
    TVector<TMaybe<ui32>> expected = {ui32{1}, Nothing(), ui32{3}};
    TestBlockGuess(data, expected, 0u);
}

Y_UNIT_TEST(OptionalTupleVariant_AllNull) {
    using TVariant = std::variant<ui32, ui64>;
    TVector<TMaybe<TVariant>> data = {TMaybe<TVariant>{}, TMaybe<TVariant>{}, TMaybe<TVariant>{}};
    TVector<TMaybe<ui32>> expected = {Nothing(), Nothing(), Nothing()};
    TestBlockGuess(data, expected, 0u);
}

Y_UNIT_TEST(OptionalTupleVariant_NoNull) {
    using TVariant = std::variant<ui32, ui64>;
    TVector<TMaybe<TVariant>> data = {
        TMaybe<TVariant>{TVariant{ui32{1}}},
        TMaybe<TVariant>{TVariant{ui64{2}}},
        TMaybe<TVariant>{TVariant{ui32{3}}},
    };
    TVector<TMaybe<ui32>> expected = {ui32{1}, Nothing(), ui32{3}};
    TestBlockGuess(data, expected, 0u);
}

Y_UNIT_TEST(OptionalTupleVariant_MixedNull) {
    using TVariant = std::variant<ui32, ui64>;
    TVector<TMaybe<TVariant>> data = {
        TMaybe<TVariant>{TVariant{ui64{10}}},
        TMaybe<TVariant>{},
        TMaybe<TVariant>{TVariant{ui64{20}}},
    };
    TVector<TMaybe<ui64>> expected = {ui64{10}, Nothing(), ui64{20}};
    TestBlockGuess(data, expected, 1u);
}

Y_UNIT_TEST(OptionalTupleVariant_SomeFieldsSet_StringAlternative) {
    using TVariant = std::variant<TString, ui64>;
    TVector<TMaybe<TVariant>> data = {
        TMaybe<TVariant>{TVariant{TString{"hello"}}},
        TMaybe<TVariant>{},
        TMaybe<TVariant>{TVariant{ui64{42}}},
        TMaybe<TVariant>{TVariant{TString{"world"}}},
    };
    TVector<TMaybe<TString>> expected = {
        TMaybe<TString>{"hello"},
        Nothing(),
        Nothing(),
        TMaybe<TString>{"world"},
    };
    TestBlockGuess(data, expected, 0u);
}

Y_UNIT_TEST(OptionalTupleVariant_AllFieldsSet_StringAlternative) {
    using TVariant = std::variant<TString, ui64>;
    TVector<TMaybe<TVariant>> data = {
        TMaybe<TVariant>{TVariant{TString{"foo"}}},
        TMaybe<TVariant>{TVariant{TString{"bar"}}},
        TMaybe<TVariant>{TVariant{TString{"baz"}}},
    };
    TVector<TMaybe<TString>> expected = {
        TMaybe<TString>{"foo"},
        TMaybe<TString>{"bar"},
        TMaybe<TString>{"baz"},
    };
    TestBlockGuess(data, expected, 0u);
}

Y_UNIT_TEST(TupleVariant_OptionalString_AllStringsSet) {
    using TVariant = std::variant<TMaybe<TString>, ui64>;
    TVector<TVariant> data = {
        TVariant{TMaybe<TString>{"foo"}},
        TVariant{TMaybe<TString>{"bar"}},
        TVariant{TMaybe<TString>{"baz"}},
    };
    TVector<TMaybe<TMaybe<TString>>> expected = {
        TMaybe<TMaybe<TString>>{TMaybe<TString>{"foo"}},
        TMaybe<TMaybe<TString>>{TMaybe<TString>{"bar"}},
        TMaybe<TMaybe<TString>>{TMaybe<TString>{"baz"}},
    };
    TestBlockGuess(data, expected, 0u);
}

Y_UNIT_TEST(TupleVariant_OptionalDouble_AllDoublesSet) {
    using TVariant = std::variant<TMaybe<double>, ui64>;
    TVector<TVariant> data = {
        TVariant{TMaybe<double>{1.5}},
        TVariant{TMaybe<double>{2.5}},
        TVariant{TMaybe<double>{3.5}},
    };
    TVector<TMaybe<TMaybe<double>>> expected = {
        TMaybe<TMaybe<double>>{TMaybe<double>{1.5}},
        TMaybe<TMaybe<double>>{TMaybe<double>{2.5}},
        TMaybe<TMaybe<double>>{TMaybe<double>{3.5}},
    };
    TestBlockGuess(data, expected, 0u);
}

Y_UNIT_TEST(OptionalTupleVariant_AllVariantsOmitted_StringAlternative) {
    using TVariant = std::variant<TString, ui64>;
    TVector<TMaybe<TVariant>> data = {
        TMaybe<TVariant>{},
        TMaybe<TVariant>{},
        TMaybe<TVariant>{},
    };
    TVector<TMaybe<TString>> expected = {Nothing(), Nothing(), Nothing()};
    TestBlockGuess(data, expected, 0u);
}

Y_UNIT_TEST(TupleVariant_OptionalString_AllStringsOmitted) {
    using TVariant = std::variant<TMaybe<TString>, ui64>;
    TVector<TVariant> data = {
        TVariant{TMaybe<TString>{}},
        TVariant{TMaybe<TString>{}},
        TVariant{TMaybe<TString>{}},
    };
    TVector<TMaybe<TMaybe<TString>>> expected = {
        TMaybe<TMaybe<TString>>{TMaybe<TString>{}},
        TMaybe<TMaybe<TString>>{TMaybe<TString>{}},
        TMaybe<TMaybe<TString>>{TMaybe<TString>{}},
    };
    TestBlockGuess(data, expected, 0u);
}

} // Y_UNIT_TEST_SUITE(TMiniKQLBlockGuessTest)

} // namespace NKikimr::NMiniKQL
