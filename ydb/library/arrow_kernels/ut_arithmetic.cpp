#include "ut_common.h"

namespace cp = ::arrow::compute;
using cp::internal::applicator::ScalarBinary;
using cp::internal::applicator::ScalarUnary;


namespace NKikimr::NKernels {

const std::vector<std::shared_ptr<arrow::DataType>> nonPrimitiveTypes = {
    arrow::list(arrow::utf8()),
    arrow::list(arrow::int64()),
    arrow::large_list(arrow::large_utf8()),
    arrow::fixed_size_list(arrow::utf8(), 3),
    arrow::fixed_size_list(arrow::int64(), 4),
    arrow::dictionary(arrow::int32(), arrow::utf8())
};

const std::vector<std::shared_ptr<arrow::DataType>> decimalTypes = {
    arrow::decimal128(12, 2),
    arrow::decimal256(12, 2)
};


template<typename TType>
EnableIfSigned<TType> MakeRandomNum() {
    double lowerLimit = -120;
    double upperLimit = 120;
    return static_cast<TType>(double(rand()) * (upperLimit - lowerLimit) / RAND_MAX + lowerLimit);
}

template<typename TType>
EnableIfUnsigned<TType> MakeRandomNum() {
    double lowerLimit = 0;
    double upperLimit = 120;
    return static_cast<TType>(double(rand()) * (upperLimit - lowerLimit) / RAND_MAX + lowerLimit);
}

template<typename TType>
std::shared_ptr<TArray<TType>> GenerateTestArray(int64_t sz) {
    std::srand(std::time(nullptr));
    TBuilder<TType> builder;
    std::shared_ptr<TArray<TType>> res;
    arrow::Status st;
    UNIT_ASSERT(builder.Reserve(sz).ok());
    for (int64_t i = 0; i < sz; ++i) {
        UNIT_ASSERT(builder.Append(MakeRandomNum<typename TType::c_type>()).ok());
    }
    UNIT_ASSERT(builder.Finish(&res).ok());
    return res;
}

void TestWrongTypeUnary(const std::string& func_name, std::shared_ptr<arrow::DataType> type, cp::ExecContext* exc = nullptr) {
    auto res = arrow::compute::CallFunction(func_name, {arrow::MakeArrayOfNull(type, 1).ValueOrDie()}, exc);
    UNIT_ASSERT_EQUAL(res.ok(), false);
}

void TestWrongTypeBinary(const std::string& func_name, std::shared_ptr<arrow::DataType> type0,
                                                       std::shared_ptr<arrow::DataType> type1,
                                                       cp::ExecContext* exc = nullptr) {
    auto res = arrow::compute::CallFunction(func_name, {arrow::MakeArrayOfNull(type0, 1).ValueOrDie(),
                                                        arrow::MakeArrayOfNull(type1, 1).ValueOrDie()},
                                                        exc);
    UNIT_ASSERT_EQUAL(res.ok(), false);
}

std::shared_ptr<arrow::Array> MakeBooleanArray(const std::vector<bool>& arr) {
    arrow::BooleanBuilder builder;
    std::shared_ptr<arrow::BooleanArray> res;
    UNIT_ASSERT(builder.Reserve(arr.size()).ok());
    for (size_t i = 0; i < arr.size(); ++i) {
        UNIT_ASSERT(builder.Append(arr[i]).ok());
    }
    UNIT_ASSERT(builder.Finish(&res).ok());
    return res;
}

Y_UNIT_TEST_SUITE(ArrowAbsTest) {
    Y_UNIT_TEST(AbsSignedInts) {
        for (auto type : cp::internal::SignedIntTypes()) {
            auto arg0 = NumVecToArray(type, {-32, -12, 54});
            auto expected = NumVecToArray(type, {32, 12, 54});
            auto res = arrow::compute::CallFunction("abs", {arg0});
            UNIT_ASSERT(res->Equals(expected));
        }
    }

    Y_UNIT_TEST(AbsUnsignedInts) {
        for (auto type : cp::internal::UnsignedIntTypes()) {
            auto arg0 = NumVecToArray(type, {32, 12, 54});
            auto expected = NumVecToArray(type, {32, 12, 54});
            auto res = arrow::compute::CallFunction("abs", {arg0});
            UNIT_ASSERT(res->Equals(expected));
        }
    }

    Y_UNIT_TEST(AbsFloating) {
        for (auto type : cp::internal::FloatingPointTypes()) {
            auto arg0 = NumVecToArray(type, {-32.4, -12.3, 54.7});
            auto expected = NumVecToArray(type, {32.4, 12.3, 54.7});
            auto res = arrow::compute::CallFunction("abs", {arg0});
            UNIT_ASSERT(res->Equals(expected));
        }
    }

    Y_UNIT_TEST(AbsNull) {
        for (auto type : cp::internal::IntTypes()) {
            auto arg0 = arrow::MakeArrayOfNull(type, 3).ValueOrDie();
            auto expected = arrow::MakeArrayOfNull(type, 3).ValueOrDie();
            auto res = arrow::compute::CallFunction("abs", {arg0});
            UNIT_ASSERT(res->Equals(expected));
        }
    }

    /*Y_UNIT_TEST(AbsWrongTypes) {
        for (auto type : cp::internal::TemporalTypes()) {
            TestWrongTypeUnary(TAbsoluteValue::Name, type);
        }
        for (auto type : cp::internal::StringTypes()) {
            TestWrongTypeUnary(TAbsoluteValue::Name, type);
        }
        for (auto type : cp::internal::BaseBinaryTypes()) {
            TestWrongTypeUnary(TAbsoluteValue::Name, type);
        }
        for (auto type : nonPrimitiveTypes) {
            TestWrongTypeUnary(TAbsoluteValue::Name, type);
        }
    }*/
}

Y_UNIT_TEST_SUITE(ArrowNegateTest) {
    Y_UNIT_TEST(NegateSignedInts) {
        for (auto type : cp::internal::SignedIntTypes()) {
            auto arg0 = NumVecToArray(type, {-32, -12, 54});
            auto expected = NumVecToArray(type, {32, 12, -54});
            auto res = arrow::compute::CallFunction("negate", {arg0});
            UNIT_ASSERT(res->Equals(expected));
        }
    }

    Y_UNIT_TEST(NegateFloating) {
        for (auto type : cp::internal::FloatingPointTypes()) {
            auto arg0 = NumVecToArray(type, {-32.4, -12.3, 54.7});
            auto expected = NumVecToArray(type, {32.4, 12.3, -54.7});
            auto res = arrow::compute::CallFunction("negate", {arg0});
            UNIT_ASSERT(res->Equals(expected));
        }
    }

    Y_UNIT_TEST(NegateWrongTypes) {
        for (auto type : cp::internal::TemporalTypes()) {
            TestWrongTypeUnary("negate", type);
        }
        for (auto type : cp::internal::StringTypes()) {
            TestWrongTypeUnary("negate", type);
        }
        for (auto type : cp::internal::BaseBinaryTypes()) {
            TestWrongTypeUnary("negate", type);
        }
        for (auto type : nonPrimitiveTypes) {
            TestWrongTypeUnary("negate", type);
        }
    }

    Y_UNIT_TEST(NegateNull) {
        for (auto type : cp::internal::SignedIntTypes()) {
            auto arg0 = arrow::MakeArrayOfNull(type, 3).ValueOrDie();
            auto expected = arrow::MakeArrayOfNull(type, 3).ValueOrDie();
            auto res = arrow::compute::CallFunction("negate", {arg0});
            UNIT_ASSERT(res->Equals(expected));
        }
    }
}



Y_UNIT_TEST_SUITE(GcdTest) {
    Y_UNIT_TEST(GcdInts) {
        for (auto type : cp::internal::IntTypes()) {
            auto arg0 = NumVecToArray(type, {32, 12});
            auto arg1 = NumVecToArray(type, {24, 10});
            auto expected = NumVecToArray(type, {8, 2});
            auto res = arrow::compute::CallFunction(TGreatestCommonDivisor::Name, {arg0, arg1}, GetCustomExecContext());
            UNIT_ASSERT(res->Equals(expected));
        }
    }

    Y_UNIT_TEST(GcdNull) {
        for (auto type : cp::internal::IntTypes()) {
            auto arg0 = arrow::MakeArrayOfNull(type, 2).ValueOrDie();
            auto arg1 = NumVecToArray(type, {24, 10});
            auto expected = arrow::MakeArrayOfNull(type, 2).ValueOrDie();
            auto res = arrow::compute::CallFunction(TGreatestCommonDivisor::Name, {arg0, arg1}, GetCustomExecContext());
            UNIT_ASSERT(res->Equals(expected));
        }
    }

    Y_UNIT_TEST(GcdDiffTypes) {
        auto registry = cp::GetFunctionRegistry();
        auto func = MakeArithmeticIntBinary<TGreatestCommonDivisor>(TGreatestCommonDivisor::Name);
        UNIT_ASSERT(registry->AddFunction(func, true).ok());
        for (auto type0 : cp::internal::IntTypes()) {
            for (auto type1 : cp::internal::IntTypes()) {
                auto arg0 = NumVecToArray(type0, {32, 12});
                auto arg1 = NumVecToArray(type1, {16, 10});
                auto res = arrow::compute::CallFunction(TGreatestCommonDivisor::Name, {arg0, arg1}, GetCustomExecContext());
                auto expected = NumVecToArray(res->type(), {16, 2});
                UNIT_ASSERT(res->Equals(expected));
            }
        }
    }

    Y_UNIT_TEST(GcdWrongTypes) {
        for (auto type0 : cp::internal::FloatingPointTypes()) {
            for (auto type1 : cp::internal::NumericTypes()) {
                TestWrongTypeBinary(TGreatestCommonDivisor::Name, type0, type1, GetCustomExecContext());
            }
        }
        for (auto type : cp::internal::FloatingPointTypes()) {
            TestWrongTypeBinary(TGreatestCommonDivisor::Name, type, type, GetCustomExecContext());
        }
        for (auto type : cp::internal::TemporalTypes()) {
            TestWrongTypeBinary(TGreatestCommonDivisor::Name, type, type, GetCustomExecContext());
        }
        for (auto type : cp::internal::StringTypes()) {
            TestWrongTypeBinary(TGreatestCommonDivisor::Name, type, type, GetCustomExecContext());
        }
        for (auto type : cp::internal::BaseBinaryTypes()) {
            TestWrongTypeBinary(TGreatestCommonDivisor::Name, type, type);
        }
        for (auto type : nonPrimitiveTypes) {
            TestWrongTypeBinary(TGreatestCommonDivisor::Name, type, type);
        }
    }
}


Y_UNIT_TEST_SUITE(LcmTest) {
    Y_UNIT_TEST(LcmInts) {
        for (auto type : cp::internal::IntTypes()) {
            auto arg0 = NumVecToArray(type, {6, 12});
            auto arg1 = NumVecToArray(type, {3, 10});
            auto expected = NumVecToArray(type, {6, 60});
            auto res = arrow::compute::CallFunction(TLeastCommonMultiple::Name, {arg0, arg1}, GetCustomExecContext());
            UNIT_ASSERT(res->Equals(expected));
        }
    }

    Y_UNIT_TEST(LcmNull) {
        for (auto type : cp::internal::IntTypes()) {
            auto arg0 = arrow::MakeArrayOfNull(type, 2).ValueOrDie();
            auto arg1 = NumVecToArray(type, {24, 10});
            auto expected = arrow::MakeArrayOfNull(type, 2).ValueOrDie();
            auto res = arrow::compute::CallFunction(TLeastCommonMultiple::Name, {arg0, arg1}, GetCustomExecContext());
            UNIT_ASSERT(res->Equals(expected));
        }
    }

    Y_UNIT_TEST(LcmDiffTypes) {
        for (auto type0 : cp::internal::IntTypes()) {
            for (auto type1 : cp::internal::IntTypes()) {
                auto arg0 = NumVecToArray(type0, {6, 12});
                auto arg1 = NumVecToArray(type1, {3, 10});
                auto res = arrow::compute::CallFunction(TLeastCommonMultiple::Name, {arg0, arg1}, GetCustomExecContext());
                auto expected = NumVecToArray(res->type(), {6, 60});
                UNIT_ASSERT(res->Equals(expected));
            }
        }
    }

    Y_UNIT_TEST(LcmWrongTypes) {
        for (auto type0 : cp::internal::FloatingPointTypes()) {
            for (auto type1 : cp::internal::NumericTypes()) {
                TestWrongTypeBinary(TLeastCommonMultiple::Name, type0, type1, GetCustomExecContext());
            }
        }
        for (auto type : cp::internal::FloatingPointTypes()) {
            TestWrongTypeBinary(TLeastCommonMultiple::Name, type, type, GetCustomExecContext());
        }
        for (auto type : cp::internal::TemporalTypes()) {
            TestWrongTypeBinary(TLeastCommonMultiple::Name, type, type, GetCustomExecContext());
        }
        for (auto type : cp::internal::StringTypes()) {
            TestWrongTypeBinary(TLeastCommonMultiple::Name, type, type, GetCustomExecContext());
        }
        for (auto type : cp::internal::BaseBinaryTypes()) {
            TestWrongTypeBinary(TLeastCommonMultiple::Name, type, type, GetCustomExecContext());
        }
        for (auto type : nonPrimitiveTypes) {
            TestWrongTypeBinary(TLeastCommonMultiple::Name, type, type, GetCustomExecContext());
        }
    }

    Y_UNIT_TEST(LcmNullDivide) {
        for (auto type : cp::internal::IntTypes()) {
            auto arg0 = NumVecToArray(type, {0, 16});
            auto arg1 = NumVecToArray(type, {0, 5});
            auto res = arrow::compute::CallFunction(TLeastCommonMultiple::Name, {arg0, arg1}, GetCustomExecContext());
            UNIT_ASSERT(!res.ok());
        }
    }
}

Y_UNIT_TEST_SUITE(ModuloTest) {
    Y_UNIT_TEST(ModuloInts) {
        for (auto type : cp::internal::IntTypes()) {
            auto arg0 = NumVecToArray(type, {10, 16});
            auto arg1 = NumVecToArray(type, {3, 5});
            auto expected = NumVecToArray(type, {1, 1});
            auto res = arrow::compute::CallFunction(TModulo::Name, {arg0, arg1}, GetCustomExecContext());
            UNIT_ASSERT(res->Equals(expected));
        }
    }

    Y_UNIT_TEST(ModuloFloats) {
        for (auto type : cp::internal::FloatingPointTypes()) {
            auto arg0 = NumVecToArray(type, {10.2234, 16.2347});
            auto arg1 = NumVecToArray(type, {3.22343, 5.4234});
            auto expected = NumVecToArray(type, {1, 1});
            auto res = arrow::compute::CallFunction(TModulo::Name, {arg0, arg1}, GetCustomExecContext());
            UNIT_ASSERT(res->Equals(expected));
        }
    }

    Y_UNIT_TEST(ModuloNullDivide) {
        for (auto type : cp::internal::NumericTypes()) {
            auto arg0 = NumVecToArray(type, {10.2234, 16.2347});
            auto arg1 = NumVecToArray(type, {0, 5.4234});
            auto res = arrow::compute::CallFunction(TModulo::Name, {arg0, arg1}, GetCustomExecContext());
            UNIT_ASSERT(!res.ok());
        }
    }
}

Y_UNIT_TEST_SUITE(ModuloOrZeroTest) {
    Y_UNIT_TEST(ModuloOrZeroInts) {
        for (auto type : cp::internal::IntTypes()) {
            auto arg0 = NumVecToArray(type, {10, 16});
            auto arg1 = NumVecToArray(type, {6, 0});
            auto expected = NumVecToArray(type, {4, 0});
            auto res = arrow::compute::CallFunction(TModuloOrZero::Name, {arg0, arg1}, GetCustomExecContext());
            UNIT_ASSERT(res->Equals(expected));
        }
    }

    Y_UNIT_TEST(ModuloOrZeroFloats) {
        for (auto type : cp::internal::FloatingPointTypes()) {
            auto arg0 = NumVecToArray(type, {10.2234, 16.2347});
            auto arg1 = NumVecToArray(type, {0.23, 5.4234});
            auto expected = NumVecToArray(type, {0, 1});
            auto res = arrow::compute::CallFunction(TModuloOrZero::Name, {arg0, arg1}, GetCustomExecContext());
            UNIT_ASSERT(res->Equals(expected));
        }
    }
}

}
