#include "ut_common.h"

namespace cp = ::arrow::compute;


namespace NKikimr::NKernels {

Y_UNIT_TEST_SUITE(MathTest) {
    Y_UNIT_TEST(E) {
        auto res = arrow::compute::CallFunction(TE::Name, {}, GetCustomExecContext());
        UNIT_ASSERT(res->scalar()->Equals(arrow::MakeScalar(std::exp(1.0))));
    }

    Y_UNIT_TEST(Pi) {
        auto res = arrow::compute::CallFunction(TPi::Name, {}, GetCustomExecContext());
        UNIT_ASSERT(res->scalar()->Equals(arrow::MakeScalar(std::atan2(0, -1))));
    }

    Y_UNIT_TEST(AcoshFloat32) {
        std::vector<double> argVec = {2.324, 1.34234, 41.14324, 123};
        std::vector<double> expVec;
        for (auto val : argVec) {
            expVec.push_back(std::acosh(static_cast<float>(val)));
        }
        auto expRes = NumVecToArray(arrow::float64(), expVec);
        auto res = arrow::compute::CallFunction(TAcosh::Name, {NumVecToArray(arrow::float32(), argVec)}, GetCustomExecContext());
        UNIT_ASSERT(res->Equals(expRes));
    }

    Y_UNIT_TEST(AcoshFloat64) {
        std::vector<double> argVec = {2.324, 1.34234, 41.14324, 123};
        std::vector<double> expVec;
        for (auto val : argVec) {
            expVec.push_back(std::acosh(val));
        }
        auto expRes = NumVecToArray(arrow::float64(), expVec);
        auto res = arrow::compute::CallFunction(TAcosh::Name, {NumVecToArray(arrow::float64(), argVec)}, GetCustomExecContext());
        UNIT_ASSERT(res->Equals(expRes));
    }

    Y_UNIT_TEST(AcoshInts) {
        std::vector<double> argVec = {2.324, 1.34234, 41.14324, 123};
        std::vector<double> expVec;
        for (auto val : argVec) {
            expVec.push_back(std::acosh(static_cast<int64_t>(val)));
        }
        auto expRes = NumVecToArray(arrow::float64(), expVec);
        for (auto type : cp::internal::IntTypes()) {
            auto res = arrow::compute::CallFunction(TAcosh::Name, {NumVecToArray(type, argVec)}, GetCustomExecContext());
            UNIT_ASSERT(res->Equals(expRes));
        }
    }
}

}
