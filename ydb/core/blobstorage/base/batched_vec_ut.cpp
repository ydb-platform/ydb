#include "batched_vec.h"

#include <library/cpp/testing/gtest/gtest.h>
#include <ydb/library/testlib/unittest_gtest_macro_subst.h>


namespace NKikimr {

    namespace {
        TEST(TBatchedVecTest, TestToStringInt) {
            TBatchedVec<ui64> vec {0, 1, 2, 3};
            UNIT_ASSERT_C(vec.ToString() == "[0 1 2 3]", "given string: " << vec.ToString());
        }

        struct TOutputType {
            char c;

            void Output(IOutputStream &os) const {
                os << c;
            }
        };

        TEST(TBatchedVecTest, TestOutputTOutputType) {
            TBatchedVec<TOutputType> vec { {'a'}, {'b'}, {'c'}, {'d'} };
            TStringStream str;
            vec.Output(str);
            UNIT_ASSERT_C(str.Str() == "[a b c d]", "given string: " << str.Str());
        }
    }

}
