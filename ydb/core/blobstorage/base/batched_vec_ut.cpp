#include "batched_vec.h"

#include <library/cpp/testing/unittest/registar.h>


namespace NKikimr {

    Y_UNIT_TEST_SUITE(TBatchedVecTest) {
        Y_UNIT_TEST(TestToStringInt) {
            TBatchedVec<ui64> vec {0, 1, 2, 3};
            UNIT_ASSERT_C(vec.ToString() == "[0 1 2 3]", "given string: " << vec.ToString());
        }

        struct TOutputType {
            char c;

            void Output(IOutputStream &os) const {
                os << c;
            }
        };

        Y_UNIT_TEST(TestOutputTOutputType) {
            TBatchedVec<TOutputType> vec { {'a'}, {'b'}, {'c'}, {'d'} };
            TStringStream str;
            vec.Output(str);
            UNIT_ASSERT_C(str.Str() == "[a b c d]", "given string: " << str.Str());
        }
    }

}
