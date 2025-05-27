#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/hash/calcer.h>
#include <ydb/library/formats/arrow/hash/xx_hash.h>

Y_UNIT_TEST_SUITE(Hash) {

    using namespace NKikimr::NArrow;

    Y_UNIT_TEST(ScalarBinaryHash) {
        std::shared_ptr<arrow::Scalar> s1 = std::make_shared<arrow::StringScalar>("abcde");
        std::shared_ptr<arrow::Scalar> s2 = std::make_shared<arrow::StringScalar>("abcde");
        NHash::NXX64::TStreamStringHashCalcer calcer1(0);
        calcer1.Start();
        NHash::TXX64::AppendField(s1, calcer1);

        NHash::NXX64::TStreamStringHashCalcer calcer2(0);
        calcer2.Start();
        NHash::TXX64::AppendField(s2, calcer2);
        const ui64 hash = calcer1.Finish();
        Cerr << hash << Endl;
        Y_ABORT_UNLESS(hash == calcer2.Finish());
    }

    Y_UNIT_TEST(ScalarCTypeHash) {
        std::shared_ptr<arrow::Scalar> s1 = std::make_shared<arrow::UInt32Scalar>(52);
        std::shared_ptr<arrow::Scalar> s2 = std::make_shared<arrow::UInt32Scalar>(52);
        NHash::NXX64::TStreamStringHashCalcer calcer1(0);
        calcer1.Start();
        NHash::TXX64::AppendField(s1, calcer1);

        NHash::NXX64::TStreamStringHashCalcer calcer2(0);
        calcer2.Start();
        NHash::TXX64::AppendField(s2, calcer2);
        const ui64 hash = calcer1.Finish();
        Cerr << hash << Endl;
        Y_ABORT_UNLESS(hash == calcer2.Finish());
    }

    Y_UNIT_TEST(ScalarCompositeHash) {
        std::shared_ptr<arrow::Scalar> s11 = std::make_shared<arrow::StringScalar>("abcde");
        std::shared_ptr<arrow::Scalar> s12 = std::make_shared<arrow::UInt32Scalar>(52);
        std::shared_ptr<arrow::Scalar> s21 = std::make_shared<arrow::StringScalar>("abcde");
        std::shared_ptr<arrow::Scalar> s22 = std::make_shared<arrow::UInt32Scalar>(52);
        NHash::NXX64::TStreamStringHashCalcer calcer1(0);
        calcer1.Start();
        NHash::TXX64::AppendField(s11, calcer1);
        NHash::TXX64::AppendField(s12, calcer1);

        NHash::NXX64::TStreamStringHashCalcer calcer2(0);
        calcer2.Start();
        NHash::TXX64::AppendField(s21, calcer2);
        NHash::TXX64::AppendField(s22, calcer2);
        const ui64 hash = calcer1.Finish();
        Cerr << hash << Endl;
        Y_ABORT_UNLESS(hash == calcer2.Finish());
    }


};
