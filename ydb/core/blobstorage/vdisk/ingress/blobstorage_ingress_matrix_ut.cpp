#include "blobstorage_ingress_matrix.h"

#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <library/cpp/testing/unittest/registar.h>

#define STR Cnull

namespace NKikimr {

    using namespace NMatrix;

    Y_UNIT_TEST_SUITE(TBlobStorageIngressMatrix) {

        Y_UNIT_TEST(VectorTest) {
            TVectorType vec(0, 7);
            vec.Set(0);
            vec.Set(4);
            vec.Set(6);
            UNIT_ASSERT(vec.Get(0));
            UNIT_ASSERT(vec.Get(4));
            UNIT_ASSERT(vec.Get(6));
            UNIT_ASSERT(!vec.Get(1));
            UNIT_ASSERT(!vec.Get(2));
            UNIT_ASSERT(!vec.Get(3));
            UNIT_ASSERT(!vec.Get(5));
            vec.Clear(5);
            UNIT_ASSERT(!vec.Get(5));
            vec.Clear(6);
            UNIT_ASSERT(!vec.Get(6));

            TVectorType vec1_1(0xA6, 8);
            TVectorType vec1_2(0x8C, 8);
            TVectorType vec2_1(0xA6, 8);
            TVectorType vec2_2(0x8C, 8);
            vec1_1 |= vec1_2;
            vec2_2 |= vec2_1;
            UNIT_ASSERT(vec1_1 == vec2_2);
            TVectorType expectedResult(0xAE, 8);
            UNIT_ASSERT(vec1_1 == expectedResult);

            UNIT_ASSERT(TVectorType(0, 1).Empty());
            UNIT_ASSERT(!TVectorType(0x80, 1).Empty());

            UNIT_ASSERT(TVectorType(0x07, 5).Empty());
            UNIT_ASSERT(!TVectorType(0x37, 5).Empty());
        }

        Y_UNIT_TEST(VectorTestEmpty) {
            TVectorType vec(0, 7);
            UNIT_ASSERT(vec.Empty());
            vec.Set(0);
            UNIT_ASSERT(!vec.Empty());
            vec.Set(4);
            vec.Clear(0);
            UNIT_ASSERT(!vec.Empty());
            vec.Clear(4);
            UNIT_ASSERT(vec.Empty());
            vec.Set(6);
            UNIT_ASSERT(!vec.Empty());
        }

        Y_UNIT_TEST(VectorTestMinus) {
            TVectorType vec1(0, 7);
            vec1.Set(0);
            vec1.Set(4);
            vec1.Set(6);

            TVectorType vec2(0, 7);
            vec2.Set(1);
            vec2.Set(5);
            vec2.Set(6);

            TVectorType res(0, 7);
            res.Set(0);
            res.Set(4);
            UNIT_ASSERT(vec1 - vec2 == res);
        }

        Y_UNIT_TEST(VectorTestBitwiseAnd) {
            TVectorType vec1(0, 7);
            vec1.Set(0);
            vec1.Set(4);
            vec1.Set(6);

            TVectorType vec2(0, 7);
            vec2.Set(1);
            vec2.Set(5);
            vec2.Set(6);

            TVectorType res(0, 7);
            res.Set(6);
            UNIT_ASSERT((vec1 & vec2) == res);
        }

        Y_UNIT_TEST(VectorTestBitwiseComplement1) {
            TVectorType vec1(0, 4);
            TVectorType vec2(~vec1);
            TVectorType vec3(0, 4);
            vec3.Set(0);
            vec3.Set(1);
            vec3.Set(2);
            vec3.Set(3);

            UNIT_ASSERT(vec2 == vec3);
        }

        Y_UNIT_TEST(VectorTestBitwiseComplement2) {
            TVectorType vec1(0, 5);
            vec1.Set(2);
            vec1.Set(4);
            TVectorType vec2(~vec1);
            TVectorType vec3(0, 5);
            vec3.Set(0);
            vec3.Set(1);
            vec3.Set(3);

            UNIT_ASSERT(vec2 == vec3);
        }

        Y_UNIT_TEST(VectorTestBitsBefore1) {
            TVectorType vec1(0, 7);
            vec1.Set(0);
            vec1.Set(4);
            vec1.Set(6);

            TVector<ui8> res;
            for (unsigned i = 0; i < vec1.GetSize(); i++)
                res.push_back(vec1.BitsBefore(i));

            TVector<ui8> canon;
            canon.push_back(0);
            canon.push_back(1);
            canon.push_back(1);
            canon.push_back(1);
            canon.push_back(1);
            canon.push_back(2);
            canon.push_back(2);

            UNIT_ASSERT(res == canon);
        }

        Y_UNIT_TEST(VectorTestBitsBefore2) {
            TVectorType vec1(0, 4);
            vec1.Set(1);
            vec1.Set(2);

            TVector<ui8> res;
            for (unsigned i = 0; i < vec1.GetSize(); i++)
                res.push_back(vec1.BitsBefore(i));

            TVector<ui8> canon;
            canon.push_back(0);
            canon.push_back(0);
            canon.push_back(1);
            canon.push_back(2);

            UNIT_ASSERT(res == canon);
        }

        Y_UNIT_TEST(VectorTestIterator1) {
            TVectorType vec1(0, 7);
            vec1.Set(1);
            vec1.Set(4);
            vec1.Set(6);

            TVector<ui8> res;
            for (ui8 i = vec1.FirstPosition(); i != vec1.GetSize(); i = vec1.NextPosition(i)) {
                res.push_back(i);
            }

            TVector<ui8> canon;
            canon.push_back(1);
            canon.push_back(4);
            canon.push_back(6);
            UNIT_ASSERT(res == canon);
        }

        Y_UNIT_TEST(VectorTestIterator2) {
            TVectorType vec1(0, 7);
            vec1.Set(0);
            vec1.Set(1);
            vec1.Set(5);

            TVector<ui8> res;
            for (ui8 i = vec1.FirstPosition(); i != vec1.GetSize(); i = vec1.NextPosition(i))
                res.push_back(i);

            TVector<ui8> canon;
            canon.push_back(0);
            canon.push_back(1);
            canon.push_back(5);
            UNIT_ASSERT(res == canon);
        }

        Y_UNIT_TEST(VectorTestIterator3) {
            TVectorType vec1(0, 6);
            vec1.Set(0);

            TVector<ui8> res;
            for (ui8 i = vec1.FirstPosition(); i != vec1.GetSize(); i = vec1.NextPosition(i)) {
                res.push_back(i);
            }

            TVector<ui8> canon;
            canon.push_back(0);
            UNIT_ASSERT(res == canon);
        }

        Y_UNIT_TEST(MatrixTest) {
            ui8 data[7];
            TMatrix m(data, 8, 7);
            m.Zero();
            for (ui8 i = 0; i < 7; i++)
                for (ui8 j = 0; j < 7; j++) {
                    UNIT_ASSERT(!m.Get(i, j));
                }

            m.Set(0, 0);
            m.Set(1, 1);
            m.Set(3, 3);
            m.Set(4, 4);
            m.Set(5, 5);
            m.Set(6, 6);
            m.Set(7, 2);

            UNIT_ASSERT(m.Get(0, 0));
            UNIT_ASSERT(m.Get(1, 1));
            UNIT_ASSERT(m.Get(3, 3));
            UNIT_ASSERT(m.Get(4, 4));
            UNIT_ASSERT(m.Get(5, 5));
            UNIT_ASSERT(m.Get(6, 6));
            UNIT_ASSERT(m.Get(7, 2));

            TVectorType vecRows = m.OrRows();
            TVectorType vecRowsExpected(0xFE, 7);
            UNIT_ASSERT(vecRows == vecRowsExpected);

            TVectorType vecColumns = m.OrColumns();
            TVectorType vecColumnsExpected(0xDF, 8);
            UNIT_ASSERT(vecColumns == vecColumnsExpected);

            //m.DebugPrint();

            UNIT_ASSERT(m.GetRow(0) == TVectorType(0x80, 7));
            UNIT_ASSERT(m.GetRow(1) == TVectorType(0x40, 7));
            UNIT_ASSERT(m.GetRow(2) == TVectorType(0, 7));
            UNIT_ASSERT(m.GetRow(3) == TVectorType(0x10, 7));
            UNIT_ASSERT(m.GetRow(4) == TVectorType(0x8, 7));
            UNIT_ASSERT(m.GetRow(5) == TVectorType(0x4, 7));
            UNIT_ASSERT(m.GetRow(6) == TVectorType(0x2, 7));
            UNIT_ASSERT(m.GetRow(7) == TVectorType(0x20, 7));

            UNIT_ASSERT(m.GetColumn(0) == TVectorType(0x80, 8));
            UNIT_ASSERT(m.GetColumn(2) == TVectorType(0x01, 8));
        }

        void DebugPrint(const TVector<std::pair<TVDiskID, TActorId> > &vec) {
            for (unsigned i = 0; i < vec.size(); i++) {
                fprintf(stderr, "%s\n", vec[i].first.ToString().data());
            }
        }


        Y_UNIT_TEST(ShiftedBitVecBase) {
            ui64 data = 0;
            TShiftedBitVecBase vec((ui8 *)&data, 3, 15); // i.e. size=12
            // test set
            vec.Set(0);
            vec.Set(4);
            vec.Set(6);
            vec.Set(9);
            vec.Set(11);
            UNIT_ASSERT(vec.Get(0));
            UNIT_ASSERT(!vec.Get(1));
            UNIT_ASSERT(!vec.Get(2));
            UNIT_ASSERT(!vec.Get(3));
            UNIT_ASSERT(vec.Get(4));
            UNIT_ASSERT(!vec.Get(5));
            UNIT_ASSERT(vec.Get(6));
            UNIT_ASSERT(!vec.Get(7));
            UNIT_ASSERT(!vec.Get(8));
            UNIT_ASSERT(vec.Get(9));
            UNIT_ASSERT(!vec.Get(10));
            UNIT_ASSERT(vec.Get(11));
            // test clear
            vec.Set(1);
            vec.Set(3);
            vec.Set(5);
            vec.Set(8);
            vec.Set(10);
            vec.Clear(0);
            vec.Clear(4);
            vec.Clear(6);
            vec.Clear(9);
            vec.Clear(11);
            UNIT_ASSERT(!vec.Get(0));
            UNIT_ASSERT(vec.Get(1));
            UNIT_ASSERT(!vec.Get(2));
            UNIT_ASSERT(vec.Get(3));
            UNIT_ASSERT(!vec.Get(4));
            UNIT_ASSERT(vec.Get(5));
            UNIT_ASSERT(!vec.Get(6));
            UNIT_ASSERT(!vec.Get(7));
            UNIT_ASSERT(vec.Get(8));
            UNIT_ASSERT(!vec.Get(9));
            UNIT_ASSERT(vec.Get(10));
            UNIT_ASSERT(!vec.Get(11));
            // test iterator
            ui16 dd = 0;
            TShiftedBitVecBase::TIterator it = vec.Begin();
            while (!it.IsEnd()) {
                dd <<= 1;
                dd |= ui8(it.Get());
                it.Next();
            }
            UNIT_ASSERT(dd == 0x54A);
        }

        Y_UNIT_TEST(ShiftedMainBitVec) {
            ui64 data = 0;
            TShiftedMainBitVec vec((ui8*)&data, 7, 14);
            vec.Set(0);
            vec.Set(2);
            vec.Set(4);
            vec.Set(5);
            UNIT_ASSERT(vec.ToVector() == TVectorType(0xAC, 7));
        }

        Y_UNIT_TEST(ShiftedHandoffBitVec) {
            ui64 data = 0;
            TShiftedHandoffBitVec vec((ui8*)&data, 7, 21);
            vec.Set(0);
            vec.Set(2);
            vec.Set(4);
            vec.Set(5);
            UNIT_ASSERT(vec.ToVector() == TVectorType(0xAC, 7));
            vec.Delete(2);
            vec.Delete(5);
            UNIT_ASSERT(vec.ToVector() == TVectorType(0x88, 7));
        }
    }

} // NKikimr
