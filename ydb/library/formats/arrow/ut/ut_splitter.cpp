#include <ydb/library/actors/core/log.h>
#include <ydb/library/formats/arrow/splitter/similar_packer.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/string/join.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(SplitterTest) {
    Y_UNIT_TEST(SimpleNoFrac) {
        AFL_VERIFY(JoinSeq(",", NArrow::NSplitter::TSimilarPacker::SplitWithExpected(10, 1)) == "1,1,1,1,1,1,1,1,1,1");
        AFL_VERIFY(JoinSeq(",", NArrow::NSplitter::TSimilarPacker::SplitWithExpected(10, 5)) == "5,5");
        AFL_VERIFY(JoinSeq(",", NArrow::NSplitter::TSimilarPacker::SplitWithExpected(10, 2)) == "2,2,2,2,2");
        AFL_VERIFY(JoinSeq(",", NArrow::NSplitter::TSimilarPacker::SplitWithExpected(10, 10)) == "10");
        AFL_VERIFY(JoinSeq(",", NArrow::NSplitter::TSimilarPacker::SplitWithExpected(10, 100)) == "10");
    }
    Y_UNIT_TEST(Simple) {
        Cerr << JoinSeq(",", NArrow::NSplitter::TSimilarPacker::SplitWithExpected(10, 3)) << Endl;
        AFL_VERIFY(JoinSeq(",", NArrow::NSplitter::TSimilarPacker::SplitWithExpected(10, 3)) == "3,3,4");
    }
    Y_UNIT_TEST(SimpleBig) {
        Cerr << JoinSeq(",", NArrow::NSplitter::TSimilarPacker::SplitWithExpected(100000, 30000)) << Endl;
        AFL_VERIFY(JoinSeq(",", NArrow::NSplitter::TSimilarPacker::SplitWithExpected(100000, 30000)) == "33333,33333,33334");
        const auto result = NArrow::NSplitter::TSimilarPacker::SplitWithExpected(143237, 10000);
        Cerr << JoinSeq(",", result) << Endl;
        for (auto&& i : result) {
            AFL_VERIFY(10231 <= i && i <= 10232);
        }
    }
    Y_UNIT_TEST(Generic) {
        for (ui32 i = 0; i < 1000000; ++i) {
            for (auto&& c : { 10000, 13341, 17000, 231233 }) {
                auto values = NArrow::NSplitter::TSimilarPacker::SplitWithExpected(i, c);
                ui32 min = values.front();
                ui32 max = values.front();
                ui32 sum = 0;
                for (auto&& c : values) {
                    if (c < min) {
                        min = c;
                    }
                    if (max < c) {
                        max = c;
                    }
                    sum += c;
                }
                AFL_VERIFY(sum == i);
                AFL_VERIFY(max - min <= 1);
            }
        }
    }
}

}   // namespace NKikimr
