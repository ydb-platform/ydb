#include "levenshtein_diff.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/string.h>

namespace {

    float unaryZeroWeigher(const char&) {
        return 0.0f;
    }

    float unaryMaxWeigher(const char&) {
        return 1.0f;
    }

    float binaryZeroWeigher(const char&, const char&) {
        return 0.0f;
    }

    float binaryMaxWeigher(const char&, const char&) {
        return 1.0f;
    }

}

Y_UNIT_TEST_SUITE(Levenstein) {
    Y_UNIT_TEST(Distance) {
        UNIT_ASSERT_VALUES_EQUAL(NLevenshtein::Distance(TStringBuf("knight"), TStringBuf("king")), 4);
        UNIT_ASSERT_VALUES_EQUAL(NLevenshtein::Distance(TStringBuf("life flies"), TStringBuf("fly lives")), 6);
        UNIT_ASSERT_VALUES_EQUAL(NLevenshtein::Distance(TStringBuf("spot trail"), TStringBuf("stop trial")), 4);
        UNIT_ASSERT_VALUES_EQUAL(NLevenshtein::Distance(TStringBuf("hello"), TStringBuf("hulloah")), 3);
        UNIT_ASSERT_VALUES_EQUAL(NLevenshtein::Distance(TStringBuf("yeoman"), TStringBuf("yo man")), 2);
    }

    Y_UNIT_TEST(DamerauDistance) {
        UNIT_ASSERT_VALUES_EQUAL(NLevenshtein::DamerauDistance(TStringBuf("knight"), TStringBuf("king")), 3);
        UNIT_ASSERT_VALUES_EQUAL(NLevenshtein::DamerauDistance(TStringBuf("life flies"), TStringBuf("fly lives")), 6);
        UNIT_ASSERT_VALUES_EQUAL(NLevenshtein::DamerauDistance(TStringBuf("spot trail"), TStringBuf("stop trial")), 3);
        UNIT_ASSERT_VALUES_EQUAL(NLevenshtein::DamerauDistance(TStringBuf("status"), TStringBuf("tsatus")), 1);
        UNIT_ASSERT_VALUES_EQUAL(NLevenshtein::DamerauDistance(TStringBuf("hello"), TStringBuf("heh lol")), 3);
    }
}

Y_UNIT_TEST_SUITE(WeightedLevenstein) {
    Y_UNIT_TEST(EqualStrings) {
        NLevenshtein::TEditChain chain;
        float distance = 0.0f;
        NLevenshtein::GetEditChain(TString("12345"), TString("12345"), chain, &distance, binaryMaxWeigher, unaryMaxWeigher, unaryMaxWeigher);
        UNIT_ASSERT_VALUES_EQUAL(distance, 0.0f);
        UNIT_ASSERT_VALUES_EQUAL(chain.size(), 5);
    }

    Y_UNIT_TEST(EmptyStrings) {
        NLevenshtein::TEditChain chain;
        float distance = 0.0f;
        NLevenshtein::GetEditChain(TString(""), TString(""), chain, &distance, binaryMaxWeigher, unaryMaxWeigher, unaryMaxWeigher);
        UNIT_ASSERT_VALUES_EQUAL(distance, 0.0f);
        UNIT_ASSERT_VALUES_EQUAL(chain.size(), 0);
    }

    Y_UNIT_TEST(InsertsOnly) {
        auto unaryWeigher = [](const char&) {
            return 2.0f;
        };
        NLevenshtein::TEditChain chain;
        float distance = 0.0f;
        NLevenshtein::GetEditChain(TString(""), TString("12345"), chain, &distance, binaryZeroWeigher, unaryZeroWeigher, unaryWeigher);
        UNIT_ASSERT_VALUES_EQUAL(distance, 10.0f);
        UNIT_ASSERT_VALUES_EQUAL(chain.size(), 5);
    }

    Y_UNIT_TEST(DeletionsOnly) {
        auto unaryWeigher = [](const char&) {
            return 3.0f;
        };
        NLevenshtein::TEditChain chain;
        float distance = 0.0f;
        NLevenshtein::GetEditChain(TString("54321"), TString(""), chain, &distance, binaryZeroWeigher, unaryWeigher, unaryZeroWeigher);
        UNIT_ASSERT_VALUES_EQUAL(distance, 15.0f);
        UNIT_ASSERT_VALUES_EQUAL(chain.size(), 5);
    }

    Y_UNIT_TEST(SymmetryCheck) {
        const TString str1 = "123x5";
        const TString str2 = "x2345";
        const float trgDistance = 2.0f;
        const size_t trgChainLen = 5;

        NLevenshtein::TEditChain chainLeftRight;
        float distanceLeftRight = 0.0f;
        NLevenshtein::GetEditChain(str1, str2, chainLeftRight, &distanceLeftRight, binaryMaxWeigher, unaryMaxWeigher, unaryMaxWeigher);
        UNIT_ASSERT_VALUES_EQUAL(distanceLeftRight, trgDistance);
        UNIT_ASSERT_VALUES_EQUAL(chainLeftRight.size(), trgChainLen);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(chainLeftRight[0]), static_cast<int>(NLevenshtein::EMT_REPLACE));
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(chainLeftRight[1]), static_cast<int>(NLevenshtein::EMT_PRESERVE));
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(chainLeftRight[2]), static_cast<int>(NLevenshtein::EMT_PRESERVE));
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(chainLeftRight[3]), static_cast<int>(NLevenshtein::EMT_REPLACE));
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(chainLeftRight[4]), static_cast<int>(NLevenshtein::EMT_PRESERVE));

        NLevenshtein::TEditChain chainRightLeft;
        float distanceRightLeft = 0.0f;
        NLevenshtein::GetEditChain(str2, str1, chainRightLeft, &distanceRightLeft, binaryMaxWeigher, unaryMaxWeigher, unaryMaxWeigher);
        UNIT_ASSERT_VALUES_EQUAL(distanceRightLeft, trgDistance);
        UNIT_ASSERT_VALUES_EQUAL(chainRightLeft.size(), trgChainLen);
        UNIT_ASSERT(chainRightLeft == chainLeftRight);
    }

    Y_UNIT_TEST(PreferReplacements) {
        auto binaryWeigher = [](const char&, const char&) {
            return 0.0625f;
        };
        NLevenshtein::TEditChain chain;
        float distance = 0.0f;
        NLevenshtein::GetEditChain(TString("54321"), TString("43210"), chain, &distance, binaryWeigher, unaryMaxWeigher, unaryMaxWeigher);
        UNIT_ASSERT_VALUES_EQUAL(distance, 0.3125f);
        UNIT_ASSERT_VALUES_EQUAL(chain.size(), 5);
    }

    Y_UNIT_TEST(PreferInsertDeletions) {
        auto unaryWeigher = [](const char&) {
            return 0.0625f;
        };
        NLevenshtein::TEditChain chain;
        float distance = 0.0f;
        NLevenshtein::GetEditChain(TString("54321"), TString("98765"), chain, &distance, binaryMaxWeigher, unaryWeigher, unaryWeigher);
        UNIT_ASSERT_VALUES_EQUAL(distance, 0.5f);
        UNIT_ASSERT_VALUES_EQUAL(chain.size(), 9);
    }

    Y_UNIT_TEST(NoXDeletions) {
        auto unaryWeigher = [](const char& c) {
            return c == 'x' ? 100.0f : 1.0f;
        };
        NLevenshtein::TEditChain chain;
        float distance = 0.0f;
        NLevenshtein::GetEditChain(TString("543x1"), TString("5431"), chain, &distance, binaryMaxWeigher, unaryWeigher, unaryMaxWeigher);
        UNIT_ASSERT_VALUES_EQUAL(chain.size(), 5);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(chain[3]), static_cast<int>(NLevenshtein::EMT_REPLACE));
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(chain[4]), static_cast<int>(NLevenshtein::EMT_DELETE));
        UNIT_ASSERT_VALUES_EQUAL(distance, 2.0f);
    }

    Y_UNIT_TEST(NoXInsertions) {
        auto unaryWeigher = [](const char& c) {
            return c == 'x' ? 100.0f : 1.0f;
        };
        NLevenshtein::TEditChain chain;
        float distance = 0.0f;
        NLevenshtein::GetEditChain(TString("5431"), TString("543x1"), chain, &distance, binaryMaxWeigher, unaryMaxWeigher, unaryWeigher);
        UNIT_ASSERT_VALUES_EQUAL(chain.size(), 5);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(chain[3]), static_cast<int>(NLevenshtein::EMT_REPLACE));
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(chain[4]), static_cast<int>(NLevenshtein::EMT_INSERT));
        UNIT_ASSERT_VALUES_EQUAL(distance, 2.0f);
    }

    Y_UNIT_TEST(NoReplacementsOfX) {
        auto binaryWeigher = [](const char& l, const char&) {
            return l == 'x' ? 100.0f : 1.0f;
        };
        NLevenshtein::TEditChain chain;
        float distance = 0.0f;
        NLevenshtein::GetEditChain(TString("5432x"), TString("5432y"), chain, &distance, binaryWeigher, unaryMaxWeigher, unaryMaxWeigher);
        UNIT_ASSERT_VALUES_EQUAL(chain.size(), 6);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(chain[4]), static_cast<int>(NLevenshtein::EMT_DELETE));
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(chain[5]), static_cast<int>(NLevenshtein::EMT_INSERT));
        UNIT_ASSERT_VALUES_EQUAL(distance, 2.0f);
    }

    Y_UNIT_TEST(NoReplacementsForX) {
        auto binaryWeigher = [](const char&, const char& r) {
            return r == 'x' ? 100.0f : 1.0f;
        };
        NLevenshtein::TEditChain chain;
        float distance = 0.0f;
        NLevenshtein::GetEditChain(TString("y4321"), TString("x4321"), chain, &distance, binaryWeigher, unaryMaxWeigher, unaryMaxWeigher);
        UNIT_ASSERT_VALUES_EQUAL(chain.size(), 6);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(chain[0]), static_cast<int>(NLevenshtein::EMT_DELETE));
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(chain[1]), static_cast<int>(NLevenshtein::EMT_INSERT));
        UNIT_ASSERT_VALUES_EQUAL(distance, 2.0f);
    }

    Y_UNIT_TEST(SimilarOperationPriorities) {
        auto replaceWeigher = [](const char&, const char&) {
            return 0.5f;
        };
        auto deleteWeigher = [](const char&) {
            return 0.2f;
        };
        auto insertWeigher = [](const char&) {
            return 0.9f;
        };
        NLevenshtein::TEditChain chain;
        float distance = 0.0f;
        NLevenshtein::GetEditChain(TString("y0"), TString("0x"), chain, &distance, replaceWeigher, deleteWeigher, insertWeigher);
        UNIT_ASSERT_VALUES_EQUAL(chain.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(chain[0]), static_cast<int>(NLevenshtein::EMT_REPLACE));
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(chain[1]), static_cast<int>(NLevenshtein::EMT_REPLACE));
        UNIT_ASSERT_VALUES_EQUAL(distance, 1.0f);
    }

    Y_UNIT_TEST(DamerauLevenshtein) {
        int distance;
        NLevenshtein::TEditChain chain;
        NLevenshtein::GetEditChainGeneric<TString, true>(TString("status"), TString("tsatus"), chain, &distance);
        UNIT_ASSERT_VALUES_EQUAL(distance, 1);
        UNIT_ASSERT_VALUES_EQUAL(chain.size(), 5);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(chain[0]), static_cast<int>(NLevenshtein::EMT_TRANSPOSE));
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(chain[1]), static_cast<int>(NLevenshtein::EMT_PRESERVE));

        NLevenshtein::GetEditChainGeneric<TString, true>(TString("hello"), TString("heh lol"), chain, &distance);
        UNIT_ASSERT_VALUES_EQUAL(distance, 3);
        UNIT_ASSERT_VALUES_EQUAL(chain.size(), 6);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(chain[0]), static_cast<int>(NLevenshtein::EMT_PRESERVE));
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(chain[1]), static_cast<int>(NLevenshtein::EMT_PRESERVE));
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(chain[2]), static_cast<int>(NLevenshtein::EMT_INSERT));
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(chain[3]), static_cast<int>(NLevenshtein::EMT_INSERT));
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(chain[4]), static_cast<int>(NLevenshtein::EMT_PRESERVE));
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(chain[5]), static_cast<int>(NLevenshtein::EMT_TRANSPOSE));
    }
}
