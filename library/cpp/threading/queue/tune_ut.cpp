#include <library/cpp/testing/unittest/registar.h>
#include "tune.h"

struct TDefaultStructA {
};

struct TDefaultStructB {
};

struct TDefaults {
    using TStructA = TDefaultStructA;
    using TStructB = TDefaultStructB;
    static constexpr ui32 Param1 = 42;
    static constexpr ui32 Param2 = 42;
};

DeclareTuneTypeParam(TweakStructA, TStructA);
DeclareTuneTypeParam(TweakStructB, TStructB);
DeclareTuneValueParam(TweakParam1, ui32, Param1);
DeclareTuneValueParam(TweakParam2, ui32, Param2);

Y_UNIT_TEST_SUITE(TestTuning) {
    Y_UNIT_TEST(Defaults) {
        using TTuned = TTune<TDefaults>;
        using TunedA = TTuned::TStructA;
        using TunedB = TTuned::TStructB;
        auto sameA = std::is_same<TDefaultStructA, TunedA>::value;
        auto sameB = std::is_same<TDefaultStructB, TunedB>::value;
        auto param1 = TTuned::Param1;
        auto param2 = TTuned::Param2;

        UNIT_ASSERT(sameA);
        UNIT_ASSERT(sameB);
        UNIT_ASSERT_EQUAL(param1, 42);
        UNIT_ASSERT_EQUAL(param2, 42);
    }

    Y_UNIT_TEST(TuneStructA) {
        struct TMyStruct {
        };

        using TTuned = TTune<TDefaults, TweakStructA<TMyStruct>>;

        using TunedA = TTuned::TStructA;
        using TunedB = TTuned::TStructB;
        //auto sameA = std::is_same<TDefaultStructA, TunedA>::value;
        auto sameB = std::is_same<TDefaultStructB, TunedB>::value;
        auto param1 = TTuned::Param1;
        auto param2 = TTuned::Param2;

        auto sameA = std::is_same<TMyStruct, TunedA>::value;

        UNIT_ASSERT(sameA);
        UNIT_ASSERT(sameB);
        UNIT_ASSERT_EQUAL(param1, 42);
        UNIT_ASSERT_EQUAL(param2, 42);
    }

    Y_UNIT_TEST(TuneParam1) {
        using TTuned = TTune<TDefaults, TweakParam1<24>>;

        using TunedA = TTuned::TStructA;
        using TunedB = TTuned::TStructB;
        auto sameA = std::is_same<TDefaultStructA, TunedA>::value;
        auto sameB = std::is_same<TDefaultStructB, TunedB>::value;
        auto param1 = TTuned::Param1;
        auto param2 = TTuned::Param2;

        UNIT_ASSERT(sameA);
        UNIT_ASSERT(sameB);
        UNIT_ASSERT_EQUAL(param1, 24);
        UNIT_ASSERT_EQUAL(param2, 42);
    }

    Y_UNIT_TEST(TuneStructAAndParam1) {
        struct TMyStruct {
        };

        using TTuned =
            TTune<TDefaults, TweakStructA<TMyStruct>, TweakParam1<24>>;

        using TunedA = TTuned::TStructA;
        using TunedB = TTuned::TStructB;
        //auto sameA = std::is_same<TDefaultStructA, TunedA>::value;
        auto sameB = std::is_same<TDefaultStructB, TunedB>::value;
        auto param1 = TTuned::Param1;
        auto param2 = TTuned::Param2;

        auto sameA = std::is_same<TMyStruct, TunedA>::value;

        UNIT_ASSERT(sameA);
        UNIT_ASSERT(sameB);
        UNIT_ASSERT_EQUAL(param1, 24);
        UNIT_ASSERT_EQUAL(param2, 42);
    }

    Y_UNIT_TEST(TuneParam1AndStructA) {
        struct TMyStruct {
        };

        using TTuned =
            TTune<TDefaults, TweakParam1<24>, TweakStructA<TMyStruct>>;

        using TunedA = TTuned::TStructA;
        using TunedB = TTuned::TStructB;
        //auto sameA = std::is_same<TDefaultStructA, TunedA>::value;
        auto sameB = std::is_same<TDefaultStructB, TunedB>::value;
        auto param1 = TTuned::Param1;
        auto param2 = TTuned::Param2;

        auto sameA = std::is_same<TMyStruct, TunedA>::value;

        UNIT_ASSERT(sameA);
        UNIT_ASSERT(sameB);
        UNIT_ASSERT_EQUAL(param1, 24);
        UNIT_ASSERT_EQUAL(param2, 42);
    }
}
