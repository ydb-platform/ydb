#include <library/cpp/regex/hyperscan/hyperscan.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/set.h>

#include <array>
#include <algorithm>

Y_UNIT_TEST_SUITE(HyperscanWrappers) {
    using namespace NHyperscan;
    using namespace NHyperscan::NPrivate;

    Y_UNIT_TEST(CompileAndScan) {
        TDatabase db = Compile("a.c", HS_FLAG_DOTALL | HS_FLAG_SINGLEMATCH);
        TScratch scratch = MakeScratch(db);

        unsigned int foundId = 42;
        auto callback = [&](unsigned int id, unsigned long long /* from */, unsigned long long /* to */) {
            foundId = id;
        };
        NHyperscan::Scan(
            db,
            scratch,
            "abc",
            callback);
        UNIT_ASSERT_EQUAL(foundId, 0);
    }

    Y_UNIT_TEST(Matches) {
        NHyperscan::TDatabase db = NHyperscan::Compile(
            "a.c",
            HS_FLAG_DOTALL | HS_FLAG_SINGLEMATCH);
        NHyperscan::TScratch scratch = NHyperscan::MakeScratch(db);
        UNIT_ASSERT(NHyperscan::Matches(db, scratch, "abc"));
        UNIT_ASSERT(!NHyperscan::Matches(db, scratch, "foo"));
    }

    Y_UNIT_TEST(Multi) {
        NHyperscan::TDatabase db = NHyperscan::CompileMulti(
            {
                "foo",
                "bar",
            },
            {
                HS_FLAG_DOTALL | HS_FLAG_SINGLEMATCH,
                HS_FLAG_DOTALL | HS_FLAG_SINGLEMATCH | HS_FLAG_CASELESS,
            },
            {
                42,
                241,
            });
        NHyperscan::TScratch scratch = NHyperscan::MakeScratch(db);

        UNIT_ASSERT(NHyperscan::Matches(db, scratch, "foo"));
        UNIT_ASSERT(NHyperscan::Matches(db, scratch, "bar"));
        UNIT_ASSERT(NHyperscan::Matches(db, scratch, "BAR"));
        UNIT_ASSERT(!NHyperscan::Matches(db, scratch, "FOO"));

        TSet<unsigned int> foundIds;
        auto callback = [&](unsigned int id, unsigned long long /* from */, unsigned long long /* to */) {
            foundIds.insert(id);
        };
        NHyperscan::Scan(
            db,
            scratch,
            "fooBaR",
            callback);
        UNIT_ASSERT_EQUAL(foundIds.size(), 2);
        UNIT_ASSERT(foundIds.contains(42));
        UNIT_ASSERT(foundIds.contains(241));
    }

    // https://ml.yandex-team.ru/thread/2370000002965712422/
    Y_UNIT_TEST(MultiRegression) {
        NHyperscan::CompileMulti(
            {
                "aa.bb/cc.dd",
            },
            {
                HS_FLAG_UTF8,
            },
            {
                0,
            });
    }

    Y_UNIT_TEST(Serialize) {
        NHyperscan::TDatabase db = NHyperscan::Compile(
            "foo",
            HS_FLAG_DOTALL | HS_FLAG_SINGLEMATCH);
        TString serialization = Serialize(db);
        db.Reset();
        TDatabase db2 = Deserialize(serialization);
        NHyperscan::TScratch scratch = NHyperscan::MakeScratch(db2);

        UNIT_ASSERT(NHyperscan::Matches(db2, scratch, "foo"));
        UNIT_ASSERT(!NHyperscan::Matches(db2, scratch, "FOO"));
    }

    Y_UNIT_TEST(GrowScratch) {
        NHyperscan::TDatabase db1 = NHyperscan::Compile(
            "foo",
            HS_FLAG_DOTALL | HS_FLAG_SINGLEMATCH);
        NHyperscan::TDatabase db2 = NHyperscan::Compile(
            "longer\\w\\w\\wpattern",
            HS_FLAG_DOTALL | HS_FLAG_SINGLEMATCH | HS_FLAG_UTF8);
        NHyperscan::TScratch scratch = NHyperscan::MakeScratch(db1);
        NHyperscan::GrowScratch(scratch, db2);
        UNIT_ASSERT(NHyperscan::Matches(db1, scratch, "foo"));
        UNIT_ASSERT(NHyperscan::Matches(db2, scratch, "longerWWWpattern"));
    }

    Y_UNIT_TEST(CloneScratch) {
        NHyperscan::TDatabase db = NHyperscan::Compile(
            "foo",
            HS_FLAG_DOTALL | HS_FLAG_SINGLEMATCH);
        NHyperscan::TScratch scratch1 = NHyperscan::MakeScratch(db);
        NHyperscan::TScratch scratch2 = NHyperscan::CloneScratch(scratch1);
        scratch1.Reset();
        UNIT_ASSERT(NHyperscan::Matches(db, scratch2, "foo"));
    }

    class TSimpleSingleRegex {
    public:
        static TDatabase Compile(TCPUFeatures cpuFeatures) {
            return NHyperscan::Compile("foo", HS_FLAG_DOTALL | HS_FLAG_SINGLEMATCH, cpuFeatures);
        }
        static void Check(const TDatabase& db, const NHyperscan::NPrivate::TImpl& impl) {
            NHyperscan::TScratch scratch = NHyperscan::MakeScratch(db);
            UNIT_ASSERT(NHyperscan::NPrivate::Matches(db, scratch, "foo", impl));
            UNIT_ASSERT(!NHyperscan::NPrivate::Matches(db, scratch, "FOO", impl));
        }
    };

    // This regex uses AVX2 instructions on long (>70) texts.
    // It crushes when compiled for machine with AVX2 and run on machine without it.
    class TAvx2SingleRegex {
        public:
        static TDatabase Compile(TCPUFeatures cpuFeatures) {
            auto regex = "[ЁАБВГДЕЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯабвгдежзийклмнопрстуфхцчшщъыьэюяё]+"
                         "[.][\\-ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz]{2,5}";
            unsigned int flags = HS_FLAG_UTF8 | HS_FLAG_DOTALL | HS_FLAG_SINGLEMATCH | HS_FLAG_ALLOWEMPTY;
            return NHyperscan::Compile(regex, flags, cpuFeatures);
        }
        static void Check(const TDatabase& db, const NHyperscan::NPrivate::TImpl& impl) {
            NHyperscan::TScratch scratch = NHyperscan::MakeScratch(db);
            UNIT_ASSERT(NHyperscan::NPrivate::Matches(
                db,
                scratch,
                "_________________________________________________________________"
                "фу.bar"
                "_________________________________________________________________",
                impl));
            UNIT_ASSERT(!NHyperscan::NPrivate::Matches(
                db,
                scratch,
                "_________________________________________________________________"
                "фу"
                "_________________________________________________________________",
                impl));
        }
    };

    class TSimpleMultiRegex {
    public:
        static TDatabase Compile(TCPUFeatures cpuFeatures) {
            return NHyperscan::CompileMulti(
                {
                    "foo",
                    "bar",
                },
                {
                    HS_FLAG_DOTALL | HS_FLAG_SINGLEMATCH,
                    HS_FLAG_DOTALL | HS_FLAG_SINGLEMATCH | HS_FLAG_CASELESS,
                },
                {
                    42,
                    241,
                },
                cpuFeatures);
        }
        static void Check(const TDatabase& db, const NHyperscan::NPrivate::TImpl& impl) {
            NHyperscan::TScratch scratch = NHyperscan::MakeScratch(db);

            UNIT_ASSERT(NHyperscan::NPrivate::Matches(db, scratch, "foo", impl));
            UNIT_ASSERT(NHyperscan::NPrivate::Matches(db, scratch, "bar", impl));
            UNIT_ASSERT(NHyperscan::NPrivate::Matches(db, scratch, "BAR", impl));
            UNIT_ASSERT(!NHyperscan::NPrivate::Matches(db, scratch, "FOO", impl));

            TSet<unsigned int> foundIds;
            auto callback = [&](unsigned int id, unsigned long long /* from */, unsigned long long /* to */) {
                foundIds.insert(id);
            };
            NHyperscan::NPrivate::Scan(
                    db,
                    scratch,
                    "fooBaR",
                    callback,
                    impl);
            UNIT_ASSERT_EQUAL(foundIds.size(), 2);
            UNIT_ASSERT(foundIds.contains(42));
            UNIT_ASSERT(foundIds.contains(241));
        }
    };

    template <class Regex>
    void TestCrossPlatformCompile() {
        const std::array<ERuntime, 4> runtimes = {
            ERuntime::Core2,
            ERuntime::Corei7,
            ERuntime::AVX2,
            ERuntime::AVX512
        };

        // Unfortunately, we cannot emulate runtimes with more capabilities than current machine.
        auto currentRuntimeIter = std::find(runtimes.cbegin(), runtimes.cend(), DetectCurrentRuntime());
        Y_ASSERT(currentRuntimeIter != runtimes.cend());

        for (auto targetRuntime = runtimes.cbegin(); targetRuntime <= currentRuntimeIter; ++targetRuntime) {
            auto db = Regex::Compile(RuntimeCpuFeatures(*targetRuntime));
            Regex::Check(db, NHyperscan::NPrivate::TImpl{*targetRuntime});
        }
    }

    Y_UNIT_TEST(CrossPlatformCompile) {
        TestCrossPlatformCompile<TSimpleSingleRegex>();
        TestCrossPlatformCompile<TAvx2SingleRegex>();
        TestCrossPlatformCompile<TSimpleMultiRegex>();
    }
}
