#include "huff.h"
#include "metainfo.h"
#include "bit.h"

#include <util/generic/vector.h>
#include <util/generic/map.h>
#include <util/system/protect.h>

#include <library/cpp/testing/unittest/registar.h>

static ui64 gSeed = 42;

static void FlushPseudoRandom() {
    gSeed = 42;
}

static ui32 PseudoRandom(ui32 max) {
    // stupid and non-threadsafe, but very predictable chaos generator
    gSeed += 1;
    gSeed *= 419;
    gSeed = gSeed ^ (ui64(max) << 17);
    return gSeed % max;
}

enum ECompMode {
    CM_SINGLEPASS,
    CM_TWOPASS
};

struct TTestParams {
    size_t DataSize;
    ui32 ValueArraySize;
};

template <typename X>
void TestSaveLoadMeta(NCompProto::TMetaInfo<X>& src) {
    TStringStream ss;
    src.Save(ss);
    TString data = ss.Str();
    NCompProto::TMetaInfo<X> loadedMeta(data);
    ss = TStringStream();
    loadedMeta.Save(ss);
    UNIT_ASSERT_EQUAL(ss.Str(), data);
}

template <typename TDecompressor, template <typename, typename> class TSerialize>
void TestWithParams(const TString& metainfo, const ECompMode mode, const TTestParams& params) {
    using namespace NCompProto;
    FlushPseudoRandom();

    TStringInput stream(metainfo);

    THolder<TMetaInfo<THuff>> meta;
    if (mode == CM_TWOPASS) {
        TMetaInfo<THist> hist(stream);
        TEmpty empty;
        TSerialize<THist, TEmpty>::Serialize(hist, empty, params);
        meta.Reset(new TMetaInfo<THuff>(hist, THistToHuff::Instance()));
    } else {
        meta.Reset(new TMetaInfo<THuff>(stream));
    }
    TestSaveLoadMeta(*meta.Get());

    TBitBuffer buffer;
    TSerialize<THuff, TBitBuffer>::Serialize(*meta, buffer, params);

    ui64 codedSize = buffer.Position;

    TMetaInfo<TTable> decompressor(*meta, THuffToTable::Instance());

    // verify that no memory read beyond buffer occurs
    const size_t byteSize = buffer.ByteLength();
    const size_t PAGESIZEX = 4096;
    const size_t busyPages = (byteSize + (PAGESIZEX - 1)) / PAGESIZEX;
    const size_t allPages = busyPages + 1;
    const size_t allocSize = (allPages + 1) * PAGESIZEX;
    TVector<ui8> readBuffer(allocSize);
    ui8* start = &readBuffer[0];
    ui8* pageStart = reinterpret_cast<ui8*>((size_t(start) + PAGESIZEX) & ~(PAGESIZEX - 1));
    // XX DATA  DATA  DATA DATA PROT
    //      |     |     |     |     | pages
    // calculate dataStart so that data ends exactly at the page end
    ui8* dataStart = pageStart + busyPages * PAGESIZEX - byteSize;
    ui8* dataEnd = pageStart + busyPages * PAGESIZEX;
    ProtectMemory(dataEnd, PAGESIZEX, PM_NONE);
    // memory copying should be performed without any problems
    memcpy(dataStart, buffer.Out.data(), byteSize);

    ui64 position = 0;
    TMetaIterator<TDecompressor> instance;
    // we should not read beyond dataEnd here
    instance.Decompress(&decompressor, dataStart, position);
    const ui64 decodedSize = position;
    UNIT_ASSERT_EQUAL(codedSize, decodedSize);
    // unprotect memory
    ProtectMemory(dataEnd, PAGESIZEX, PM_READ | PM_WRITE | PM_EXEC);
}

template <typename TDecompressor, template <typename, typename> class TSerialize>
void Test(const TString& metainfo, const ECompMode mode) {
    for (size_t ds = 3; ds < 42; ds += (3 + PseudoRandom(5))) {
        for (size_t vas = 5; vas < 42; vas += (4 + PseudoRandom(10))) {
            TTestParams params;
            params.DataSize = ds;
            params.ValueArraySize = vas;
            TestWithParams<TDecompressor, TSerialize>(metainfo, mode, params);
        }
    }
}

Y_UNIT_TEST_SUITE(CompProtoTestBasic) {
    using namespace NCompProto;

    const TString metainfo =
        "\n\
    repeated data id 0\n\
        scalar clicks id 0 default const 0\n\
        scalar shows id 1 default const 0\n\
        repeated regClicks id 2\n\
            scalar clicks id 0 default const 0\n\
            scalar shows id 1 default const 0\n\
        end\n\
        scalar extra id 31 default const 0\n\
    end\n";

    struct TRegInfo {
        ui32 Clicks;
        ui32 Shows;
    };

    struct TData {
        ui32 Clicks;
        ui32 Shows;
        ui32 Extra;
        TMap<ui32, TRegInfo> RegClicks;
    };

    TVector<TData> data;

    template <class TMeta, class TFunctor>
    struct TSerialize {
        static void Serialize(TMetaInfo<TMeta>& meta, TFunctor& functor, const TTestParams& params) {
            FlushPseudoRandom();
            meta.BeginSelf(functor);
            data.clear();
            data.resize(params.DataSize);
            for (ui32 i = 0; i < params.DataSize; ++i) {
                meta.BeginElement(i, functor);

                data[i].Clicks = PseudoRandom(16) + 100;
                data[i].Shows = PseudoRandom(500) * PseudoRandom(16);
                data[i].Extra = PseudoRandom(500) + (1UL << 31); // test also saving of big values
                meta.SetScalar(0, data[i].Clicks, functor);
                meta.SetScalar(1, data[i].Shows, functor);

                TMetaInfo<TMeta>& regClicks = meta.BeginRepeated(2, functor);
                for (ui32 j = 0; j < PseudoRandom(200); j += 1 + PseudoRandom(10)) {
                    regClicks.BeginElement(j, functor);
                    TRegInfo& r = data[i].RegClicks[j];

                    r.Clicks = PseudoRandom(2);
                    r.Shows = PseudoRandom(800) * PseudoRandom(8) + 56;
                    regClicks.SetScalar(0, r.Clicks, functor);
                    regClicks.SetScalar(1, r.Shows, functor);

                    regClicks.EndElement(functor);
                }
                regClicks.EndRepeated(functor);
                meta.SetScalar(31, data[i].Extra, functor);
                meta.EndElement(functor);
            }
            meta.EndRepeated(functor);
        }
    };

    struct TMultiDecompressor: public TParentHold<TMultiDecompressor> {
        struct TRegClicks: public TParentHold<TRegClicks> {
            const TData* Data;
            const TRegInfo* Elem;
            TRegClicks()
                : Data(nullptr)
                , Elem(nullptr)
            {
            }
            void BeginSelf(ui32 /*count*/, ui32 /*id*/) {
            }
            void EndSelf() {
            }
            void BeginElement(ui32 element) {
                TMap<ui32, TRegInfo>::const_iterator it = Data->RegClicks.find(element);
                if (it == Data->RegClicks.end()) {
                    UNIT_ASSERT(0);
                }
                Elem = &it->second;
            }
            void EndElement() {
            }
            void SetScalar(size_t index, ui32 val) {
                if (index == 0)
                    UNIT_ASSERT_EQUAL(val, Elem->Clicks);
                if (index == 1)
                    UNIT_ASSERT_EQUAL(val, Elem->Shows);
            }
            IDecompressor& GetDecompressor(size_t) {
                UNIT_ASSERT(0);
                return GetEmptyDecompressor();
            }
        };

        const TData* Elem;
        TMetaIterator<TRegClicks> RegClicks;
        void BeginSelf(ui32 /*count*/, ui32 /*id*/) {
        }
        void EndSelf() {
        }
        void BeginElement(ui32 element) {
            UNIT_ASSERT(element < data.size());
            Elem = &data[element];
        }
        void EndElement() {
        }
        void SetScalar(size_t index, ui32 val) {
            if (index == 0)
                UNIT_ASSERT_EQUAL(val, Elem->Clicks);
            if (index == 1)
                UNIT_ASSERT_EQUAL(val, Elem->Shows);
            if (index == 31)
                UNIT_ASSERT_EQUAL(val, Elem->Extra);
        }
        IDecompressor& GetDecompressor(size_t index) {
            if (index == 2) {
                RegClicks.Self.Data = Elem;
                return RegClicks;
            }
            UNIT_ASSERT(0);
            return GetEmptyDecompressor();
        }
        TMultiDecompressor()
            : Elem(nullptr)
        {
        }
    };

    struct TVerifyingDecompressor: public TParentHold<TVerifyingDecompressor> {
        enum EState {
            Startstop,
            OutDataElem,
            InDataElem,
            InRegClicks,
        };
        EState State;

        ui32 DataInd;
        TMap<ui32, TRegInfo>::iterator RegIter;

        TMetaIterator<TVerifyingDecompressor>& GetDecompressor(size_t index) {
            Y_UNUSED(index);
            return *Parent;
        }

        TVerifyingDecompressor()
            : State(Startstop)
            , DataInd(0)
        {
        }
        void BeginSelf(ui32 /*count*/, ui32 id) {
            switch (State) {
                case Startstop:
                    UNIT_ASSERT_EQUAL(id, 0);
                    State = OutDataElem;
                    break;
                case OutDataElem:
                    UNIT_ASSERT(0);
                case InDataElem:
                    UNIT_ASSERT_EQUAL(id, 2);
                    State = InRegClicks;
                    RegIter = data[DataInd].RegClicks.begin();
                    break;
                case InRegClicks:
                    UNIT_ASSERT(0);
                default:
                    UNIT_ASSERT(0);
            }
        }
        void EndSelf() {
            switch (State) {
                case Startstop:
                    UNIT_ASSERT(0);
                case OutDataElem:
                    State = Startstop;
                    break;
                case InDataElem:
                    UNIT_ASSERT(0);
                case InRegClicks:
                    UNIT_ASSERT_EQUAL(RegIter, data[DataInd].RegClicks.end());
                    State = InDataElem;
                    break;
                default:
                    UNIT_ASSERT(0);
            }
        }
        void BeginElement(ui32 element) {
            switch (State) {
                case Startstop:
                    UNIT_ASSERT(0);
                case OutDataElem:
                    UNIT_ASSERT(element < data.size());
                    State = InDataElem;
                    break;
                case InDataElem:
                    UNIT_ASSERT(0);
                case InRegClicks:
                    UNIT_ASSERT_EQUAL(element, RegIter->first);
                    break;
            }
        }
        void EndElement() {
            switch (State) {
                case Startstop:
                    UNIT_ASSERT(0);
                case OutDataElem:
                    UNIT_ASSERT(0);
                case InDataElem:
                    State = OutDataElem;
                    ++DataInd;
                    break;
                case InRegClicks:
                    ++RegIter;
                    break;
            }
        }

        void SetScalar(size_t index, ui32 val) {
            switch (State) {
                case OutDataElem:
                    UNIT_ASSERT(0);
                case InDataElem:
                    if (index == 0)
                        UNIT_ASSERT_EQUAL(val, data[DataInd].Clicks);
                    if (index == 1)
                        UNIT_ASSERT_EQUAL(val, data[DataInd].Shows);
                    if (index == 31)
                        UNIT_ASSERT_EQUAL(val, data[DataInd].Extra);
                    break;
                case InRegClicks:
                    if (index == 0)
                        UNIT_ASSERT_EQUAL(val, RegIter->second.Clicks);
                    if (index == 1)
                        UNIT_ASSERT_EQUAL(val, RegIter->second.Shows);
                    break;
                default:
                    UNIT_ASSERT(0);
            }
        }
    };

    Y_UNIT_TEST(VerifyDecompression) {
        Test<TVerifyingDecompressor, TSerialize>(metainfo, CM_SINGLEPASS);
    }

    Y_UNIT_TEST(VerifyHistDecompression) {
        Test<TVerifyingDecompressor, TSerialize>(metainfo, CM_TWOPASS);
    }

    Y_UNIT_TEST(VerifyDecompressionMulti) {
        Test<TMultiDecompressor, TSerialize>(metainfo, CM_SINGLEPASS);
    }

    Y_UNIT_TEST(VerifyHistDecompressionMulti) {
        Test<TMultiDecompressor, TSerialize>(metainfo, CM_TWOPASS);
    }
}

Y_UNIT_TEST_SUITE(CompProtoTestExtended) {
    using namespace NCompProto;
    const TString metainfo =
        "\n\
    repeated data id 0\n\
        repeated second id 3\n\
            scalar inner2 id 0 default const 0\n\
        end\n\
        repeated first id 2\n\
            scalar inner id 0 default const 0\n\
        end\n\
    end\n";
    TVector<std::pair<TVector<ui32>, TVector<ui32>>> data;

    template <class TMeta, class TFunctor>
    struct TSerialize {
        static void Serialize(TMetaInfo<TMeta>& meta, TFunctor& functor, const TTestParams& params) {
            FlushPseudoRandom();
            meta.BeginSelf(functor);
            data.clear();
            data.resize(params.DataSize);
            for (size_t i = 0; i < params.DataSize; ++i) {
                meta.BeginElement(i, functor);
                TMetaInfo<TMeta>& first = meta.BeginRepeated(2, functor);
                data[i].first.resize(params.ValueArraySize);
                for (ui32 j = 0; j < params.ValueArraySize; j++) {
                    first.BeginElement(j, functor);

                    ui32 val = PseudoRandom(42 * 42 * 42);
                    first.SetScalar(0, val, functor);
                    data[i].first[j] = val;

                    first.EndElement(functor);
                }
                first.EndRepeated(functor);

                TMetaInfo<TMeta>& second = meta.BeginRepeated(3, functor);
                data[i].second.resize(params.ValueArraySize);
                for (ui32 j = 0; j < params.ValueArraySize; j++) {
                    second.BeginElement(j, functor);

                    ui32 val = PseudoRandom(42 * 42 * 42);
                    second.SetScalar(0, val, functor);
                    data[i].second[j] = val;

                    second.EndElement(functor);
                }
                second.EndRepeated(functor);
                meta.EndElement(functor);
            }
            meta.EndRepeated(functor);
        }
    };

    struct TVerifyingDecompressor: public TParentHold<TVerifyingDecompressor> {
        enum EState {
            Startstop,
            OutDataElem,
            InDataElemBeforeSecond,
            InDataElemSecond,
            InFirst,
            InSecond,
        };
        EState State;

        ui32 DataInd;
        ui32 ArrayInd;

        TVerifyingDecompressor()
            : State(Startstop)
            , DataInd(0)
            , ArrayInd(0)
        {
        }

        TMetaIterator<TVerifyingDecompressor>& GetDecompressor(size_t index) {
            Y_UNUSED(index);
            return *Parent;
        }

        void BeginSelf(ui32 /*count*/, ui32 id) {
            switch (State) {
                case Startstop:
                    UNIT_ASSERT_EQUAL(id, 0);
                    State = OutDataElem;
                    break;
                case InDataElemBeforeSecond:
                    UNIT_ASSERT_EQUAL(id, 2);
                    State = InFirst;
                    ArrayInd = 0;
                    break;
                case InDataElemSecond:
                    UNIT_ASSERT_EQUAL(id, 3);
                    State = InSecond;
                    ArrayInd = 0;
                    break;
                default:
                    UNIT_ASSERT(0);
            }
        }
        void EndSelf() {
            switch (State) {
                case OutDataElem:
                    State = Startstop;
                    break;
                case InFirst:
                    State = InDataElemSecond;
                    break;
                case InSecond:
                    State = InDataElemSecond;
                    break;
                default:
                    UNIT_ASSERT(0);
            }
        }
        void BeginElement(ui32 element) {
            switch (State) {
                case OutDataElem:
                    UNIT_ASSERT(element < data.size());
                    State = InDataElemBeforeSecond;
                    break;
                case InFirst:
                    UNIT_ASSERT(element < data[DataInd].first.size());
                    break;
                case InSecond:
                    UNIT_ASSERT(element < data[DataInd].second.size());
                    break;
                default:
                    Cerr << (ui32)State << Endl;
                    UNIT_ASSERT(0);
            }
        }
        void EndElement() {
            switch (State) {
                case InFirst:
                case InSecond:
                    ++ArrayInd;
                    break;
                case InDataElemSecond:
                    ++DataInd;
                    State = OutDataElem;
                    break;
                default:
                    Cerr << (ui32)State << Endl;
                    UNIT_ASSERT(0);
            }
        }

        void SetScalar(size_t index, ui32 val) {
            UNIT_ASSERT_EQUAL(index, 0);
            switch (State) {
                case InFirst:
                    UNIT_ASSERT_EQUAL(val, data[DataInd].first[ArrayInd]);
                    break;
                case InSecond:
                    UNIT_ASSERT_EQUAL(val, data[DataInd].second[ArrayInd]);
                    break;
                default:
                    UNIT_ASSERT(0);
            }
        }
    };
    Y_UNIT_TEST(VerifyDecompression) {
        Test<TVerifyingDecompressor, TSerialize>(metainfo, CM_SINGLEPASS);
    }

    Y_UNIT_TEST(VerifyHistDecompression) {
        Test<TVerifyingDecompressor, TSerialize>(metainfo, CM_TWOPASS);
    }
}
