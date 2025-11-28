#include "type_codecs.h"
#include "type_codecs_defs.h"
#include <ydb/core/scheme_types/scheme_types_defs.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/vector.h>
#include <util/random/fast.h>
#include <util/datetime/base.h>

namespace NKikimr::NPQ {

using ICodec = NScheme::ICodec;
using TTypeCodecs = NScheme::TTypeCodecs;
using TCodecSig = NScheme::TCodecSig;
using TCodecType = NScheme::TCodecType;
using TDataRef = NScheme::TDataRef;

Y_UNIT_TEST_SUITE(TTypeCodecsTest) {

    void Metrics(const TVector<TDataRef>& values, const ICodec* codec) {
        TBuffer output;
        auto chunk = codec->MakeChunk(output);

        auto start = TInstant::Now();
        for (const auto& value : values) {
            if (value.IsNull())
                chunk->AddNull();
            else
                chunk->AddData(value.Data(), value.Size());
        }
        chunk->Seal();
        auto duration = TInstant::Now() - start;

        Cerr << "Size: " << output.Size() << Endl;
        Cerr << "Create chunk: " << duration << Endl;

        auto reading = codec->ReadChunk(output);

        start = TInstant::Now();
        for (size_t i = 0, size = values.size(); i != size; ++i) {
            auto value = reading->GetValue(i);
            Y_UNUSED(value);
        }
        Cerr << "Read by index: " << TInstant::Now() - start << Endl;

        auto iter = reading->MakeIterator();
        start = TInstant::Now();
        for (size_t i = 0; i != values.size(); ++i) {
            auto value = iter->Next();
            Y_UNUSED(value);
        }
        Cerr << "Iterate: " << TInstant::Now() - start << Endl;
    }

    void TestImpl(const TVector<TDataRef>& values, const ICodec* codec) {
        TBuffer output;
        auto chunk = codec->MakeChunk(output);
        for (const auto& value : values) {
            if (value.IsNull())
                chunk->AddNull();
            else
                chunk->AddData(value.Data(), value.Size());
        }
        chunk->Seal();

        auto reading = codec->ReadChunk(output);
        auto iter = reading->MakeIterator();

        for (size_t i = 0; i != values.size(); ++i) {
            const auto& value = values[i];
            auto value1 = reading->GetValue(i);
            auto value2 = iter->Peek();
            auto value3 = iter->Next();
            UNIT_ASSERT_EQUAL(value, value1);
            UNIT_ASSERT_EQUAL(value1, value2);
            UNIT_ASSERT_EQUAL(value1, value3);
            UNIT_ASSERT_EQUAL(value2, value3);
        }

        Metrics(values, codec);
    };

    Y_UNIT_TEST(TestBoolCodec) {
        static const bool VALUE_FALSE = false;
        static const bool VALUE_TRUE = true;

        THolder<TTypeCodecs> codecs(new TTypeCodecs(NScheme::TBool::TypeId));

        TVector<TDataRef> values(1000, TDataRef((const char*)&VALUE_FALSE, sizeof(VALUE_FALSE)));
        for (int i = 0; i < 100; ++i) {
            values[i * 2] = TDataRef((const char*)&VALUE_TRUE, sizeof(VALUE_TRUE));
            values[500 + i] = TDataRef((const char*)&VALUE_TRUE, sizeof(VALUE_TRUE));
            values[700 + 3 * i] = TDataRef((const char*)&VALUE_TRUE, sizeof(VALUE_TRUE));
        }

        auto codec = codecs->GetCodec(TCodecSig(TCodecType::Bool, false));
        TestImpl(values, codec);

        for (int i = 0; i < 200; ++i) {
            values[i * 5] = TDataRef();
        }
        codec = codecs->GetCodec(TCodecSig(TCodecType::Bool, true));
        TestImpl(values, codec);
    }

    Y_UNIT_TEST(TestFixedLenCodec) {
        THolder<TTypeCodecs> codecs(new TTypeCodecs(NScheme::TInt32::TypeId));

        TVector<TDataRef> values;
        for (int i = 0; i < 1000; ++i) {
            int data = i;
            values.push_back(TDataRef((const char*)&data, sizeof(data), true));
            data = -i;
            values.push_back(TDataRef((const char*)&data, sizeof(data), true));
        }

        auto codec = codecs->GetCodec(TCodecSig(TCodecType::FixedLen, false));
        TestImpl(values, codec);

        for (int i = 0; i < 200; ++i) {
            values[i * 5] = TDataRef();
            values[1200 + i] = TDataRef();
        }
        codec = codecs->GetCodec(TCodecSig(TCodecType::FixedLen, true));
        TestImpl(values, codec);
    }

    Y_UNIT_TEST(TestVarLenCodec) {
        THolder<TTypeCodecs> codecs(new TTypeCodecs(NScheme::TString::TypeId));

        TReallyFastRng32 rand(100500);

        TVector<TDataRef> values;
        for (int i = 0; i < 1000; ++i) {
            TVector<char> value(rand.Uniform(10));
            for (char& c : value)
                c = 'a' + rand.Uniform(26);
            values.push_back(TDataRef(value.data(), value.size(), true));
        }

        auto codec = codecs->GetCodec(TCodecSig(TCodecType::VarLen, false));
        TestImpl(values, codec);

        for (int i = 0; i < 100; ++i) {
            values[i * 5] = TDataRef();
            values[800 + i] = TDataRef();
        }
        codec = codecs->GetCodec(TCodecSig(TCodecType::VarLen, true));
        TestImpl(values, codec);
    }

    Y_UNIT_TEST(TestVarIntCodec) {
        THolder<TTypeCodecs> codecs(new TTypeCodecs(NScheme::TUint32::TypeId));

        TReallyFastRng32 rand(100500);

        TVector<TDataRef> values;
        for (int i = 0; i < 1000; ++i) {
            ui32 value = rand.Uniform(100500);
            values.push_back(TDataRef((const char*)&value, sizeof(value), true));
        }

        auto codec = codecs->GetCodec(TCodecSig(TCodecType::VarInt, false));
        TestImpl(values, codec);

        for (int i = 0; i < 100; ++i) {
            values[i * 5] = TDataRef();
            values[800 + i] = TDataRef();
        }
        codec = codecs->GetCodec(TCodecSig(TCodecType::VarInt, true));
        TestImpl(values, codec);
    }

    Y_UNIT_TEST(TestZigZagCodec) {
        THolder<TTypeCodecs> codecs(new TTypeCodecs(NScheme::TUint32::TypeId));

        TReallyFastRng32 rand(100500);

        TVector<TDataRef> values;
        for (int i = 0; i < 1000; ++i) {
            i32 value = rand.Uniform(100500);
            if (i & 1)
                value = -value;
            values.push_back(TDataRef((const char*)&value, sizeof(value), true));
        }

        auto codec = codecs->GetCodec(TCodecSig(TCodecType::ZigZag, false));
        TestImpl(values, codec);

        for (int i = 0; i < 100; ++i) {
            values[i * 5] = TDataRef();
            values[800 + i] = TDataRef();
        }
        codec = codecs->GetCodec(TCodecSig(TCodecType::ZigZag, true));
        TestImpl(values, codec);
    }

    void TestDeltaVarIntCodecImpl(TCodecType type, bool rev) {
        THolder<TTypeCodecs> codecs(new TTypeCodecs(NScheme::TUint32::TypeId));

        TReallyFastRng32 rand(100500);

        TVector<TDataRef> values;
        ui32 value = 2000000;
        for (int i = 0; i < 1000; ++i) {
            if (rev)
                value -= rand.Uniform(1000);
            else
                value += rand.Uniform(1000);
            values.push_back(TDataRef((const char*)&value, sizeof(value), true));
        }

        auto codec = codecs->GetCodec(TCodecSig(type, false));
        TestImpl(values, codec);

        for (int i = 0; i < 100; ++i) {
            values[i * 5] = TDataRef();
            values[800 + i] = TDataRef();
        }
        codec = codecs->GetCodec(TCodecSig(type, true));
        TestImpl(values, codec);
    }

    Y_UNIT_TEST(TestDeltaVarIntCodecAndRev) {
        TestDeltaVarIntCodecImpl(TCodecType::DeltaVarInt, false);
        TestDeltaVarIntCodecImpl(TCodecType::DeltaRevVarInt, true);
    }

    Y_UNIT_TEST(TestDeltaZigZagCodec) {
        THolder<TTypeCodecs> codecs(new TTypeCodecs(NScheme::TInt32::TypeId));

        TReallyFastRng32 rand(100500);

        TVector<TDataRef> values;
        ui32 value = -17;
        for (int i = 0; i < 1000; ++i) {
            value += rand.Uniform(1000);
            if (rand.GenRand() & 1)
                value = -value;
            values.push_back(TDataRef((const char*)&value, sizeof(value), true));
        }

        auto codec = codecs->GetCodec(TCodecSig(TCodecType::DeltaZigZag, false));
        TestImpl(values, codec);

        for (int i = 0; i < 100; ++i) {
            values[i * 5] = TDataRef();
            values[800 + i] = TDataRef();
        }
        codec = codecs->GetCodec(TCodecSig(TCodecType::DeltaZigZag, true));
        TestImpl(values, codec);
    }

}

} // namespace NKikimr::NPQ
