#include <kikimr/persqueue/sdk/deprecated/cpp/v2/types.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/builder.h>

namespace NPersQueue {
Y_UNIT_TEST_SUITE(TDataTest) {
    ECodec Codecs[] = {
        ECodec::LZOP,
        ECodec::GZIP,
    };

    TString DebugString(const TData& data) {
        return TStringBuilder() << "{ Ts: " << data.GetTimestamp().GetValue() << ", SrcData: \"" << data.GetSourceData()
                                << "\", Encoded: \"" << (data.IsEncoded() ? data.GetEncodedData() : TString())
                                << "\", Codec: " << data.GetCodecType() << " }";
    }

    Y_UNIT_TEST(RawDataIsEncoded) {
        auto now = TInstant::Now();
        TData data = TData::Raw("trololo", now);
        TData otherRawData = TData("trololo", ECodec::RAW, now);
        UNIT_ASSERT_C(data.IsEncoded(), "data: " << DebugString(data));
        UNIT_ASSERT_C(!data.Empty(), "data: " << DebugString(data));
        UNIT_ASSERT_EQUAL_C(data.GetCodecType(), ECodec::RAW, "data: " << DebugString(data));
        UNIT_ASSERT_EQUAL_C(data, otherRawData, "data: " << DebugString(data) << ", other: " << DebugString(otherRawData));
    }

    Y_UNIT_TEST(EncodedDataIsEncoded) {
        for (ECodec codec : Codecs) {
            TData data = TData::Encoded("trololo", codec);
            UNIT_ASSERT(data.IsEncoded());
            UNIT_ASSERT(!data.Empty());
            UNIT_ASSERT_EQUAL(data.GetCodecType(), codec);
        }
    }

    Y_UNIT_TEST(ModifiesState) {
        for (ECodec codec : Codecs) {
            for (ECodec defaultCodec : Codecs) {
                TData data("trololo", codec);
                UNIT_ASSERT(!data.IsEncoded());
                UNIT_ASSERT(!data.Empty());

                for (size_t i = 0; i < 2; ++i) {
                    data = TData::Encode(std::move(data), defaultCodec, 2); // encode twice is OK
                    UNIT_ASSERT(data.IsEncoded());
                    UNIT_ASSERT(!data.Empty());
                    UNIT_ASSERT(!data.GetEncodedData().empty());
                    UNIT_ASSERT_EQUAL(data.GetCodecType(), codec);
                }
            }
        }
    }

    Y_UNIT_TEST(HandlesDefaultCodec) {
        for (ECodec defaultCodec : Codecs) {
            TData data = TString("trololo");
            UNIT_ASSERT(!data.IsEncoded());
            UNIT_ASSERT(!data.Empty());

            data = TData::Encode(data, defaultCodec, -1);
            UNIT_ASSERT(data.IsEncoded());
            UNIT_ASSERT(!data.Empty());
            UNIT_ASSERT_STRINGS_EQUAL(data.GetSourceData(), "trololo");
            UNIT_ASSERT(!data.GetEncodedData().empty());
            UNIT_ASSERT_EQUAL(data.GetCodecType(), defaultCodec);
        }
    }

    Y_UNIT_TEST(MakesRaw) {
        TData data = TString("trololo");
        UNIT_ASSERT(!data.IsEncoded());
        data = TData::MakeRawIfNotEncoded(data);
        UNIT_ASSERT(data.IsEncoded());
        UNIT_ASSERT_EQUAL(data.GetCodecType(), ECodec::RAW);
    }

    Y_UNIT_TEST(DoesNotMakeRaw) {
        TData data = TData::Encoded("trololo", ECodec::GZIP);
        UNIT_ASSERT(data.IsEncoded());
        data = TData::MakeRawIfNotEncoded(data);
        UNIT_ASSERT(data.IsEncoded());
        UNIT_ASSERT_EQUAL(data.GetCodecType(), ECodec::GZIP);
    }
}
} // namespace NPersQueue
