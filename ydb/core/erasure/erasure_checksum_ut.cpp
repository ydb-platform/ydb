#include "erasure.h"
#include "ut_util.h"

#include <ydb/core/base/logoblob.h>
#include <ydb/core/erasure/erasure.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TErasureChecksumTest) {

    Y_UNIT_TEST(IsRequired) {
        const TLogoBlobID logoBlobCrc0(1, 1, 1, 0, 100, 0, 0, TErasureType::CrcModeNone);
        const TLogoBlobID logoBlobCrc1(1, 1, 1, 0, 100, 0, 0, TErasureType::CrcModeWholePart);

        UNIT_ASSERT(!TChecksum::IsRequired(logoBlobCrc0));
        UNIT_ASSERT(TChecksum::IsRequired(logoBlobCrc1));
    }

    void TestRoundTrip(TErasureType erasure, TErasureType::ECrcMode crcMode, const TString& payload) {
        const auto logoBlob = TLogoBlobID::Make(1, 1, 1, 0, payload.size(), 0, crcMode);

        TDataPartSet parts;
        erasure.SplitData(MakeCrcNoneStubBlobId(payload.size()), payload, parts);

        for (ui64 i = 0; i < erasure.TotalPartCount(); ++i) {
            const TLogoBlobID partId(logoBlob, i + 1);
            const TRope rope = TChecksum::Append(partId, erasure, parts.Parts[i].OwnedString);
            UNIT_ASSERT_C(TChecksum::IsValid(partId, erasure, rope), "partId = " << i);
            UNIT_ASSERT_EQUAL_C(TChecksum::Remove(partId, erasure, rope), parts.Parts[i].OwnedString, "partId = " << i);
        }
    }

    void TestIdempotence(TErasureType erasure, TErasureType::ECrcMode crcMode, const TString& payload) {
        const auto logoBlob = TLogoBlobID::Make(1, 1, 1, 0, payload.size(), 0, crcMode);

        TRope data(payload);
        TDataPartSet parts;
        erasure.SplitData(MakeCrcNoneStubBlobId(payload.size()), data, parts);

        for (ui32 i = 0; i < erasure.TotalPartCount(); ++i) {
            const TLogoBlobID partId(logoBlob, i + 1);
            const TString context = TStringBuilder() << "part# " << partId.PartId();
            const TRope originalData = std::move(parts.Parts[i].OwnedString);
            TRope part = originalData;

            for (ui32 attempt = 0; attempt < 10; ++attempt) {
                part = TChecksum::Remove(partId, erasure, std::move(part));
                UNIT_ASSERT_C(part == originalData, context << " Remove attempt# " << attempt);
            }

            for (ui32 attempt = 0; attempt < 10; ++attempt) {
                part = TChecksum::Append(partId, erasure, std::move(part));
                UNIT_ASSERT_C(TChecksum::IsValid(partId, erasure, part), context << " Append attempt# " << attempt);
                if (!part.IsEmpty() && TChecksum::IsRequired(partId)) {
                    UNIT_ASSERT_C(part != originalData, context << " Append attempt# " << attempt);
                } else {
                    UNIT_ASSERT_C(part == originalData, context << " Append attempt# " << attempt);
                }
            }

            part = TChecksum::Remove(partId, erasure, originalData);
            UNIT_ASSERT_C(part == originalData, context << " after final Remove");
        }
    }

    void TestChecksumCorruption(TErasureType erasure, bool corruptPayload) {
        const TString payload = "Payload";
        const auto logoBlob = TLogoBlobID::Make(1, 1, 1, 0, payload.size(), 0, TErasureType::CrcModeWholePart);

        TDataPartSet parts;
        erasure.SplitData(MakeCrcNoneStubBlobId(payload.size()), payload, parts);

        for (ui64 i = 0; i < erasure.TotalPartCount(); ++i) {
            const TLogoBlobID partId(logoBlob, i + 1);
            const TRope rope = TChecksum::Append(partId, erasure, parts.Parts[i].OwnedString);
            UNIT_ASSERT(rope.size() != parts.Parts[i].size());
            UNIT_ASSERT(TChecksum::IsValid(partId, erasure, rope));

            TString data = rope.ConvertToString();
            if (corruptPayload) {
                data.front() = data.front() ^ 1;
            } else {
                data.back() = data.back() ^ 1;
            }

            UNIT_ASSERT(!TChecksum::IsValid(partId, erasure, TRope(data)));
        }
    }

    void TestWrongBlob(TErasureType erasure, TErasureType::ECrcMode crcMode) {
        const TString payload = "Payload";
        const auto logoBlob = TLogoBlobID::Make(1, 1, 1, 0, payload.size(), 0, crcMode);

        TDataPartSet parts;
        erasure.SplitData(MakeCrcNoneStubBlobId(payload.size()), payload, parts);

        const bool acceptsWrongId = crcMode == TErasureType::CrcModeNone
            || erasure.ErasureFamily() == TErasureType::ErasureMirror;
        const auto wrongLogoBlob = TLogoBlobID::Make(2, 1, 1, 0, payload.size(), 0, crcMode);
        const ui32 partsCount = erasure.TotalPartCount();
        for (ui32 i = 0; i < partsCount; ++i) {
            const TLogoBlobID partId(logoBlob, i + 1);
            const TRope rope = TChecksum::Append(partId, erasure, parts.Parts[i].OwnedString);
            UNIT_ASSERT(TChecksum::IsValid(partId, erasure, rope));

            const TLogoBlobID wrongLogoBlobPartId(wrongLogoBlob, i + 1);
            UNIT_ASSERT_VALUES_EQUAL(TChecksum::IsValid(wrongLogoBlobPartId, erasure, rope), acceptsWrongId);

            const TLogoBlobID wrongPartId(logoBlob, partId.PartId() == 1 ? 2 : 1);
            UNIT_ASSERT_VALUES_EQUAL(TChecksum::IsValid(wrongPartId, erasure, rope), acceptsWrongId);
        }
    }


#ifndef FOR_EACH_ERASURE
#define FOR_EACH_ERASURE(M)   \
    M(ErasureNone)            \
    M(ErasureMirror3)         \
    M(Erasure3Plus1Block)     \
    M(Erasure3Plus1Stripe)    \
    M(Erasure4Plus2Block)     \
    M(Erasure3Plus2Block)     \
    M(Erasure4Plus2Stripe)    \
    M(Erasure3Plus2Stripe)    \
    M(ErasureMirror3Plus2)    \
    M(ErasureMirror3dc)       \
    M(Erasure4Plus3Block)     \
    M(Erasure4Plus3Stripe)    \
    M(Erasure3Plus3Block)     \
    M(Erasure3Plus3Stripe)    \
    M(Erasure2Plus3Block)     \
    M(Erasure2Plus3Stripe)    \
    M(Erasure2Plus2Block)     \
    M(Erasure2Plus2Stripe)    \
    M(ErasureMirror3of4)

#define TEST_CHECKSUM_ROUND_TRIP(erasure)                                                   \
    Y_UNIT_TEST(TestRoundTrip_NoCrc_WithPayload_##erasure) {                                \
        TestRoundTrip(TErasureType::erasure, TErasureType::CrcModeNone, "payload");         \
    }                                                                                       \
    Y_UNIT_TEST(TestRoundTrip_NoCrc_NoPayload_##erasure) {                                  \
        TestRoundTrip(TErasureType::erasure, TErasureType::CrcModeNone, "");                \
    }                                                                                       \
    Y_UNIT_TEST(TestRoundTrip_WithCrc_WithPayload_##erasure) {                              \
        TestRoundTrip(TErasureType::erasure, TErasureType::CrcModeWholePart, "payload");    \
    }                                                                                       \
    Y_UNIT_TEST(TestRoundTrip_WithCrc_NoPayload_##erasure) {                                \
        TestRoundTrip(TErasureType::erasure, TErasureType::CrcModeWholePart, "");           \
    }

#define TEST_CHECKSUM_WRONG_BLOB(erasure)                                                   \
    Y_UNIT_TEST(TestChecksumWrongBlob_NoCrc_##erasure) {                                    \
        TestWrongBlob(TErasureType::erasure, TErasureType::CrcModeNone);                    \
    }                                                                                       \
    Y_UNIT_TEST(TestChecksumWrongBlob_WithCrc_##erasure) {                                  \
        TestWrongBlob(TErasureType::erasure, TErasureType::CrcModeWholePart);               \
    }

#define TEST_CHECKSUM_CORRUPTION(erasure)                                                   \
    Y_UNIT_TEST(TestChecksumCorruption_CorruptPayload_##erasure) {                          \
        TestChecksumCorruption(TErasureType::erasure, true);                                \
    }                                                                                       \
    Y_UNIT_TEST(TestChecksumCorruption_CorruptChecksum_##erasure) {                         \
        TestChecksumCorruption(TErasureType::erasure, false);                               \
    }

#define TEST_CHECKSUM_IDEMPOTENCE(erasure)                                                     \
    Y_UNIT_TEST(TestIdempotence_NoCrc_WithPayload_##erasure) {                                 \
        TestIdempotence(TErasureType::erasure, TErasureType::CrcModeNone, "payload");          \
    }                                                                                          \
    Y_UNIT_TEST(TestIdempotence_NoCrc_NoPayload_##erasure) {                                   \
        TestIdempotence(TErasureType::erasure, TErasureType::CrcModeNone, "");                 \
    }                                                                                          \
    Y_UNIT_TEST(TestIdempotence_WithCrc_WithPayload_##erasure) {                               \
        TestIdempotence(TErasureType::erasure, TErasureType::CrcModeWholePart, "payload");     \
    }                                                                                          \
    Y_UNIT_TEST(TestIdempotence_WithCrc_NoPayload_##erasure) {                                 \
        TestIdempotence(TErasureType::erasure, TErasureType::CrcModeWholePart, "");            \
    }

FOR_EACH_ERASURE(TEST_CHECKSUM_ROUND_TRIP);
FOR_EACH_ERASURE(TEST_CHECKSUM_WRONG_BLOB);
FOR_EACH_ERASURE(TEST_CHECKSUM_CORRUPTION);
FOR_EACH_ERASURE(TEST_CHECKSUM_IDEMPOTENCE);

#undef FOR_EACH_ERASURE
#endif // FOR_EACH_ERASURE

}

} // NKikimr
