#include "env.h"

#include <ydb/core/blobstorage/base/blobstorage_checksum.h>

using namespace NKikimr;

Y_UNIT_TEST_SUITE(VDiskChecksumTests) {

    struct TChecksumCase {
        const char *Name;
        bool HasChecksum;
        ui64 ChecksumXor;
        bool HasChecksumType;
        NKikimrBlobStorage::TChecksumType ChecksumType;
        NKikimrProto::EReplyStatus ExpectedStatus;
        const char *ExpectedErrorReason;
    };

    const TVector<TChecksumCase>& GetChecksumCases() {
        static const TVector<TChecksumCase> cases = {
            {"NoChecksumFields", false, 0, false, NKikimrBlobStorage::TChecksumType::NoChecksum,
                NKikimrProto::OK, ""},
            {"NoChecksumTypeOnly", false, 0, true, NKikimrBlobStorage::TChecksumType::NoChecksum,
                NKikimrProto::OK, ""},
            {"ValidXxh3Checksum", true, 0, true, NKikimrBlobStorage::TChecksumType::XXH3_64BitBlob,
                NKikimrProto::OK, ""},
            {"LegacyValidXxh3ChecksumWithoutType", true, 0, false, NKikimrBlobStorage::TChecksumType::NoChecksum,
                NKikimrProto::OK, ""},
            {"InvalidXxh3Checksum", true, 1, true, NKikimrBlobStorage::TChecksumType::XXH3_64BitBlob,
                NKikimrProto::ERROR, "buffer checksum mismatch"},
            {"ChecksumTypeWithoutChecksum", false, 0, true, NKikimrBlobStorage::TChecksumType::XXH3_64BitBlob,
                NKikimrProto::ERROR, "checksum type without checksum"},
            {"ChecksumWithNoChecksumType", true, 0, true, NKikimrBlobStorage::TChecksumType::NoChecksum,
                NKikimrProto::ERROR, "checksum with NoChecksum type"},
            {"UnsupportedChecksumType", true, 0, true,
                NKikimrBlobStorage::TChecksumType::XXH3_64BitBlobAndLogoBlobId,
                NKikimrProto::ERROR, "unsupported checksum type"},
        };
        return cases;
    }

    TLogoBlobID MakeBlobId(ui32 step, ui32 size) {
        return TLogoBlobID(1, 1, step, 0, size, step, 1);
    }

    TString MakeData(ui32 step, ui32 size) {
        TString data(size, '\0');
        for (ui32 i = 0; i < size; ++i) {
            data[i] = static_cast<char>(step + i) % 26 + 'a';
        }
        return data;
    }

    ui64 CalculateChecksum(const TString& data) {
        const TRope rope(data);
        return CalculateXxh3Hash(rope.Begin(), rope.GetSize()).second;
    }

    void ApplyChecksumCase(NKikimrBlobStorage::TEvVPut& record, const TString& data, const TChecksumCase& testCase) {
        if (testCase.HasChecksum) {
            record.SetChecksum(CalculateChecksum(data) ^ testCase.ChecksumXor);
        }
        if (testCase.HasChecksumType) {
            record.SetChecksumType(testCase.ChecksumType);
        }
    }

    void ApplyChecksumCase(NKikimrBlobStorage::TVMultiPutItem& item, const TString& data,
            const TChecksumCase& testCase) {
        if (testCase.HasChecksum) {
            item.SetChecksum(CalculateChecksum(data) ^ testCase.ChecksumXor);
        }
        if (testCase.HasChecksumType) {
            item.SetChecksumType(testCase.ChecksumType);
        }
    }

    void AssertReadBack(TTestEnv& env, const TLogoBlobID& id, const TString& expectedData, const TString& testCaseName) {
        auto getResult = env.Get(id);
        UNIT_ASSERT_VALUES_EQUAL_C(getResult.GetStatus(), NKikimrProto::OK, testCaseName);
        UNIT_ASSERT_VALUES_EQUAL_C(getResult.ResultSize(), 1, testCaseName);
        const auto& item = getResult.GetResult(0);
        UNIT_ASSERT_VALUES_EQUAL_C(item.GetStatus(), NKikimrProto::OK, testCaseName);
        UNIT_ASSERT_VALUES_EQUAL_C(item.GetBufferData(), expectedData, testCaseName);
    }

    Y_UNIT_TEST(VPutAcceptsAndRejectsChecksums) {
        std::optional<TTestEnv> env(std::in_place);

        ui32 step = 1;
        for (const auto& testCase : GetChecksumCases()) {
            const TLogoBlobID id = MakeBlobId(step, 64);
            const TString data = MakeData(step, id.BlobSize());
            auto put = std::make_unique<TEvBlobStorage::TEvVPut>(id, TRope(data), env->GetVDiskId(), false, nullptr,
                TInstant::Max(), NKikimrBlobStorage::EPutHandleClass::TabletLog, false);
            ApplyChecksumCase(put->Record, data, testCase);

            auto putResult = env->SendVPut(std::move(put));
            UNIT_ASSERT_VALUES_EQUAL_C(putResult.GetStatus(), testCase.ExpectedStatus, testCase.Name);
            if (testCase.ExpectedStatus == NKikimrProto::OK) {
                AssertReadBack(*env, id, data, testCase.Name);
            } else {
                UNIT_ASSERT_VALUES_EQUAL_C(putResult.GetErrorReason(), testCase.ExpectedErrorReason, testCase.Name);
            }
            ++step;
        }
    }

    Y_UNIT_TEST(VMultiPutAcceptsAndRejectsChecksums) {
        std::optional<TTestEnv> env(std::in_place);

        ui32 step = 1;
        for (const auto& testCase : GetChecksumCases()) {
            const TLogoBlobID id = MakeBlobId(step, 64);
            const TString data = MakeData(step, id.BlobSize());
            auto multiPut = std::make_unique<TEvBlobStorage::TEvVMultiPut>(env->GetVDiskId(), TInstant::Max(),
                NKikimrBlobStorage::EPutHandleClass::TabletLog, false);
            multiPut->AddVPut(id, TRcBuf(data), nullptr, false, false, false, nullptr, {}, false);
            ApplyChecksumCase(*multiPut->Record.MutableItems(0), data, testCase);

            auto multiPutResult = env->SendVMultiPut(std::move(multiPut));
            UNIT_ASSERT_VALUES_EQUAL_C(multiPutResult.GetStatus(), NKikimrProto::OK, testCase.Name);
            UNIT_ASSERT_VALUES_EQUAL_C(multiPutResult.ItemsSize(), 1, testCase.Name);
            const auto& item = multiPutResult.GetItems(0);
            UNIT_ASSERT_VALUES_EQUAL_C(item.GetStatus(), testCase.ExpectedStatus, testCase.Name);
            if (testCase.ExpectedStatus == NKikimrProto::OK) {
                AssertReadBack(*env, id, data, testCase.Name);
            } else {
                UNIT_ASSERT_VALUES_EQUAL_C(item.GetErrorReason(), testCase.ExpectedErrorReason, testCase.Name);
            }
            ++step;
        }
    }

}
