#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_data.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_signature.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_glue.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_logoblob.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_block.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_barrier.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_rec.h>
#include <ydb/core/blobstorage/vdisk/common/disk_part.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NKikimr::NPDisk;

Y_UNIT_TEST_SUITE(TPDiskToolVersionGuards) {
    Y_UNIT_TEST(OnDiskConstants) {
        // If any of these fire, production started writing a new layout and pdisktool must be updated.
        UNIT_ASSERT_VALUES_EQUAL_C(PDISK_FORMAT_VERSION, 3,
            "PDISK_FORMAT_VERSION changed; update pdisktool");
        UNIT_ASSERT_VALUES_EQUAL_C(PDISK_SYS_LOG_RECORD_VERSION_8, 8,
            "PDISK_SYS_LOG_RECORD_VERSION_8 changed; update pdisktool");
        UNIT_ASSERT_VALUES_EQUAL_C(ui32(TLogSignature::Max), 27u,
            "TLogSignature::Max changed; update pdisktool log/hull parsers");
        constexpr ui32 placeholderSignature = TIdxDiskPlaceHolder::Signature;
        UNIT_ASSERT_VALUES_EQUAL_C(placeholderSignature, 0x12345679u,
            "TIdxDiskPlaceHolder::Signature changed; update pdisktool SST walker");
        UNIT_ASSERT_VALUES_EQUAL_C(0x93F7ADD5u, 0x93F7ADD5u,
            "THullDbSignatureRoutines magic; keep in sync with hull.cpp HullEntryMagic");
    }

    Y_UNIT_TEST(OnDiskStructSizes) {
        UNIT_ASSERT_C(sizeof(TDiskFormat) == 1168,
            "sizeof(TDiskFormat)=" << sizeof(TDiskFormat) << "; update pdisktool");
        UNIT_ASSERT_C(sizeof(TChunkInfo) == 10,
            "sizeof(TChunkInfo)=" << sizeof(TChunkInfo) << "; update pdisktool");
        UNIT_ASSERT_C(sizeof(TCommitRecordFooter) == 25,
            "sizeof(TCommitRecordFooter)=" << sizeof(TCommitRecordFooter) << "; update pdisktool");
        UNIT_ASSERT_C(sizeof(TIdxDiskPlaceHolder) == 96,
            "sizeof(TIdxDiskPlaceHolder)=" << sizeof(TIdxDiskPlaceHolder) << "; update pdisktool");
        UNIT_ASSERT_VALUES_EQUAL(sizeof(TKeyLogoBlob), 24u);
        UNIT_ASSERT_VALUES_EQUAL(sizeof(TMemRecLogoBlob), 20u);
        UNIT_ASSERT_VALUES_EQUAL(sizeof(TDiskPart), 12u);
        UNIT_ASSERT_VALUES_EQUAL(sizeof(TIndexRecord<TKeyLogoBlob, TMemRecLogoBlob>), 44u);
    }
}
