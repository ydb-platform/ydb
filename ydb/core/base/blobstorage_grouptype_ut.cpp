#include <ydb/core/blobstorage/defs.h>

#include "blobstorage_grouptype.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/string/printf.h>
#include <util/stream/null.h>

namespace NKikimr {

#define STR Cnull

Y_UNIT_TEST_SUITE(TBlobStorageGroupTypeTest) {

    Y_UNIT_TEST(OutputInfoAboutErasureSpecies) {

        for (auto [es, _] : TErasureType::ErasureNames) {
            TBlobStorageGroupType groupType(es);
            STR << groupType.ToString() << ":\n";
            STR << "  ParityParts:                " << groupType.ParityParts() << "\n";
            STR << "  DataParts:                  " << groupType.DataParts() << "\n";
            STR << "  TotalPartCount:             " << groupType.TotalPartCount() << "\n";
            STR << "  MinimalRestorablePartCount: " << groupType.MinimalRestorablePartCount() << "\n";
            STR << "  MinimalBlockSize:           " << groupType.MinimalBlockSize() << "\n";
            STR << "  Prime:                      " << groupType.Prime() << "\n";
            STR << "  BlobSubgroupSize:           " << groupType.BlobSubgroupSize() << "\n";
            STR << "  Handoff:                    " << groupType.Handoff() << "\n";
        }
    }

}

} // namespace NKikimr

