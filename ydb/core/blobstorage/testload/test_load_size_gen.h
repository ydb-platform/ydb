#pragma once

#include "defs.h"
#include "test_load_gen.h"
#include <ydb/core/protos/blobstorage.pb.h>

namespace NKikimr {
    namespace NSizeGenerator {
        struct TItem {
            ui32 Min;
            ui32 Max;

            TItem(const NKikimrBlobStorage::TEvTestLoadRequest::TSizeInfo& x)
                : Min(x.GetMin())
                , Max(x.GetMax())
            {
                Y_VERIFY(x.HasMin() && x.HasMax());
            }

            ui32 Generate() const {
                ui32 range = Max - Min + 1;
                return Min + TAppData::RandomProvider->GenRand64() % range;
            }
        };
    } // NSizeGenerator

    using TSizeGenerator = TGenerator<NSizeGenerator::TItem>;
} // NKikimr
