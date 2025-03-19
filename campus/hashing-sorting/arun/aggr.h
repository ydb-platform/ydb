#pragma once

#include <util/system/types.h>
#include <util/stream/file.h>

void aggr_memory_lp_ht(TFileInput& fi, TFileOutput& fo, ui64 rowCount, ui32 keyCount, ui64 cardinality);
void aggr_external_rh_ht(TFileInput& fi, TFileOutput& fo, ui64 rowCount, ui32 keyCount, ui64 cardinality, ui16 hashBits, ui16 fillRatio, ui16 partBits, ui32 partBufferSize1, ui16 hashBits2, ui32 partBufferSize2);

template <ui32 keyCount>
void aggr_external_merge(TFileInput& fi, TFileOutput& fo, ui64 rowCount, ui64 cardinality, ui16 hashBits, ui16 fillRatio, ui32 partBufferSize);

void aggr_memory_rh_ht(TFileInput& fi, TFileOutput& fo, ui64 rowCount, ui32 keyCount, ui64 cardinality, ui16 hashBits);
void aggr_memory_rhi_ht(TFileInput& fi, TFileOutput& fo, ui64 rowCount, ui32 keyCount, ui64 cardinality);

ui32 round_to_nearest_power_of_two(ui64 n);
ui64 hash_keys(ui64 * keys, ui64 keyCount);