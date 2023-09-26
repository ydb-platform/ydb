#pragma once

#include <util/system/types.h>

namespace NKikimr::NKesus::NQuoter {

constexpr inline double FADING_ALLOCATION_COEFFICIENT = 0.999;
constexpr inline double PREFETCH_COEFFICIENT_DEFAULT = 0.20;
constexpr inline double PREFETCH_WATERMARK_DEFAULT = 0.75;
constexpr inline ui32 REPLICATION_PERIOD_MS_DEFAULT = 5000;

/*
 * 0 or default means it do not support replication
 * 1 added replication for hdrr and immediately_fill_up_to, kesus shouldn't wait for any
 * consumption reports, shouldn't rely on inital availiable and shouldn't send any sync messages **until** this version
 */
constexpr inline ui32 QUOTER_PROTOCOL_VERSION = 1;
/*
 * 0 or default means it do not support replication
 * 1 added replication for hdrr and immediately_fill_up_to, quoter shouldn't send any
 * consumption reports, shouldn't await inital availiable and shouldn't await any sync messages **until** this version
 */
constexpr inline ui32 KESUS_PROTOCOL_VERSION = 1;


}
