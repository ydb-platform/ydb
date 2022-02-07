#pragma once

#include <util/datetime/base.h>
#include <util/system/sanitizers.h>
#include <ydb/core/erasure/erasure.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>

constexpr TDuration TIMEOUT = NSan::PlainOrUnderSanitizer(TDuration::Seconds(400), TDuration::Seconds(800));
constexpr TDuration SMALL_TIMEOUT = NSan::PlainOrUnderSanitizer(TDuration::MilliSeconds(100), TDuration::MilliSeconds(500));
constexpr ui32 DefChunkSize = 512u << 10u;
constexpr ui64 DefDiskSize = 16ull << 30ull;
constexpr ui32 DefDomainsNum = 4u;
constexpr ui32 DefDisksInDomain = 2u;
constexpr NKikimr::TErasureType::EErasureSpecies DefErasure = NKikimr::TBlobStorageGroupType::ErasureMirror3;

constexpr NKikimrBlobStorage::EPutHandleClass UNK = NKikimrBlobStorage::EPutHandleClass::TabletLog;
constexpr NKikimrBlobStorage::EPutHandleClass HUGEB = NKikimrBlobStorage::EPutHandleClass::AsyncBlob;
