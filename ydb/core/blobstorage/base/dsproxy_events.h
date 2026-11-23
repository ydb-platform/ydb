#pragma once

#include <ydb/core/base/blobstorage.h>

#define DSPROXY_ENUM_EVENTS(XX) \
    XX(TEvBlobStorage::TEvPut) \
    XX(TEvBlobStorage::TEvGet) \
    XX(TEvBlobStorage::TEvBlock) \
    XX(TEvBlobStorage::TEvGetBlock) \
    XX(TEvBlobStorage::TEvDiscover) \
    XX(TEvBlobStorage::TEvRange) \
    XX(TEvBlobStorage::TEvCollectGarbage) \
    XX(TEvBlobStorage::TEvStatus) \
    XX(TEvBlobStorage::TEvPatch) \
    XX(TEvBlobStorage::TEvAssimilate) \
    XX(TEvBlobStorage::TEvCheckIntegrity) \
//
