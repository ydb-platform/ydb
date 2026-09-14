#pragma once

#include <ydb/core/protos/blobstorage_config.pb.h>

namespace NKikimr::NBsController {

    NKikimrBlobStorage::TUpdateSettings FromBscConfig(const NKikimrBlobStorage::TBscConfig &config);

}  // NKikimr::NBsController
