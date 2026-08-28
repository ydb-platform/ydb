#pragma once

#include "domain.h"

#include <ydb/core/protos/blobstorage_config.pb.h>

#include <util/generic/hash.h>

namespace NKikimr {

class TDomainsInfo::TDomain::TStoragePoolKinds
    : public THashMap<TString, NKikimrBlobStorage::TDefineStoragePool>
{
public:
    using THashMap::THashMap;
};

} // NKikimr
