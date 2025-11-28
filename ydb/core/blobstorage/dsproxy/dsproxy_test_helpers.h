#pragma once

#include "defs.h"
#include "dsproxy.h"

namespace NKikimr {

struct TEvExplicitMultiPut : public TEventLocal<TEvExplicitMultiPut, TEvBlobStorage::EvExplicitMultiPut> {
public:
    TEvExplicitMultiPut(TBlobStorageGroupMultiPutParameters params)
        : Parameters(params)
    {}

public:
    TBlobStorageGroupMultiPutParameters Parameters;
};

} // namespace NKikimr
