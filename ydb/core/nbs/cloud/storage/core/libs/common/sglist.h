#pragma once

#include "public.h"

#include "block_data_ref.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

#include <ydb/library/actors/util/rope.h>

#include <util/generic/strbuf.h>
#include <util/generic/vector.h>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

using TSgList = TVector<TBlockDataRef>;

size_t SgListGetSize(const TSgList& sglist);
size_t SgListCopy(const TSgList& src, const TSgList& dst);
size_t SgListCopy(TBlockDataRef src, const TSgList& dst);
size_t SgListCopy(const TSgList& src, TBlockDataRef dst);

TResultOrError<TSgList> SgListNormalize(TBlockDataRef buffer, ui32 blockSize);
TResultOrError<TSgList> SgListNormalize(TSgList sglist, ui32 blockSize);

TSgList CreateSgList(const TRope& rope);
TSgList CreateSgList(const TVector<TRope>& ropes);

}   // namespace NYdb::NBS
