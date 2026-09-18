#pragma once

#include <ydb/core/nbs/nbs1_compat_api/cloud/blockstore/libs/service/public.h>

#include <util/generic/ptr.h>

namespace NYdbGrpc {
    class IGRpcService;
}

namespace NKikimr::NGRpcService {

    TIntrusivePtr<NYdbGrpc::IGRpcService> CreateClassicNbsGrpcService(
        NYdb::NBS::NNbs1CompatApi::NBlockStore::IBlockStorePtr blockStore);

} // namespace NKikimr::NGRpcService
