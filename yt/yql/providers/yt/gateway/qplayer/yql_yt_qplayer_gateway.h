#pragma once
#include <yt/yql/providers/yt/provider/yql_yt_gateway.h>
#include <yql/essentials/core/qplayer/storage/interface/yql_qstorage.h>
#include <yql/essentials/core/file_storage/file_storage.h>

#include <library/cpp/random_provider/random_provider.h>

namespace NYql {

IYtGateway::TPtr WrapYtGatewayWithQContext(IYtGateway::TPtr gateway, const TQContext& qContext,
    const TIntrusivePtr<IRandomProvider>& randomProvider, const TFileStoragePtr& fileStorage);

}