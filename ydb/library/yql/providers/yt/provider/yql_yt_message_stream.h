#pragma once
#include "yql_yt_message_stream_integration.h"
#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>
namespace NYql {
TIntrusivePtr<IDataProvider> WrapYtDataSourceWithMessageStreams(
    TIntrusivePtr<IDataProvider> tableSource,
    std::shared_ptr<IYtMessageStreamIntegration> streams);
std::shared_ptr<IYtMessageStreamIntegration> CreateYtMessageStreamIntegration(IStructuredTokenCredentialsFactory::TPtr credentialsFactory);
}
