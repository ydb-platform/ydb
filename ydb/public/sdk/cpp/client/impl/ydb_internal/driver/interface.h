#pragma once

#include <ydb/public/sdk/cpp/client/impl/ydb_internal/internal_header.h>

namespace NYdb {

class TDriver;
class TGRpcConnectionsImpl;

std::shared_ptr<TGRpcConnectionsImpl> CreateInternalInterface(const TDriver connection);

} // namespace NYdb
