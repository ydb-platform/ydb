#pragma once

#include <ydb/core/path_aliasing/context/path_context.h>

namespace NKikimrSchemeOp {
    class TModifyScheme;
} // namespace NKikimrSchemeOp

namespace NKikimrClient {
    class TCmsRequest;
    class TConsoleRequest;
} // namespace NKikimrClient

namespace NKikimr::NMsgBusProxy {

    // Only the legacy public message-bus owner calls this; internal scheme
    // transactions are already resolved and must never pass through it.
    TConclusionStatus NormalizeMessageBusSchemaPaths(
        NKikimrSchemeOp::TModifyScheme& scheme, const NPathAliasing::TPathContext& context);

    // Tenant-management requests are public resource owners. Configuration scopes,
    // deployment identifiers and internal console commands are not schema operands.
    TConclusionStatus NormalizeMessageBusDatabasePaths(
        NKikimrClient::TConsoleRequest& request, const NPathAliasing::TPathContext& context);

    TConclusionStatus NormalizeMessageBusMaintenancePaths(
        NKikimrClient::TCmsRequest& request, const NPathAliasing::TPathContext& context);

} // namespace NKikimr::NMsgBusProxy
