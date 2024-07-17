#pragma once

#include "audit_log.h"
#include "base_handler.h"
#include "cms.h"
#include "cms_state.h"

#include <util/generic/set.h>
#include <util/generic/string.h>

namespace NKikimr::NCms {

constexpr const char *WALLE_CMS_USER = "Wall-E";
constexpr const char *WALLE_API_URL_PREFIX = "/api/walle/v11/";

constexpr i32 WALLE_DEFAULT_PRIORITY = 20;
constexpr i32 WALLE_SOFT_MAINTENANCE_PRIORITY = 50;

IActor *CreateWalleAdapter(TEvCms::TEvWalleCreateTaskRequest::TPtr &ev, TActorId cms);
IActor *CreateWalleAdapter(TEvCms::TEvWalleListTasksRequest::TPtr &ev, const TCmsStatePtr state);
IActor *CreateWalleAdapter(TEvCms::TEvWalleCheckTaskRequest::TPtr &ev, const TCmsStatePtr state, TActorId cms);
IActor *CreateWalleAdapter(TEvCms::TEvWalleRemoveTaskRequest::TPtr &ev, const TCmsStatePtr state, TActorId cms);

IActor *CreateWalleApiHandler(NMon::TEvHttpInfo::TPtr &event);

class TWalleApiHandler : public TApiMethodHandlerBase {
public:
    virtual ~TWalleApiHandler() = default;

    virtual IActor *CreateHandlerActor(NMon::TEvHttpInfo::TPtr &event) {
        return CreateWalleApiHandler(event);
    }
};

inline void WalleAuditLog(const IEventBase *request, const IEventBase *response, const TActorContext &ctx) {
    AuditLog("Wall-E adapter", request, response, ctx);
}

} // namespace NKikimr::NCms
