#pragma once
#include <ydb/core/protos/subdomains.pb.h>
#include <util/system/types.h>

namespace NKikimr::NFlatTxCoordinator {

    class ICoordinatorHooks {
    protected:
        ~ICoordinatorHooks() = default;

    public:
        virtual bool PersistConfig(ui64 tabletId, const NKikimrSubDomains::TProcessingParams& config);

    public:
        static ICoordinatorHooks* Get();
        static void Set(ICoordinatorHooks* hooks);
    };

    class TCoordinatorHooksGuard {
    public:
        TCoordinatorHooksGuard(ICoordinatorHooks& hooks);
        ~TCoordinatorHooksGuard();
    };

} // namespace NKikimr::NFlatTxCoordinator
