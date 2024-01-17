#include "resource_manager.h"

#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>

#include <ydb/library/yql/providers/dq/global_worker_manager/coordination_helper.h>

namespace NYql {
    using namespace NActors;

    class TResourceManagerStub: public TActor<TResourceManagerStub> {
    public:
        TResourceManagerStub()
            : TActor<TResourceManagerStub>(&TResourceManagerStub::Handler)
        { }

        void Handler(STFUNC_SIG) {
            Y_UNUSED(ev);
        }
    };

    IActor* CreateResourceManager(const TResourceManagerOptions& options, const ICoordinationHelper::TPtr& coordinator) {
        Y_UNUSED(options);
#ifdef WIN32
        return new TResourceManagerStub();
#else
        IActor* CreateYtResourceManager(
            const TResourceManagerOptions& options,
            const ICoordinationHelper::TPtr& coordinator);
        return CreateYtResourceManager(options, coordinator);
#endif
    }


    NActors::IActor* CreateResourceUploader(const TResourceManagerOptions& options, const ICoordinationHelper::TPtr& coordinator) {
        Y_UNUSED(options);
#ifdef WIN32
        return new TResourceManagerStub();
#else
        IActor* CreateYtResourceUploader(
            const TResourceManagerOptions& options,
            const ICoordinationHelper::TPtr& coordinator);
        return CreateYtResourceUploader(options, coordinator);
#endif
    }

    NActors::IActor* CreateResourceDownloader(const TResourceManagerOptions& options, const ICoordinationHelper::TPtr& coordinator) {
        Y_UNUSED(options);
#ifdef WIN32
        return new TResourceManagerStub();
#else
        IActor* CreateYtResourceDownloader(
            const TResourceManagerOptions& options, const ICoordinationHelper::TPtr& coordinator);
        return CreateYtResourceDownloader(options, coordinator);
#endif
    }

    NActors::IActor* CreateResourceCleaner(const TResourceManagerOptions& options, const TIntrusivePtr<ICoordinationHelper>& coordinator) {
        Y_UNUSED(options);
#ifdef WIN32
        return new TResourceManagerStub();
#else
        IActor* CreateYtResourceCleaner(
            const TResourceManagerOptions& options,
            const TIntrusivePtr<ICoordinationHelper>& coordinator);
        return CreateYtResourceCleaner(options, coordinator);
#endif
    }
}
