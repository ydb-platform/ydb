#pragma once

#include <library/cpp/object_factory/object_factory.h>
#include <library/cpp/getopt/last_getopt.h>

namespace NYdb::NGlobalPlugins {
    class IPlugin: public TAtomicRefCount<IPlugin> {
    public:
        using TPtr = TIntrusivePtr<IPlugin>;
        virtual ~IPlugin() = default;
        virtual void Start() = 0;
        virtual void Stop() = 0;
    };

    class IPluginInitilizer {
    public:
        virtual ~IPluginInitilizer() = default;
        virtual void SetupOpts(NLastGetopt::TOpts& opts) = 0;
        virtual IPlugin::TPtr CreatePlugin() const = 0;
    };

    using TPluginFactory = NObjectFactory::TObjectFactory<IPluginInitilizer, TString>;
}