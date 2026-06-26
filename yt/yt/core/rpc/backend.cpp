#include "backend.h"

#include <yt/yt/core/misc/collection_helpers.h>

#include <library/cpp/yt/memory/leaky_singleton.h>

#include <library/cpp/yt/threading/atomic_object.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

class TBackendRegistryImpl
{
public:
    static TBackendRegistryImpl* Get()
    {
        return LeakySingleton<TBackendRegistryImpl>();
    }

    std::vector<IBackend*> GetBackends()
    {
        return ProtocolToBackend_.Read([] (const auto& backends) {
            std::vector<IBackend*> result;
            result.reserve(backends.size());
            for (auto [_, backend] : backends) {
                result.push_back(backend);
            }
            return result;
        });
    }

    IBackend* FindBackend(TStringBuf protocol)
    {
        return ProtocolToBackend_.Read([&] (const auto& backends) {
            return GetOrDefault(backends, protocol);
        });
    }

    void RegisterBackend(IBackend* backend)
    {
        ProtocolToBackend_.Transform([&] (auto& backends) {
            EmplaceOrCrash(backends, backend->GetProtocol(), backend);
        });
    }

private:
    NThreading::TAtomicObject<THashMap<TStringBuf, IBackend*>> ProtocolToBackend_;

    TBackendRegistryImpl() = default;

    DECLARE_LEAKY_SINGLETON_FRIEND()
};

////////////////////////////////////////////////////////////////////////////////

std::vector<IBackend*> TBackendRegistry::GetBackends()
{
    return TBackendRegistryImpl::Get()->GetBackends();
}

IBackend* TBackendRegistry::FindBackend(TStringBuf protocol)
{
    return TBackendRegistryImpl::Get()->FindBackend(protocol);
}

void TBackendRegistry::RegisterBackend(IBackend* backend)
{
    TBackendRegistryImpl::Get()->RegisterBackend(backend);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
