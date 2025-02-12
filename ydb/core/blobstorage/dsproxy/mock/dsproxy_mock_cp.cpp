#include "dsproxy_mock_cp.h"
#include "model.h"

namespace NKikimr::NFake {

    TProxyDSCP::TProxyDSCP()
    {}

    void TProxyDSCP::AddMock(TGroupId groupId, TProxyDS* model) {
        TGuard<TMutex> guard(Mutex);
        Models[groupId] = model;
    }

    void TProxyDSCP::RemoveMock(TGroupId groupId) {
        TGuard<TMutex> guard(Mutex);
        Models.erase(groupId);
    }

    TProxyDS* TProxyDSCP::GetMock(TGroupId groupId) {
        TGuard<TMutex> guard(Mutex);
        return Models[groupId];
    }

    TMap<TGroupId, TProxyDS*> TProxyDSCP::GetMocks() {
        TGuard<TMutex> guard(Mutex);
        return Models;
    }

    std::unique_ptr<TProxyDSCP> CreateProxyDSCP() {
        return std::make_unique<TProxyDSCP>();
    }

    std::unique_ptr<TProxyDSCP> TProxyDS::Cp = CreateProxyDSCP();

}
