#include "./dq_compute_actor_impl.h"

namespace NYql::NDq {

template<typename TDerived>
class TDqSyncComputeActorBase: public TDqComputeActorBase<TDerived> {
public:
    using TDqComputeActorBase<TDerived>::TDqComputeActorBase;
    static constexpr bool HasAsyncTaskRunner = false;
};

} //namespace NYql::NDq

