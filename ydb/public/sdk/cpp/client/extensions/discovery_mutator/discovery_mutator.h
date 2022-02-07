#pragma once

#include <ydb/public/sdk/cpp/client/ydb_extension/extension.h>

namespace NDiscoveryMutator {

class TDiscoveryMutator: public NYdb::IExtension {
public:
    using IApi = NYdb::IDiscoveryMutatorApi;

    using TCb = NYdb::IDiscoveryMutatorApi::TMutatorCb;

    class TParams {
        friend class TDiscoveryMutator;
    public:
        TParams(TCb mutator)
            : Mutator_(std::move(mutator))
        { }

    private:
        TCb Mutator_;
    };

    TDiscoveryMutator(TParams params, IApi* api) {
        api->SetMutatorCb(std::move(params.Mutator_));
    }
};

}; // namespace NDiscoveryModifie
