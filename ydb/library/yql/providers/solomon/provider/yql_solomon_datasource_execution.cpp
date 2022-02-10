#include "yql_solomon_provider_impl.h"

namespace NYql {

using namespace NNodes;

class TSolomonDataSourceExecTransformer : public TExecTransformerBase {
public:
    TSolomonDataSourceExecTransformer(TSolomonState::TPtr state)
        : State_(state)
    {
    }

private:
    TSolomonState::TPtr State_;
};

THolder<TExecTransformerBase> CreateSolomonDataSourceExecTransformer(TSolomonState::TPtr state) {
    return THolder(new TSolomonDataSourceExecTransformer(state));
}

} // namespace NYql
