#include "yql_solomon_provider_impl.h"

#include <ydb/library/yql/providers/common/provider/yql_provider.h>

namespace NYql {

using namespace NNodes;

class TSolomonDataSourceTypeAnnotationTransformer : public TVisitorTransformerBase {
public:
    TSolomonDataSourceTypeAnnotationTransformer(TSolomonState::TPtr state)
        : TVisitorTransformerBase(true)
        , State_(state)
    {
    }

private:
    TSolomonState::TPtr State_;
};

THolder<TVisitorTransformerBase> CreateSolomonDataSourceTypeAnnotationTransformer(TSolomonState::TPtr state) {
    return THolder(new TSolomonDataSourceTypeAnnotationTransformer(state));
}

} // namespace NYql
