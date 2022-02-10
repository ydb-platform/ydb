#include "yql_s3_provider_impl.h" 
 
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/providers/s3/expr_nodes/yql_s3_expr_nodes.h>
 
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/provider/yql_data_provider_impl.h>
 
#include <ydb/library/yql/utils/log/log.h>
 
namespace NYql { 
 
using namespace NNodes; 
 
namespace { 
 
class TS3DataSinkExecTransformer : public TExecTransformerBase { 
public: 
    TS3DataSinkExecTransformer(TS3State::TPtr state) 
        : State_(state) 
    { 
        AddHandler({TCoCommit::CallableName()}, RequireFirst(), Pass()); 
    } 
 
private: 
    TS3State::TPtr State_; 
}; 
 
} 
 
THolder<TExecTransformerBase> CreateS3DataSinkExecTransformer(TS3State::TPtr state) { 
    return THolder(new TS3DataSinkExecTransformer(state)); 
} 
 
} // namespace NYql 
