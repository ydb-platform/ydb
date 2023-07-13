#pragma once

#include <ydb/core/kqp/gateway/kqp_gateway.h>
#include <ydb/core/protos/kqp.pb.h>

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/dq/common/dq_serialized_batch.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>

namespace NKikimr {
namespace NKqp {

class TKqpProtoBuilder : private TNonCopyable {
public:
    TKqpProtoBuilder(const NMiniKQL::IFunctionRegistry& funcRegistry);

    TKqpProtoBuilder(NMiniKQL::TScopedAlloc* alloc, NMiniKQL::TTypeEnvironment* typeEnv,
        NMiniKQL::THolderFactory* holderFactory);

    ~TKqpProtoBuilder();

    Ydb::ResultSet BuildYdbResultSet(TVector<NYql::NDq::TDqSerializedBatch>&& data,
        NKikimr::NMiniKQL::TType* srcRowType, const TVector<ui32>* columnOrder = nullptr);

private:
    NMiniKQL::TScopedAlloc* Alloc = nullptr;
    NMiniKQL::TTypeEnvironment* TypeEnv = nullptr;
    NMiniKQL::THolderFactory* HolderFactory = nullptr;

    struct TSelfHosted {
        NMiniKQL::TScopedAlloc Alloc{__LOCATION__};
        NMiniKQL::TTypeEnvironment TypeEnv;
        NMiniKQL::TMemoryUsageInfo MemInfo;
        NMiniKQL::THolderFactory HolderFactory;

        explicit TSelfHosted(const NMiniKQL::IFunctionRegistry& funcRegistry);
    };
    THolder<TSelfHosted> SelfHosted;
};

} // namespace NKqp
} // namespace NKikimr
