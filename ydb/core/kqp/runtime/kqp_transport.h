#pragma once

#include <ydb/core/kqp/gateway/kqp_gateway.h>
#include <ydb/core/protos/kqp.pb.h>

#include <ydb/library/yql/ast/yql_expr.h>
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

    static NKikimrMiniKQL::TType ApplyColumnHints(const NKikimrMiniKQL::TType& srcRowType,
        const TVector<TString>& columnHints);

    void BuildValue(const TVector<NYql::NDqProto::TData>& data, const NKikimrMiniKQL::TType& type,
        NKikimrMiniKQL::TResult* result);

    void BuildValue(NMiniKQL::TUnboxedValueVector& rows, const NKikimrMiniKQL::TType& type,
        NKikimrMiniKQL::TResult* result);

    void BuildStream(const TVector<NYql::NDqProto::TData>& data, const NKikimrMiniKQL::TType& srcRowType,
        const NKikimrMiniKQL::TType* dstRowType, NKikimrMiniKQL::TResult* result);

    void BuildStream(NMiniKQL::TUnboxedValueVector& rows, const NKikimrMiniKQL::TType& srcRowType,
        const NKikimrMiniKQL::TType* dstRowType, NKikimrMiniKQL::TResult* result);

    Ydb::ResultSet BuildYdbResultSet(const TVector<NYql::NDqProto::TData>& data,
        const NKikimrMiniKQL::TType& srcRowType, const NKikimrMiniKQL::TType* dstRowType = nullptr);

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
