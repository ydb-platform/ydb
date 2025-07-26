#pragma once

#include "scheme.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/public/purecalc/common/interface.h>
#include <yql/essentials/public/udf/udf_value.h>

namespace NKikimr::NReplication::NTransfer {

using namespace NYql::NPureCalc;
using namespace NKikimr::NMiniKQL;

struct TMessage {
    TString Data;
    TString MessageGroupId;
    ui64 Offset = 0;
    ui32 Partition = 0;
    TString ProducerId;
    ui64 SeqNo = 0;
};

class TMessageInputSpec: public TInputSpecBase {
public:
    /**
     * Build input spec and associate the given message descriptor.
     */
    explicit TMessageInputSpec() =  default;

public:
    const TVector<NYT::TNode>& GetSchemas() const override;
    bool ProvidesBlocks() const override { return false; }
};

struct TOutputMessage {
    std::optional<TString> Table;
    NYql::NUdf::TUnboxedValue Value;
    NKikimr::NMiniKQL::TUnboxedValueBatch Data;
};

class TMessageOutputSpec : public NYql::NPureCalc::TOutputSpecBase {
public:
    explicit TMessageOutputSpec(const TScheme& tableScheme, const NYT::TNode& schema);

public:
    const NYT::TNode& GetSchema() const override;

    const TVector<NKikimrKqp::TKqpColumnMetadataProto>& GetTableColumns() const;
    const TVector<NKikimrKqp::TKqpColumnMetadataProto>& GetStructColumns() const;

private:
    const TScheme TableScheme;
    const NYT::TNode Schema;
};

}

namespace NYql::NPureCalc {

template<>
struct TInputSpecTraits<NKikimr::NReplication::NTransfer::TMessageInputSpec> {

    static const constexpr bool IsPartial = false;

    static const constexpr bool SupportPullStreamMode = true;
    static const constexpr bool SupportPullListMode = true;
    static const constexpr bool SupportPushStreamMode = true;

    using TInput = NKikimr::NReplication::NTransfer::TMessage;
    using TInputSpecType = NKikimr::NReplication::NTransfer::TMessageInputSpec;
    using TConsumerType = THolder<IConsumer<TInput*>>;

    static void PreparePullStreamWorker(const TInputSpecType&, IPullStreamWorker*, THolder<IStream<TInput*>>);
    static void PreparePullListWorker(const TInputSpecType&, IPullListWorker*, THolder<IStream<TInput*>>);
    static TConsumerType MakeConsumer(const TInputSpecType&, TWorkerHolder<IPushStreamWorker>);
};

template <>
struct TOutputSpecTraits<NKikimr::NReplication::NTransfer::TMessageOutputSpec> {
    static const constexpr bool IsPartial = false;

    static const constexpr bool SupportPullListMode = true;

    using TOutputItemType = NKikimr::NReplication::NTransfer::TOutputMessage*;
    using TPullStreamReturnType = THolder<IStream<TOutputItemType>>;
    using TPullListReturnType = THolder<IStream<TOutputItemType>>;

    static TPullListReturnType ConvertPullListWorkerToOutputType(
        const NKikimr::NReplication::NTransfer::TMessageOutputSpec& outputSpec,
        TWorkerHolder<IPullListWorker> worker
    );
};

}
