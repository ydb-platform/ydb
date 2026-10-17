#pragma once

#include "scheme.h"

#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>
#include <ydb/core/tx/replication/ydb_proxy/topic_message.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/public/purecalc/common/interface.h>
#include <yql/essentials/public/udf/udf_value.h>

namespace NKikimr::NReplication::NTransfer {

using namespace NYql::NPureCalc;
using namespace NKikimr::NMiniKQL;

struct TMessage {
    const ui32 PartitionId;
    const NKikimr::NReplication::TTopicMessage& Message;
};

class TMessageInputSpec: public TInputSpecBase {
public:
    TMessageInputSpec() = default;

    const TVector<NYT::TNode>& GetSchemas() const override;
    bool ProvidesBlocks() const override { return false; }
};

struct TOutputMessage {
    std::optional<TString> Table;
    NYql::NUdf::TUnboxedValue Value;
    NKikimr::NMiniKQL::TUnboxedValueBatch Data;
    size_t EstimateSize = 0;
};

class TMessageOutputSpec : public NYql::NPureCalc::TOutputSpecBase {
public:
    explicit TMessageOutputSpec(const TScheme::TPtr& tableScheme, const NYT::TNode& schema);

public:
    const NYT::TNode& GetSchema() const override;

    const TVector<NKikimrKqp::TKqpColumnMetadataProto>& GetStructColumns() const;
    size_t GetTargetTableIndex() const;

private:
    const TScheme::TPtr TableScheme;
    const NYT::TNode Schema;
};

class IProgramHolder : public NFq::IProgramHolder {
public:
    using TPtr = TIntrusivePtr<IProgramHolder>;

    virtual NYql::NPureCalc::TPullListProgram<TMessageInputSpec, TMessageOutputSpec>* GetProgram() = 0;
};

IProgramHolder::TPtr CreateProgramHolder(const TScheme::TPtr& tableScheme, const TString& sql);

}

namespace NYql::NPureCalc {

template<>
struct TInputSpecTraits<NKikimr::NReplication::NTransfer::TMessageInputSpec> {

    static const constexpr bool IsPartial = false;

    static const constexpr bool SupportPullStreamMode = false;
    static const constexpr bool SupportPullListMode = true;
    static const constexpr bool SupportPushStreamMode = false;

    using TInput = NKikimr::NReplication::NTransfer::TMessage;
    using TInputSpecType = NKikimr::NReplication::NTransfer::TMessageInputSpec;

    static void PreparePullListWorker(const TInputSpecType&, IPullListWorker*, THolder<IStream<TInput*>>);
};

template <>
struct TOutputSpecTraits<NKikimr::NReplication::NTransfer::TMessageOutputSpec> {
    static const constexpr bool IsPartial = false;

    static const constexpr bool SupportPullStreamMode = false;
    static const constexpr bool SupportPullListMode = true;
    static const constexpr bool SupportPushStreamMode = false;

    using TOutputItemType = NKikimr::NReplication::NTransfer::TOutputMessage*;
    using TPullListReturnType = THolder<IStream<TOutputItemType>>;

    static TPullListReturnType ConvertPullListWorkerToOutputType(
        const NKikimr::NReplication::NTransfer::TMessageOutputSpec& outputSpec,
        TWorkerHolder<IPullListWorker> worker
    );
};

}
