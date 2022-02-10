#pragma once 
#include "dq_output.h" 
 
#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node.h>
 
namespace NYql::NDq { 
 
struct TDqSinkStats : TDqOutputStats {
    const ui64 OutputIndex;

    explicit TDqSinkStats(ui64 outputIndex)
        : OutputIndex(outputIndex) {}
};

class IDqSink : public IDqOutput { 
public: 
    using TPtr = TIntrusivePtr<IDqSink>; 
 
    virtual ui64 GetOutputIndex() const = 0; 
 
    // Pop data to send. Return estimated size of returned data. 
    [[nodiscard]] 
    virtual ui64 Pop(NKikimr::NMiniKQL::TUnboxedValueVector& batch, ui64 bytes) = 0; 
    // Pop chechpoint. Checkpoints may be taken from sink even after it is finished. 
    [[nodiscard]] 
    virtual bool Pop(NDqProto::TCheckpoint& checkpoint) = 0; 

    virtual const TDqSinkStats* GetStats() const = 0;
}; 
 
IDqSink::TPtr CreateDqSink(ui64 outputIndex, NKikimr::NMiniKQL::TType* outputType, ui64 maxStoredBytes,
    bool collectProfileStats);
 
} // namespace NYql::NDq 
