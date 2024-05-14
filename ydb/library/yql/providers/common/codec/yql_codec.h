#pragma once

#include "yql_codec_buf.h"

#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/codec.h>
#include <ydb/library/yql/ast/yql_expr.h>

#include <util/generic/strbuf.h>
#include <util/generic/maybe.h>
#include <util/stream/output.h>

#include <library/cpp/yson/public.h>

#include <list>
#include <vector>

namespace NYT {
    class TNode;
}

namespace NYql {
namespace NCommon {

void WriteYsonValue(
    NYson::TYsonConsumerBase& writer,
    const NKikimr::NUdf::TUnboxedValuePod& value,
    NKikimr::NMiniKQL::TType* type,
    const TVector<ui32>* structPositions = nullptr
);

TString WriteYsonValue(
    const NKikimr::NUdf::TUnboxedValuePod& value,
    NKikimr::NMiniKQL::TType* type,
    const TVector<ui32>* structPositions = nullptr,
    NYson::EYsonFormat format = NYson::EYsonFormat::Binary
);

TMaybe<TVector<ui32>> CreateStructPositions(
    NKikimr::NMiniKQL::TType* inputType,
    const TVector<TString>* columns = nullptr
);

NYT::TNode ValueToNode(const NKikimr::NUdf::TUnboxedValuePod& value, NKikimr::NMiniKQL::TType* type);
TExprNode::TPtr NodeToExprLiteral(TPositionHandle pos, const TTypeAnnotationNode& type, const NYT::TNode& node, TExprContext& ctx);

struct TCodecContext {
    const NKikimr::NMiniKQL::TTypeEnvironment& Env;
    NKikimr::NMiniKQL::TProgramBuilder Builder;
    const NKikimr::NMiniKQL::THolderFactory* HolderFactory; // lazy initialized
    std::list<std::vector<size_t>> StructReorders;

    TCodecContext(
        const NKikimr::NMiniKQL::TTypeEnvironment& env,
        const NKikimr::NMiniKQL::IFunctionRegistry& functionRegistry,
        const NKikimr::NMiniKQL::THolderFactory* holderFactory = nullptr
    );
};

void SkipYson(char cmd, TInputBuf& buf);
void CopyYson(char cmd, TInputBuf& buf, TVector<char>& yson);
void CopyYsonWithAttrs(char cmd, TInputBuf& buf, TVector<char>& yson);
NKikimr::NUdf::TUnboxedValue ReadYsonValue(NKikimr::NMiniKQL::TType* type,  ui64 nativeYtTypeFlags, const NKikimr::NMiniKQL::THolderFactory& holderFactory, char cmd, TInputBuf& buf, bool isTableFormat);

TMaybe<NKikimr::NUdf::TUnboxedValue> ParseYsonValue(const NKikimr::NMiniKQL::THolderFactory& holderFactory,
    const TStringBuf& yson, NKikimr::NMiniKQL::TType* type,  ui64 nativeYtTypeFlags, IOutputStream* err, bool isTableFormat);
TMaybe<NKikimr::NUdf::TUnboxedValue> ParseYsonNode(const NKikimr::NMiniKQL::THolderFactory& holderFactory,
    const NYT::TNode& node, NKikimr::NMiniKQL::TType* type,  ui64 nativeYtTypeFlags, IOutputStream* err);

TMaybe<NKikimr::NUdf::TUnboxedValue> ParseYsonNodeInResultFormat(const NKikimr::NMiniKQL::THolderFactory& holderFactory,
    const NYT::TNode& node, NKikimr::NMiniKQL::TType* type, IOutputStream* err);

extern "C" void ReadYsonContainerValue(NKikimr::NMiniKQL::TType* type, ui64 nativeYtTypeFlags,
    const NKikimr::NMiniKQL::THolderFactory& holderFactory, NKikimr::NUdf::TUnboxedValue& value, NCommon::TInputBuf& buf,
    bool wrapOptional);

void SkipSkiffField(NKikimr::NMiniKQL::TType* type, ui64 nativeYtTypeFlags, TInputBuf& buf);

NKikimr::NUdf::TUnboxedValue ReadSkiffNativeYtValue(NKikimr::NMiniKQL::TType* type, ui64 nativeYtTypeFlags,
    const NKikimr::NMiniKQL::THolderFactory& holderFactory, TInputBuf& buf);

NKikimr::NUdf::TUnboxedValue ReadSkiffData(NKikimr::NMiniKQL::TType* type, ui64 nativeYtTypeFlags, NCommon::TInputBuf& buf);
extern "C" void ReadContainerNativeYtValue(NKikimr::NMiniKQL::TType* type, ui64 nativeYtTypeFlags,
    const NKikimr::NMiniKQL::THolderFactory& holderFactory, NKikimr::NUdf::TUnboxedValue& value, NCommon::TInputBuf& buf,
    bool wrapOptional);

extern "C" void WriteYsonContainerValue(NKikimr::NMiniKQL::TType* type, ui64 nativeYtTypeFlags,
    const NKikimr::NUdf::TUnboxedValuePod& value, NCommon::TOutputBuf& buf);

void WriteSkiffData(NKikimr::NMiniKQL::TType* type, ui64 nativeYtTypeFlags, const NKikimr::NUdf::TUnboxedValuePod& value, NCommon::TOutputBuf& buf);

void WriteSkiffNativeYtValue(NKikimr::NMiniKQL::TType* type, ui64 nativeYtTypeFlags,
    const NKikimr::NUdf::TUnboxedValuePod& value, NCommon::TOutputBuf& buf);

extern "C" void WriteContainerNativeYtValue(NKikimr::NMiniKQL::TType* type, ui64 nativeYtTypeFlags,
    const NKikimr::NUdf::TUnboxedValuePod& value, NCommon::TOutputBuf& buf);

void WriteYsonValueInTableFormat(TOutputBuf& buf, NKikimr::NMiniKQL::TType* type, ui64 nativeYtTypeFlags, const NKikimr::NUdf::TUnboxedValuePod& value, bool topLevel);

TExprNode::TPtr ValueToExprLiteral(const TTypeAnnotationNode* type, const NKikimr::NUdf::TUnboxedValuePod& value, TExprContext& ctx,
    TPositionHandle pos = {});

} // namespace NCommon
} // namespace NYql
