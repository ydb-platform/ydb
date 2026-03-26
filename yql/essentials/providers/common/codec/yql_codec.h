#pragma once

#include "yql_codec_buf.h"

#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/parser/pg_wrapper/interface/codec.h>
#include <yql/essentials/ast/yql_expr.h>

#include <util/generic/strbuf.h>
#include <util/generic/maybe.h>
#include <util/stream/output.h>

#include <library/cpp/yson/public.h>

#include <list>
#include <vector>

namespace NYT {
class TNode;
} // namespace NYT

namespace NYql::NCommon {

void WriteYsonValue(
    NYson::TYsonConsumerBase& writer,
    const NKikimr::NUdf::TUnboxedValuePod& value,
    NKikimr::NMiniKQL::TType* type,
    const TVector<ui32>* structPositions = nullptr);

TString WriteYsonValue(
    const NKikimr::NUdf::TUnboxedValuePod& value,
    NKikimr::NMiniKQL::TType* type,
    const TVector<ui32>* structPositions = nullptr,
    NYson::EYsonFormat format = NYson::EYsonFormat::Binary);

TMaybe<TVector<ui32>> CreateStructPositions(
    NKikimr::NMiniKQL::TType* inputType,
    const TVector<TString>* columns = nullptr);

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
        const NKikimr::NMiniKQL::THolderFactory* holderFactory = nullptr);
};

void SkipYson(char cmd, TInputBuf& buf);
void CopyYson(char cmd, TInputBuf& buf, TVector<char>& yson);
void CopyYsonWithAttrs(char cmd, TInputBuf& buf, TVector<char>& yson);
TStringBuf ReadNextString(char cmd, TInputBuf& buf);
NKikimr::NUdf::TUnboxedValue ReadYsonValue(NKikimr::NMiniKQL::TType* type, const NKikimr::NMiniKQL::THolderFactory& holderFactory, char cmd, TInputBuf& buf);

TMaybe<NKikimr::NUdf::TUnboxedValue> ParseYsonValue(const NKikimr::NMiniKQL::THolderFactory& holderFactory,
                                                    const TStringBuf& yson, NKikimr::NMiniKQL::TType* type, IOutputStream* err);

TMaybe<NKikimr::NUdf::TUnboxedValue> ParseYsonNodeInResultFormat(const NKikimr::NMiniKQL::THolderFactory& holderFactory,
                                                                 const NYT::TNode& node, NKikimr::NMiniKQL::TType* type, IOutputStream* err);

TExprNode::TPtr ValueToExprLiteral(const TTypeAnnotationNode* type, const NKikimr::NUdf::TUnboxedValuePod& value, TExprContext& ctx,
                                   TPositionHandle pos = {});

} // namespace NYql::NCommon
