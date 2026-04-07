#include "yql_ytflow_integration.h"

#include <util/system/yassert.h>


namespace NYql {

namespace {

[[noreturn]] void AbortUnimplemented(const char* methodName) {
    Y_ABORT(
        "Method %s is not implemented in IYtflowIntegration descendant",
        methodName);
}

} // anonymous namespace

TMaybe<bool> TEmptyYtflowIntegration::CanRead(
    const TExprNode& /*read*/,
    TExprContext& /*ctx*/
) {
    return Nothing();
}

TExprNode::TPtr TEmptyYtflowIntegration::WrapRead(
    const TExprNode::TPtr& /*read*/,
    TExprContext& /*ctx*/
) {
    AbortUnimplemented(__FUNCTION__);
}

TMaybe<bool> TEmptyYtflowIntegration::CanWrite(
    const TExprNode& /*write*/,
    TExprContext& /*ctx*/
) {
    return Nothing();
}

TExprNode::TPtr TEmptyYtflowIntegration::WrapWrite(
    const TExprNode::TPtr& /*write*/,
    TExprContext& /*ctx*/
) {
    AbortUnimplemented(__FUNCTION__);
}

TMaybe<bool> TEmptyYtflowIntegration::CanLookupRead(
    const TExprNode& /*read*/,
    const TVector<TStringBuf>& /*keys*/,
    ERowSelectionMode /*rowSelectionMode*/,
    TExprContext& /*ctx*/
) {
    return Nothing();
}

TExprNode::TPtr TEmptyYtflowIntegration::GetReadWorld(
    const TExprNode& /*read*/,
    TExprContext& /*ctx*/
) {
    AbortUnimplemented(__FUNCTION__);
}

TExprNode::TPtr TEmptyYtflowIntegration::GetWriteWorld(
    const TExprNode& /*write*/,
    TExprContext& /*ctx*/
) {
    AbortUnimplemented(__FUNCTION__);
}

TExprNode::TPtr TEmptyYtflowIntegration::UpdateWriteWorld(
    const TExprNode::TPtr& /*write*/,
    const TExprNode::TPtr& /*world*/,
    TExprContext& /*ctx*/
) {
    AbortUnimplemented(__FUNCTION__);
}

TExprNode::TPtr TEmptyYtflowIntegration::GetWriteContent(
    const TExprNode& /*write*/,
    TExprContext& /*ctx*/
) {
    AbortUnimplemented(__FUNCTION__);
}

TExprNode::TPtr TEmptyYtflowIntegration::UpdateWriteContent(
    const TExprNode::TPtr& /*write*/,
    const TExprNode::TPtr& /*content*/,
    TExprContext& /*ctx*/
) {
    AbortUnimplemented(__FUNCTION__);
}

void TEmptyYtflowIntegration::FillSourceSettings(
    const TExprNode& /*source*/,
    ::google::protobuf::Any& /*settings*/,
    TExprContext& /*ctx*/
) {
    AbortUnimplemented(__FUNCTION__);
}

void TEmptyYtflowIntegration::FillSinkSettings(
    const TExprNode& /*sink*/,
    ::google::protobuf::Any& /*settings*/,
    TExprContext& /*ctx*/
) {
    AbortUnimplemented(__FUNCTION__);
}

NKikimr::NMiniKQL::TRuntimeNode TEmptyYtflowIntegration::BuildLookupSourceArgs(
    const TExprNode& /*read*/,
    NCommon::TMkqlBuildContext& /*ctx*/
) {
    AbortUnimplemented(__FUNCTION__);
}

} // namespace NYql
