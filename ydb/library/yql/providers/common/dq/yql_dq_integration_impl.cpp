#include "yql_dq_integration_impl.h"

namespace NYql {

ui64 TDqIntegrationBase::Partition(const TDqSettings&, size_t, const TExprNode&,
    TVector<TString>&, TString*, TExprContext&, bool) {
    return 0;
}

bool TDqIntegrationBase::CanRead(const TExprNode&, TExprContext&, bool) {
    return false;
}

TMaybe<ui64> TDqIntegrationBase::EstimateReadSize(ui64, ui32, const TExprNode &, TExprContext&) {
    return Nothing();
}

TExprNode::TPtr TDqIntegrationBase::WrapRead(const TDqSettings&, const TExprNode::TPtr& read, TExprContext&) {
    return read;
}

TMaybe<bool> TDqIntegrationBase::CanWrite(const TExprNode&, TExprContext&) {
    return Nothing();
}

TExprNode::TPtr TDqIntegrationBase::WrapWrite(const TExprNode::TPtr& write, TExprContext&) {
    return write;
}

void TDqIntegrationBase::RegisterMkqlCompiler(NCommon::TMkqlCallableCompilerBase&)  {
}

bool TDqIntegrationBase::CanFallback() {
    return false;
}

void TDqIntegrationBase::FillSourceSettings(const TExprNode&, ::google::protobuf::Any&, TString&) {
}

void TDqIntegrationBase::FillSinkSettings(const TExprNode&, ::google::protobuf::Any&, TString&) {
}

void TDqIntegrationBase::FillTransformSettings(const TExprNode&, ::google::protobuf::Any&) {
}

void TDqIntegrationBase::Annotate(const TExprNode&, THashMap<TString, TString>&) {
}

bool TDqIntegrationBase::PrepareFullResultTableParams(const TExprNode&, TExprContext&, THashMap<TString, TString>&, THashMap<TString, TString>&) {
    return false;
}

void TDqIntegrationBase::WriteFullResultTableRef(NYson::TYsonWriter&, const TVector<TString>&, const THashMap<TString, TString>&) {
}

} // namespace NYql
