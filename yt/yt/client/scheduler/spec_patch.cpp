#include "spec_patch.h"

#include <yt/yt_proto/yt/client/scheduler/proto/spec_patch.pb.h>

namespace NYT::NScheduler {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TSpecPatch::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path);
    registrar.Parameter("value", &TThis::Value);
}

void ToProto(NProto::TSpecPatch* protoPatch, const TSpecPatchPtr& patch)
{
    using NYT::ToProto;
    protoPatch->set_path(patch->Path);
    protoPatch->set_value(ConvertToYsonString(patch->Value).ToString());
}

void FromProto(TSpecPatchPtr patch, const NProto::TSpecPatch* protoPatch)
{
    patch->Path = protoPatch->path();
    patch->Value = ConvertToNode(TYsonString(protoPatch->value()));
}

void FormatValue(TStringBuilderBase* builder, const TSpecPatchPtr& patch, TStringBuf /*spec*/)
{
    builder->AppendString(ConvertToYsonString(patch).ToString());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
