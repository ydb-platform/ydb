#include "spec_patch.h"

#include <yt/yt_proto/yt/client/scheduler/proto/spec_patch.pb.h>

#include <yt/yt/core/yson/protobuf_helpers.h>

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
    protoPatch->set_value(ToProto(ConvertToYsonString(patch->Value)));
}

void FromProto(TSpecPatchPtr patch, const NProto::TSpecPatch* protoPatch)
{
    patch->Path = protoPatch->path();
    patch->Value = ConvertToNode(TYsonString(protoPatch->value()));
}

void FromProto(TSpecPatchPtr* patch, const NProto::TSpecPatch& protoPatch)
{
    *patch = New<TSpecPatch>();
    FromProto(*patch, &protoPatch);
}

void FormatValue(TStringBuilderBase* builder, const TSpecPatchPtr& patch, TStringBuf /*spec*/)
{
    builder->AppendString(ConvertToYsonString(patch).ToString());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
