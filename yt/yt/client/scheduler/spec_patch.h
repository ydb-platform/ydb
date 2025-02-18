#pragma once

#include "public.h"

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TSpecPatch
    : public NYTree::TYsonStruct
{
public:
    NYPath::TYPath Path;
    NYTree::INodePtr Value;

    REGISTER_YSON_STRUCT(TSpecPatch);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSpecPatch)

void ToProto(NProto::TSpecPatch* protoPatch, const TSpecPatchPtr& patch);
void FromProto(TSpecPatchPtr patch, const NProto::TSpecPatch* protoPatch);

void FormatValue(TStringBuilderBase* builder, const TSpecPatchPtr& patch, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
