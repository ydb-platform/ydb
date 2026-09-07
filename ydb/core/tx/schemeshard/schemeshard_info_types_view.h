#pragma once

#include "schemeshard_info_types_base.h"

#include <ydb/core/protos/yql_translation_settings.pb.h>

namespace NKikimr {
namespace NSchemeShard {

struct TViewInfo : TSimpleRefCount<TViewInfo> {
    using TPtr = TIntrusivePtr<TViewInfo>;

    ui64 AlterVersion = 0;
    TString QueryText;
    NYql::NProto::TTranslationSettings CapturedContext;
};

}
}
