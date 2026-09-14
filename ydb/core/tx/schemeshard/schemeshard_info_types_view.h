#pragma once

#include <ydb/core/protos/yql_translation_settings.pb.h>

#include <util/generic/ptr.h>
#include <util/generic/string.h>

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
