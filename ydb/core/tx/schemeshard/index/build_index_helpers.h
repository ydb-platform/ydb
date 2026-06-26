#pragma once

#include <yql/essentials/public/issue/protos/issue_severity.pb.h>

#include <util/generic/string.h>

#include <vector>

#if defined LOG_T || \
    defined LOG_D || \
    defined LOG_I || \
    defined LOG_N || \
    defined LOG_W || \
    defined LOG_E
# error log macro redefinition
#endif

namespace NKikimr {

class TSerializedTableRange;

namespace NScheme {
struct TTypeInfoOrder;
}

namespace NSchemeShard {
bool PerformCrossShardUniqIndexValidation(const std::vector<NScheme::TTypeInfoOrder>& indexColumnTypeInfos, const std::vector<TString>& indexColumns, const std::vector<const TSerializedTableRange*>& ranges, TString& errorDesc);
}

}
