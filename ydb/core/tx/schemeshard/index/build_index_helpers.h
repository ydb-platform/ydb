#pragma once

#include <yql/essentials/public/issue/protos/issue_severity.pb.h>

#include <util/generic/string.h>

#include <vector>

namespace NKikimr {

class TSerializedTableRange;

namespace NScheme {
struct TTypeInfoOrder;
}

namespace NSchemeShard {
bool PerformCrossShardUniqIndexValidation(const std::vector<NScheme::TTypeInfoOrder>& indexColumnTypeInfos, const std::vector<TString>& indexColumns, const std::vector<const TSerializedTableRange*>& ranges, TString& errorDesc);
}

}
