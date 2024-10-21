#pragma once
#include "info.h"

#include <ydb/core/tx/columnshard/engines/scheme/tier_info.h>

#include <ydb/services/metadata/service.h>

#include <library/cpp/json/writer/json_value.h>

namespace NKikimr::NColumnShard::NTiers {

class TTieringRule: public TTieringRuleInfo {
public:
    static NMetadata::IClassBehaviour::TPtr GetBehaviour();

    static TString GetTypeId() {
        return "TIERING_RULE";
    }

    bool ContainsTier(const TString& tierName) const;
    NKikimr::NOlap::TTiering BuildOlapTiers() const;
};
}
