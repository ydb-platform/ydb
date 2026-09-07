#pragma once

#include "schemeshard_info_types_base.h"

#include <ydb/core/scheme/scheme_type_registry.h>

#include <optional>

namespace NKikimr {
namespace NSchemeShard {

struct TSequenceInfo : public TSimpleRefCount<TSequenceInfo> {
    using TPtr = TIntrusivePtr<TSequenceInfo>;

    explicit TSequenceInfo(ui64 alterVersion)
        : AlterVersion(alterVersion)
    { }

    TSequenceInfo(
        ui64 alterVersion,
        NKikimrSchemeOp::TSequenceDescription&& description,
        NKikimrSchemeOp::TSequenceSharding&& sharding);

    TPtr CreateNextVersion() {
        Y_ENSURE(AlterData == nullptr);
        TPtr result = new TSequenceInfo(*this);
        ++result->AlterVersion;
        this->AlterData = result;
        return result;
    }

    static bool ValidateCreate(const NKikimrSchemeOp::TSequenceDescription& p, TString& err);

    ui64 AlterVersion = 0;
    TSequenceInfo::TPtr AlterData = nullptr;
    NKikimrSchemeOp::TSequenceDescription Description;
    NKikimrSchemeOp::TSequenceSharding Sharding;

    ui64 SequenceShard = 0;
};

std::optional<std::pair<i64, i64>> ValidateSequenceType(const TString& sequenceName, const TString& dataType,
    const NKikimr::NScheme::TTypeRegistry& typeRegistry, bool pgTypesEnabled, TString& errStr);

}
}
