#pragma once

#include "schemeshard_info_types_base.h"

namespace NKikimr {
namespace NSchemeShard {

struct TSecretInfo : TSimpleRefCount<TSecretInfo> {
    using TPtr = TIntrusivePtr<TSecretInfo>;

    TSecretInfo(const ui64 alterVersion)
        : AlterVersion(alterVersion)
    {
    }

    TSecretInfo(const ui64 alterVersion, NKikimrSchemeOp::TSecretDescription&& desc)
        : AlterVersion(alterVersion)
        , Description(std::move(desc))
    {
    }

    TPtr CreateNextVersion() {
        Y_ENSURE(AlterData == nullptr);

        TPtr result = new TSecretInfo(*this);
        ++result->AlterVersion;
        this->AlterData = result;

        return result;
    }

    static TPtr New() {
        return new TSecretInfo(0);
    }

    static TPtr Create(NKikimrSchemeOp::TSecretDescription&& desc) {
        TPtr result = New();
        TPtr alterData = result->CreateNextVersion();
        alterData->Description = std::move(desc);

        return result;
    }

    ui64 AlterVersion = 0;
    TSecretInfo::TPtr AlterData = nullptr;
    NKikimrSchemeOp::TSecretDescription Description;
};

}
}
