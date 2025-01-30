#pragma once

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/tablet/tablet_setup.h>

namespace NKikimr {
namespace NFake {

    using TStorageInfo = TTabletStorageInfo;

    struct TStarter {
        using TMake = TTabletSetupInfo::TTabletCreationFunc;

        IActor* Do(TActorId user, ui32 retry, ui32 tablet, TMake make, ui32 followerId = 0) noexcept
        {
            const auto simple = TMailboxType::Simple;

            auto *info = MakeTabletInfo(tablet);
            auto *setup = new TTabletSetupInfo(make, simple, 0, simple, 0);

            return new NFake::TOwner(user, retry, info, setup, followerId);
        }

        virtual TStorageInfo* MakeTabletInfo(ui64 tablet) noexcept
        {
            const auto none = TErasureType::ErasureNone;

            auto *info = new TStorageInfo;

            info->TabletID = tablet;
            info->TabletType = TTabletTypes::Dummy;
            info->Channels.resize(4);

            for (auto num: xrange(info->Channels.size())) {
                info->Channels[num].Channel = num;
                info->Channels[num].Type = TBlobStorageGroupType(none);
                info->Channels[num].History.resize(1);
                info->Channels[num].History[0].FromGeneration = 0;
                info->Channels[num].History[0].GroupID = num;
            }

            return info;
        }
    };

}
}
