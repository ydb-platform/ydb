#pragma once

#include <ydb/core/base/logoblob.h>
#include <ydb/core/protos/blob_depot_config.pb.h>

#include <util/random/entropy.h>
#include <util/random/mersenne.h>
#include <util/random/random.h>
#include <util/system/env.h>

#include <vector>
#include <map>

struct TBlobInfo {
    enum EStatus {
        NONEXISTENT,
        WRITTEN,
        COLLECTED,
        UNKNOWN,
    };

    TBlobInfo(const TBlobInfo& other) = default;
    TBlobInfo(TBlobInfo&& other) = default;
    TBlobInfo(TString data, ui64 tablet, ui32 cookie, ui32 gen = 1, ui32 step = 1, ui32 channel = 0)
        : Status(EStatus::NONEXISTENT)
        , Id(tablet, gen, step, channel, data.size(), cookie)
        , Data(data)
    {
    }

    TString ToString() {
        TString status;
        if (Status == EStatus::NONEXISTENT) {
            status = "NONEXISTENT";
        } else if (Status == EStatus::WRITTEN) {
            status = "WRITTEN";
        } else if (Status == EStatus::UNKNOWN) {
            status = "UNKNOWN";
        } else {
            status = "COLLECTED";
        }
        return TStringBuilder() << "Status# " << status << " Id# {" << Id.ToString() << "} Data# " << Data <<
            " Keep# " << Keep << " DoNotKeep# " << DoNotKeep;
    }

    EStatus Status;
    const TLogoBlobID Id;
    TString Data;
    bool Keep = false;
    bool DoNotKeep = false;

    static const TBlobInfo& Nothing() {
        static const TBlobInfo nothing(TString(), 0, 0, 0, 0, 0);
        return nothing;
    }
};

struct TTabletInfo {
    struct TChanelInfo {
        ui32 SoftCollectGen = 0;
        ui32 SoftCollectStep = 0;
        ui32 HardCollectGen = 0;
        ui32 HardCollectStep = 0;
    };

    ui32 BlockedGen;
    std::vector<TChanelInfo> Channels;

    TTabletInfo()
        : BlockedGen(0)
        , Channels(6)
    {
    }
};

using TBSState = std::map<ui64, TTabletInfo>;

struct TEvArgs {
    enum EEventType : ui32 {
        PUT,
        GET,
        MULTIGET,
        DISCOVER,
        RANGE,
    };

    TEvArgs(EEventType type)
        : Type(type) {

    }

    EEventType Type;

    template<class Derived>
    Derived* Get() {
        return dynamic_cast<Derived*>(this);
    }

    virtual ~TEvArgs() = default;
};

struct TEvGetArgs : public TEvArgs {
    TEvGetArgs() = default;
    TEvGetArgs(bool mustRestoreFirst, bool indexOnly)
        : TEvArgs(EEventType::GET)
        , MustRestoreFirst(mustRestoreFirst)
        , IndexOnly(indexOnly)
    {
    }

    bool MustRestoreFirst = false;
    bool IndexOnly = false;
};

struct MultiTEvGetArgs : public TEvArgs {
    MultiTEvGetArgs() = default;
    MultiTEvGetArgs(bool mustRestoreFirst, bool indexOnly)
        : TEvArgs(EEventType::MULTIGET)
        , MustRestoreFirst(mustRestoreFirst)
        , IndexOnly(indexOnly)
    {
    }

    bool MustRestoreFirst = false;
    bool IndexOnly = false;
};

struct TEvDiscoverArgs : public TEvArgs {
    TEvDiscoverArgs() = default;
    TEvDiscoverArgs(ui32 minGeneration, bool readBody, bool discoverBlockedGeneration, ui32 forceBlockedGeneration, bool fromLeader)
        : TEvArgs(EEventType::DISCOVER)
        , MinGeneration(minGeneration)
        , ReadBody(readBody)
        , DiscoverBlockedGeneration(discoverBlockedGeneration)
        , ForceBlockedGeneration(forceBlockedGeneration)
        , FromLeader(fromLeader)
    {
    }

    ui32 MinGeneration = 0;
    bool ReadBody = true;
    bool DiscoverBlockedGeneration = false;
    ui32 ForceBlockedGeneration = 0;
    bool FromLeader = false;
};

struct TEvRangeArgs : public TEvArgs {
    TEvRangeArgs() = default;
    TEvRangeArgs(bool mustRestoreFirst, bool indexOnly)
        : TEvArgs(EEventType::RANGE)
        , MustRestoreFirst(mustRestoreFirst)
        , IndexOnly(indexOnly)
    {
    }

    bool MustRestoreFirst = false;
    bool IndexOnly = false;
};

struct TBlobDepotTestEnvironment {
    ui64 RandomSeed;
    TMersenne<ui32> Mt;
    TMersenne<ui64> Mt64;

    std::unique_ptr<TEnvironmentSetup> Env;
    std::vector<ui32> RegularGroups;
    ui32 BlobDepot;
    ui32 BlobDepotTabletId;

    TBlobDepotTestEnvironment(ui32 seed = 0, ui32 numGroups = 1, ui32 nodeCount = 8,
            TBlobStorageGroupType erasure = TBlobStorageGroupType::ErasureMirror3of4)
        : RandomSeed(seed)
        , Mt(seed)
        , Mt64(seed) {
        Cerr << "Mersenne random seed " << seed << Endl;
        ConfigureEnvironment(numGroups, Env, RegularGroups, BlobDepot, nodeCount, erasure);
        BlobDepotTabletId = 0;
    }

    void ConfigureEnvironment(ui32 numGroups, std::unique_ptr<TEnvironmentSetup>& envPtr, std::vector<ui32>& regularGroups, ui32& blobDepot,
            ui32 nodeCount = 8, TBlobStorageGroupType erasure = TBlobStorageGroupType::ErasureMirror3of4) {
        envPtr = std::make_unique<TEnvironmentSetup>(TEnvironmentSetup::TSettings{
            .NodeCount = nodeCount,
            .Erasure = erasure,
            .SetupHive = true,
        });

        envPtr->CreateBoxAndPool(1, numGroups);
        envPtr->Sim(TDuration::Seconds(20));

        regularGroups = envPtr->GetGroups();

        NKikimrBlobStorage::TConfigRequest request;
        TString virtualPool = "virtual_pool";
        {
            auto *cmd = request.AddCommand()->MutableDefineStoragePool();
            cmd->SetBoxId(1);
            cmd->SetName(virtualPool);
            cmd->SetErasureSpecies("none");
            cmd->SetVDiskKind("Default");
        }
        {
            auto *cmd = request.AddCommand()->MutableAllocateVirtualGroup();
            cmd->SetName("vg");
            cmd->SetHiveId(envPtr->Runtime->GetDomainsInfo()->GetHive());
            cmd->SetStoragePoolName(virtualPool);
            auto *prof = cmd->AddChannelProfiles();
            prof->SetStoragePoolName(envPtr->StoragePoolName);
            prof->SetCount(2);
            prof = cmd->AddChannelProfiles();
            prof->SetStoragePoolName(envPtr->StoragePoolName);
            prof->SetChannelKind(NKikimrBlobDepot::TChannelKind::Data);
            prof->SetCount(2);
        }

        auto response = envPtr->Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        blobDepot = response.GetStatus(1).GetGroupId(0);

        envPtr->Sim(TDuration::Seconds(5)); // some time for blob depot to crank up
    }

    TString DataGen(ui32 len) {
        TString res = "";
        for (ui32 i = 0; i < len; ++i) {
            res += 'A' + Mt.GenRand() % ('z' - 'A');
        }
        return res;
    }

    ui32 Rand(ui32 a, ui32 b) {
        if (a >= b) {
            return a;
        }
        return Mt.GenRand() % (b - a) + a;
    }

    ui32 Rand(ui32 b) {
        return Rand(0, b);
    }

    ui32 Rand() {
        return Mt.GenRand();
    }

    ui32 Rand64() {
        return Mt64.GenRand();
    }

    template <class T>
    T& Rand(std::vector<T>& v) {
        return v[Rand(v.size())];
    }

    ui32 SeedRand(ui32 a, ui32 b, ui32 seed) {
        TMersenne<ui32> temp(seed);
        if (a >= b) {
            return a;
        }
        return temp.GenRand() % (b - a) + a;
    }

    ui32 SeedRand(ui32 b, ui32 seed) {
        return SeedRand(0, b, seed);
    }

    template <class T>
    const T& Rand(const std::vector<T>& v) {
        return v[Rand(v.size())];
    }
};
