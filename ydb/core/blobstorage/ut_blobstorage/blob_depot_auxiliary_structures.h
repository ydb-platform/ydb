#pragma once

#include <ydb/core/base/logoblob.h>

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
        , KeepFlag(false)
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
        return TStringBuilder() << "Status# " << status << " Id# {" << Id.ToString() << "} Data# " << Data << " KeepFlag# " << KeepFlag;
    }

    EStatus Status;
    const TLogoBlobID Id;
    TString Data;
    bool KeepFlag;

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

struct TIntervals {
    std::vector<ui32> Borders; // [0; x_1) [x_1; x_2) ... [x_n-1; x_n)

    TIntervals(std::vector<ui32> borders) {
        Borders = borders;
        for (ui32 i = 1; i < Borders.size(); ++i) {
            Borders[i] += Borders[i - 1];
        }
    }

    ui32 GetInterval(ui32 x) {
        for (ui32 i = 0; i < Borders.size(); ++i) {
            if (x < Borders[i]) {
                return i;
            }
        }
        return Borders.size();
    }
    ui32 UpperLimit() {
        return Borders[Borders.size() - 1];
    }
};

/* ------------------------ REQUEST ARGUMENTS ----------------------- */

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
