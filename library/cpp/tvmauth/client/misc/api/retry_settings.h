#pragma once 
 
#include <library/cpp/tvmauth/client/misc/exponential_backoff.h> 
 
namespace NTvmAuth::NTvmApi { 
    struct TRetrySettings { 
        TExponentialBackoff::TSettings BackoffSettings = { 
            TDuration::Seconds(0), 
            TDuration::Minutes(1), 
            2, 
            0.5, 
        }; 
        TDuration MaxRandomSleepDefault = TDuration::Seconds(5); 
        TDuration MaxRandomSleepWhenOk = TDuration::Minutes(1); 
        ui32 RetriesOnStart = 3; 
        ui32 RetriesInBackground = 2; 
        TDuration WorkerAwakingPeriod = TDuration::Seconds(10); 
        ui32 DstsLimit = 300; 
        TDuration RolesUpdatePeriod = TDuration::Minutes(10); 
        TDuration RolesWarnPeriod = TDuration::Minutes(20); 
 
        bool operator==(const TRetrySettings& o) const { 
            return BackoffSettings == o.BackoffSettings && 
                   MaxRandomSleepDefault == o.MaxRandomSleepDefault && 
                   MaxRandomSleepWhenOk == o.MaxRandomSleepWhenOk && 
                   RetriesOnStart == o.RetriesOnStart && 
                   WorkerAwakingPeriod == o.WorkerAwakingPeriod && 
                   DstsLimit == o.DstsLimit && 
                   RolesUpdatePeriod == o.RolesUpdatePeriod && 
                   RolesWarnPeriod == o.RolesWarnPeriod; 
        } 
    }; 
} 
