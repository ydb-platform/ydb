#pragma once

#include "settings.h"

#include <library/cpp/getopt/small/modchooser.h>

#include <util/stream/file.h>

#include <ydb/core/protos/config.pb.h>
#include <ydb/library/actors/core/log_iface.h>
#include <ydb/library/services/services.pb.h>

namespace NKikimrRun {

class TMainBase : public TMainClassArgs {
protected:
    void RegisterKikimrOptions(NLastGetopt::TOpts& options, TServerSettings& settings);

    void FillLogConfig(NKikimrConfig::TLogConfig& config) const;

    static IOutputStream* GetDefaultOutput(const TString& file);

private:
    inline static std::vector<std::unique_ptr<TFileOutput>> FileHolders;

    std::optional<NActors::NLog::EPriority> DefaultLogPriority;
    std::unordered_map<NKikimrServices::EServiceKikimr, NActors::NLog::EPriority> LogPriorities;
};

}  // namespace NKikimrRun
