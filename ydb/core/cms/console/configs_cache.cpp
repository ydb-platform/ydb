#include "configs_cache.h"
#include "console_configs_subscriber.h"
#include "console.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>

#include <util/stream/file.h>
#include <util/system/fs.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <google/protobuf/text_format.h>

#if defined BLOG_DEBUG || defined BLOG_ERROR || defined BLOG_WARN
#error log macro definition clash
#endif

#define BLOG_DEBUG(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::CONFIGS_CACHE, stream)
#define BLOG_ERROR(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::CONFIGS_CACHE, stream)
#define BLOG_WARN(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::CONFIGS_CACHE, stream)

namespace NKikimr::NConsole {

namespace {
    constexpr auto TMP_FILE_SUFFIX = ".tmp";

    const THashSet<ui32> DYNAMIC_KINDS({
        (ui32)NKikimrConsole::TConfigItem::LogConfigItem,
        (ui32)NKikimrConsole::TConfigItem::TableProfilesConfigItem
    });

    struct TSave {
        TString PathToConfigCacheFile;

        explicit TSave(TString pathToConfigCacheFile)
            : PathToConfigCacheFile(std::move(pathToConfigCacheFile)) {}

        void operator()(const NKikimrConfig::TAppConfig &config) {
            if (!PathToConfigCacheFile)
                return;

            try {
                TString configString;
                if (!google::protobuf::TextFormat::PrintToString(config, &configString))
                    ythrow yexception() << "Failed to serialize config protobuf to string";

                auto cfgFilePath = PathToConfigCacheFile + TMP_FILE_SUFFIX;
                TFileOutput cfgFile(cfgFilePath);
                cfgFile << configString;

                if (!NFs::Rename(cfgFilePath, PathToConfigCacheFile))
                    ythrow yexception() << "Failed to rename temporary file " << LastSystemError() << " " << LastSystemErrorText();
            } catch (const yexception &ex) {
                BLOG_WARN("An exception occurred while saving config: " << ex.what());
            }
        }
    };

    struct TLoad {
        TString PathToConfigCacheFile;

        explicit TLoad(TString pathToConfigCacheFile)
            : PathToConfigCacheFile(std::move(pathToConfigCacheFile)) {}

        void operator()(NKikimrConfig::TAppConfig &config) {
            if (!PathToConfigCacheFile)
                return;

            try {
                auto configFile = TFileInput(PathToConfigCacheFile);
                auto configString = configFile.ReadAll();
                if (!google::protobuf::TextFormat::ParseFromString(configFile.ReadAll(), &config))
                    ythrow yexception() << "Failed to parse config protobuf from string";
            } catch (const yexception &ex) {
                BLOG_WARN("An exception occurred while getting config from cache file: " << ex.what());
            }
        };
    };
}

void TConfigsCache::Bootstrap(const TActorContext &ctx) {
    auto group = GetServiceCounters(AppData(ctx)->Counters, "utils")->GetSubgroup("component", "configs_cache");
    OutdatedConfiguration = group->GetCounter("OutdatedConfiguration");

    Load(CurrentConfig);

    BLOG_DEBUG("Restored configuration: " << CurrentConfig.ShortDebugString());

    const auto minKind = NKikimrConsole::TConfigItem::EKind_MIN;
    const auto maxKind = NKikimrConsole::TConfigItem::EKind_MAX;

    TVector<ui32> kinds;
    for (ui32 kind = minKind; kind <= maxKind; kind++) {
        if (kind == NKikimrConsole::TConfigItem::Auto || !NKikimrConsole::TConfigItem::EKind_IsValid(kind))
            continue;
        kinds.push_back(kind);
    }

    auto client = CreateConfigsSubscriber(SelfId(), kinds, CurrentConfig, 1);
    SubscriptionClient = ctx.RegisterWithSameMailbox(client);

    Become(&TThis::StateWork);
}

void TConfigsCache::Handle(TEvConsole::TEvConfigSubscriptionNotification::TPtr &ev) {
    auto &rec = ev->Get()->Record;

    if (rec.AffectedKindsSize() == 0) {
        return;
    }

    CurrentConfig.Swap(rec.MutableConfig());

    BLOG_DEBUG("Saving configuration: " << CurrentConfig.ShortDebugString());

    Save(CurrentConfig);

    for (auto kind : rec.GetAffectedKinds()) {
        if (!DYNAMIC_KINDS.contains(kind)) {
            const TActorId wb = NNodeWhiteboard::MakeNodeWhiteboardServiceId(SelfId().NodeId());
            NKikimrWhiteboard::TSystemStateInfo info;
            info.SetConfigState(NKikimrWhiteboard::Outdated);
            Send(wb, new NNodeWhiteboard::TEvWhiteboard::TEvSystemStateUpdate(info));

            *OutdatedConfiguration = 1;
        }
    }
}

void TConfigsCache::Handle(TEvConsole::TEvConfigSubscriptionError::TPtr &ev, const TActorContext &ctx) {
    auto &rec = ev->Get()->Record;

    BLOG_ERROR("Failed to create subscription " << rec.GetCode() << " " << rec.GetReason() << " will die");

    Die(ctx);
}

void TConfigsCache::Handle(TEvents::TEvPoisonPill::TPtr &/*ev*/, const TActorContext &ctx) {
    BLOG_DEBUG("Received poison pill, will die");

    Die(ctx);
}

void TConfigsCache::Die(const TActorContext &ctx) {
    if (SubscriptionClient)
        Send(SubscriptionClient, new TEvents::TEvPoisonPill());

    TBase::Die(ctx);
}

IActor *CreateConfigsCacheActor(const TString &pathToConfigCacheFile) {
    return new TConfigsCache(TSave(pathToConfigCacheFile), TLoad(pathToConfigCacheFile));
}
}
