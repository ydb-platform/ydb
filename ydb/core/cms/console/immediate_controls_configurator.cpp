#include "configs_dispatcher.h"
#include "console.h"
#include "immediate_controls_configurator.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/protos/config.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NConsole {

class TImmediateControlsConfigurator : public TActorBootstrapped<TImmediateControlsConfigurator> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType()
    {
        return NKikimrServices::TActivity::IMMEDITE_CONTROLS_CONFIGURATOR;
    }

    TImmediateControlsConfigurator(TIntrusivePtr<TControlBoard> board,
                                   const NKikimrConfig::TImmediateControlsConfig &cfg,
                                   bool allowExistingControls);

    void Bootstrap(const TActorContext &ctx);

    void Handle(TEvConsole::TEvConfigNotificationRequest::TPtr &ev,
                const TActorContext &ctx);

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvConsole::TEvConfigNotificationRequest, Handle);
            IgnoreFunc(TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse);

        default:
            Y_ABORT("unexpected event type: %" PRIx32 " event: %s",
                   ev->GetTypeRewrite(), ev->ToString().data());
            break;
        }
    }

private:
    void CreateControls(TIntrusivePtr<TControlBoard> board, bool allowExisting);
    void CreateControls(TIntrusivePtr<TControlBoard> board,
                        const google::protobuf::Descriptor *desc,
                        const TString &prefix,
                        bool allowExisting);
    void AddControl(TIntrusivePtr<TControlBoard> board,
                    const google::protobuf::FieldDescriptor *desc,
                    const TString &prefix,
                    bool allowExisting);
    void ApplyConfig(const NKikimrConfig::TImmediateControlsConfig &cfg,
                     TIntrusivePtr<TControlBoard> board);
    void ApplyConfig(const ::google::protobuf::Message &cfg,
                     const TString &prefix,
                     TIntrusivePtr<TControlBoard> board,
                     bool allowDynamicFields = false);
    TString MakePrefix(const TString &prefix,
                       const TString &name);

    THashMap<TString, TControlWrapper> Controls;
};

TImmediateControlsConfigurator::TImmediateControlsConfigurator(TIntrusivePtr<TControlBoard> board,
                                                               const NKikimrConfig::TImmediateControlsConfig &cfg,
                                                               bool allowExistingControls)
{
    CreateControls(board, allowExistingControls);
    ApplyConfig(cfg, board);
}

void TImmediateControlsConfigurator::Bootstrap(const TActorContext &ctx)
{
    LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS,
                "TImmediateControlsConfigurator Bootstrap");

    Become(&TThis::StateWork);

    LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS,
                "TImmediateControlsConfigurator: subscribe for config updates.");

    ui32 item = (ui32)NKikimrConsole::TConfigItem::ImmediateControlsConfigItem;
    ctx.Send(MakeConfigsDispatcherID(SelfId().NodeId()),
             new TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest(item));
}

void TImmediateControlsConfigurator::Handle(TEvConsole::TEvConfigNotificationRequest::TPtr &ev,
                                            const TActorContext &ctx)
{
    auto &rec = ev->Get()->Record;

    LOG_INFO_S(ctx, NKikimrServices::CMS_CONFIGS,
               "TImmediateControlsConfigurator: got new config: "
               << rec.GetConfig().ShortDebugString());

    ApplyConfig(rec.GetConfig().GetImmediateControlsConfig(), AppData(ctx)->Icb);

    auto resp = MakeHolder<TEvConsole::TEvConfigNotificationResponse>(rec);

    LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS,
                "TImmediateControlsConfigurator: Send TEvConfigNotificationResponse: "
                << resp->Record.ShortDebugString());

    ctx.Send(ev->Sender, resp.Release(), 0, ev->Cookie);
}

void TImmediateControlsConfigurator::CreateControls(TIntrusivePtr<TControlBoard> board, bool allowExisting)
{
    auto *desc = NKikimrConfig::TImmediateControlsConfig::descriptor();
    CreateControls(board, desc, "", allowExisting);
}

void TImmediateControlsConfigurator::CreateControls(TIntrusivePtr<TControlBoard> board,
                                                    const google::protobuf::Descriptor *desc,
                                                    const TString &prefix,
                                                    bool allowExisting)
{
    for (int i = 0; i < desc->field_count(); ++i) {
        auto *fieldDesc = desc->field(i);
        auto name = MakePrefix(prefix, fieldDesc->name());

        if (fieldDesc->is_map()) {
            continue;
        }

        Y_ABORT_UNLESS(!fieldDesc->is_repeated(),
                 "Repeated fields are not allowed in Immediate Controls Config");

        auto fieldType = fieldDesc->type();
        if (fieldType == google::protobuf::FieldDescriptor::TYPE_UINT64
            || fieldType == google::protobuf::FieldDescriptor::TYPE_INT64)
            AddControl(board, fieldDesc, prefix, allowExisting);
        else {
            Y_ABORT_UNLESS(fieldType == google::protobuf::FieldDescriptor::TYPE_MESSAGE,
                     "Only [u]int64 and message fields are allowed in Immediate Controls Config");

            CreateControls(board, fieldDesc->message_type(),
                           MakePrefix(prefix, fieldDesc->name()),
                           allowExisting);
        }
    }
}

void TImmediateControlsConfigurator::AddControl(TIntrusivePtr<TControlBoard> board,
                                                const google::protobuf::FieldDescriptor *desc,
                                                const TString &prefix,
                                                bool allowExisting)
{
    auto &opts = desc->options().GetExtension(NKikimrConfig::ControlOptions);
    auto name = MakePrefix(prefix, desc->name());
    ui64 defaultValue = opts.GetDefaultValue();
    ui64 minValue = opts.GetMinValue();
    ui64 maxValue = opts.GetMaxValue();

    Controls[name] = TControlWrapper(defaultValue, minValue, maxValue);

    // When we register control it is possible that is has already been
    // registered by some other code, in which case it may be used before it
    // is properly configured. It can currently only happen in configurator
    // tests, where it is created very late after some tablets have already
    // started.
    auto res = board->RegisterSharedControl(Controls[name], name);
    Y_VERIFY_S(res || allowExisting,
            "Immediate Control " << name << " was registered before "
            << "TImmediateControlsConfigurator creation");
    if (Y_UNLIKELY(!res)) {
        Cerr << "WARNING: immediate control " << name << " was registered before "
            << "TImmediateControlsConfigurator creation. "
            << "A default value may have been used before it was configured." << Endl;
        Controls[name].Reset(defaultValue, minValue, maxValue);
    }
}

void TImmediateControlsConfigurator::ApplyConfig(const NKikimrConfig::TImmediateControlsConfig &cfg,
                                                 TIntrusivePtr<TControlBoard> board)
{
    ApplyConfig(cfg, "", board);
}

void TImmediateControlsConfigurator::ApplyConfig(const ::google::protobuf::Message &cfg,
                                                 const TString &prefix,
                                                 TIntrusivePtr<TControlBoard> board,
                                                 bool allowDynamicFields)
{
    auto *desc = cfg.GetDescriptor();
    auto *reflection = cfg.GetReflection();
    for (int i = 0; i < desc->field_count(); ++i) {
        const auto *fieldDesc = desc->field(i);
        auto fieldType = fieldDesc->type();
        auto name = MakePrefix(prefix, fieldDesc->name());

        if (fieldDesc->is_map()) {
            auto *mapDesc = fieldDesc->message_type();
            auto *mapKey = mapDesc->map_key();
            auto *mapValue = mapDesc->map_value();

            auto keyType = mapKey->type();
            auto valueType = mapValue->type();

            Y_ABORT_UNLESS(keyType == google::protobuf::FieldDescriptor::TYPE_STRING,
                     "Only string keys are allowed in Immediate Controls Config maps");

            Y_ABORT_UNLESS(valueType == google::protobuf::FieldDescriptor::TYPE_MESSAGE,
                     "Only message value are allowed in Immediate Controls Config maps");

            auto entryCount = reflection->FieldSize(cfg, fieldDesc);
            for (int j = 0; j < entryCount; ++j) {
                const auto &entry = reflection->GetRepeatedMessage(cfg, fieldDesc, j);
                auto *entryReflection = entry.GetReflection();
                auto key = entryReflection->GetString(entry, mapKey);
                auto entryName = MakePrefix(name, key);
                ApplyConfig(entryReflection->GetMessage(entry, mapValue), entryName, board, true);
            }
            continue;
        }

        if (fieldType == google::protobuf::FieldDescriptor::TYPE_UINT64
            || fieldType == google::protobuf::FieldDescriptor::TYPE_INT64) {
            if (!Controls.contains(name)) {
                if (!allowDynamicFields) {
                    Y_ABORT("Missing control for field %s", name.c_str());
                }
                AddControl(board, fieldDesc, prefix, true);
            }
            
            if (reflection->HasField(cfg, fieldDesc)) {
                TAtomicBase prev;
                if (fieldType == google::protobuf::FieldDescriptor::TYPE_UINT64)
                    board->SetValue(name, reflection->GetUInt64(cfg, fieldDesc), prev);
                else
                    board->SetValue(name, reflection->GetInt64(cfg, fieldDesc), prev);
            } else {
                board->RestoreDefault(name);
            }
        } else {
            Y_ABORT_UNLESS(fieldType == google::protobuf::FieldDescriptor::TYPE_MESSAGE,
                     "Only [u]int64 and message fields are allowed in Immediate Controls Config");
            ApplyConfig(reflection->GetMessage(cfg, fieldDesc), name, board);
        }
    }
}

TString TImmediateControlsConfigurator::MakePrefix(const TString &prefix,
                                                   const TString &name)
{
    if (!prefix)
        return name;

    return prefix + "." + name;
}

IActor *CreateImmediateControlsConfigurator(TIntrusivePtr<TControlBoard> board,
                                            const NKikimrConfig::TImmediateControlsConfig &cfg,
                                            bool allowExistingControls)
{
    return new TImmediateControlsConfigurator(board, cfg, allowExistingControls);
}

} // namespace NKikimr::NConsole
