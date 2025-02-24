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
                                   TIntrusivePtr<TStaticControlBoard> staticControlBoard,
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
    void CreateControls(TIntrusivePtr<TControlBoard> board,
                        TIntrusivePtr<TStaticControlBoard> staticControlBoard,
                        bool allowExisting);
    void CreateControls(TIntrusivePtr<TControlBoard> board,
                        TIntrusivePtr<TStaticControlBoard> staticControlBoard,
                        const google::protobuf::Descriptor *desc,
                        const TString &prefix,
                        bool allowExisting);
    void AddControl(TIntrusivePtr<TControlBoard> board,
                    TIntrusivePtr<TStaticControlBoard> staticControlBoard,
                    const google::protobuf::FieldDescriptor *desc,
                    const TString &prefix,
                    bool allowExisting);
    void ApplyConfig(const NKikimrConfig::TImmediateControlsConfig &cfg,
                     TIntrusivePtr<TControlBoard> board,
                     TIntrusivePtr<TStaticControlBoard> staticControlBoard);
    void ApplyConfig(const ::google::protobuf::Message &cfg,
                     const TString &prefix,
                     TIntrusivePtr<TControlBoard> board,
                     TIntrusivePtr<TStaticControlBoard> staticControlBoard);
    TString MakePrefix(const TString &prefix,
                       const TString &name);

    THashMap<TString, TControlWrapper> Controls;
};

TImmediateControlsConfigurator::TImmediateControlsConfigurator(TIntrusivePtr<TControlBoard> board,
                                                               TIntrusivePtr<TStaticControlBoard> staticControlBoard,
                                                               const NKikimrConfig::TImmediateControlsConfig &cfg,
                                                               bool allowExistingControls)
{
    CreateControls(board, staticControlBoard, allowExistingControls);
    ApplyConfig(cfg, board, staticControlBoard);
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

    ApplyConfig(rec.GetConfig().GetImmediateControlsConfig(), AppData(ctx)->Icb, AppData(ctx)->StaticControlBoard);

    auto resp = MakeHolder<TEvConsole::TEvConfigNotificationResponse>(rec);

    LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS,
                "TImmediateControlsConfigurator: Send TEvConfigNotificationResponse: "
                << resp->Record.ShortDebugString());

    ctx.Send(ev->Sender, resp.Release(), 0, ev->Cookie);
}

void TImmediateControlsConfigurator::CreateControls(TIntrusivePtr<TControlBoard> board,
                                                    TIntrusivePtr<TStaticControlBoard> staticControlBoard,
                                                    bool allowExisting)
{
    auto *desc = NKikimrConfig::TImmediateControlsConfig::descriptor();
    CreateControls(board, staticControlBoard, desc, "", allowExisting);
}

void TImmediateControlsConfigurator::CreateControls(TIntrusivePtr<TControlBoard> board,
                                                    TIntrusivePtr<TStaticControlBoard> staticControlBoard,
                                                    const google::protobuf::Descriptor *desc,
                                                    const TString &prefix,
                                                    bool allowExisting)
{
    for (int i = 0; i < desc->field_count(); ++i) {
        auto *fieldDesc = desc->field(i);

        Y_ABORT_UNLESS(!fieldDesc->is_repeated(),
                 "Repeated fields are not allowed in Immediate Controls Config");

        auto fieldType = fieldDesc->type();
        if (fieldType == google::protobuf::FieldDescriptor::TYPE_UINT64
            || fieldType == google::protobuf::FieldDescriptor::TYPE_INT64)
            AddControl(board, staticControlBoard, fieldDesc, prefix, allowExisting);
        else {
            Y_ABORT_UNLESS(fieldType == google::protobuf::FieldDescriptor::TYPE_MESSAGE,
                     "Only [u]int64 and message fields are allowed in Immediate Controls Config");

            CreateControls(board, staticControlBoard, fieldDesc->message_type(),
                           MakePrefix(prefix, fieldDesc->name()),
                           allowExisting);
        }
    }
}

void TImmediateControlsConfigurator::AddControl(TIntrusivePtr<TControlBoard> board,
                                                TIntrusivePtr<TStaticControlBoard> staticControlBoard,
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
    bool res = false;
    if (auto controlId = staticControlBoard->GetStaticControlId(name)) {
        res = staticControlBoard->RegisterSharedControl(Controls[name], *controlId);
    } else {
        res = board->RegisterSharedControl(Controls[name], name);
    }
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
                                                 TIntrusivePtr<TControlBoard> board,
                                                 TIntrusivePtr<TStaticControlBoard> staticControlBoard)
{
    ApplyConfig(cfg, "", board, staticControlBoard);
}

void TImmediateControlsConfigurator::ApplyConfig(const ::google::protobuf::Message &cfg,
                                                 const TString &prefix,
                                                 TIntrusivePtr<TControlBoard> board,
                                                 TIntrusivePtr<TStaticControlBoard> staticControlBoard)
{
    auto *desc = cfg.GetDescriptor();
    auto *reflection = cfg.GetReflection();
    for (int i = 0; i < desc->field_count(); ++i) {
        auto *fieldDesc = desc->field(i);
        auto fieldType = fieldDesc->type();
        auto name = MakePrefix(prefix, fieldDesc->name());
        if (fieldType == google::protobuf::FieldDescriptor::TYPE_UINT64
            || fieldType == google::protobuf::FieldDescriptor::TYPE_INT64) {
            Y_ABORT_UNLESS(Controls.contains(name));
            auto controlId = staticControlBoard->GetStaticControlId(name);
            if (reflection->HasField(cfg, fieldDesc)) {
                auto applyControl = [&reflection, &fieldDesc, &cfg, &fieldType]<typename TControlBoard, typename TControlId>(TControlBoard& controlBoard, const TControlId& id) {
                    TAtomicBase prev;
                    if (fieldType == google::protobuf::FieldDescriptor::TYPE_UINT64)
                        controlBoard.SetValue(id, reflection->GetUInt64(cfg, fieldDesc), prev);
                    else
                        controlBoard.SetValue(id, reflection->GetInt64(cfg, fieldDesc), prev);
                };
                if (controlId) { // is static
                    applyControl(*staticControlBoard,  *controlId);
                } else {
                    applyControl(*board, name);
                }
            } else {
                if (controlId) {
                   staticControlBoard->RestoreDefault(*controlId);
                } else {
                    board->RestoreDefault(name);
                }
            }
        } else {
            Y_ABORT_UNLESS(fieldType == google::protobuf::FieldDescriptor::TYPE_MESSAGE,
                     "Only [u]int64 and message fields are allowed in Immediate Controls Config");
            ApplyConfig(reflection->GetMessage(cfg, fieldDesc), name, board, staticControlBoard);
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
                                            TIntrusivePtr<TStaticControlBoard> staticControlBoard,
                                            const NKikimrConfig::TImmediateControlsConfig &cfg,
                                            bool allowExistingControls)
{
    return new TImmediateControlsConfigurator(board, staticControlBoard, cfg, allowExistingControls);
}

} // namespace NKikimr::NConsole
