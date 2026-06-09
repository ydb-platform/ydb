#include "console_configs_manager.h"
#include "modifications_validator.h"

#include <ydb/library/aclib/aclib.h>

namespace NKikimr::NConsole {

class TConfigsManager::TTxConfigure : public TTransactionBase<TConfigsManager> {
public:
    TTxConfigure(TEvConsole::TEvConfigureRequest::TPtr ev,
                 TConfigsManager *self)
        : TBase(self)
        , Request(std::move(ev))
    {
    }

    bool Error(Ydb::StatusIds::StatusCode code,
               const TString &error,
               const TActorContext &ctx)
    {
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS, "Cannot modify config: " << error);

        Response->Record.MutableStatus()->SetCode(code);
        Response->Record.MutableStatus()->SetReason(error);

        Self->PendingConfigModifications.Clear();

        return true;
    }

    bool IsGenerationOk(TConfigItem::TPtr item,
                        ui64 generation,
                        const TActorContext &ctx)
    {
        if (item->Generation != generation) {
            Error(Ydb::StatusIds::BAD_REQUEST,
                  TStringBuilder() << "wrong generation for config item #" << item->Id,
                  ctx);
            return false;
        }

        return true;
    }

    bool IsConfigKindOk(const NKikimrConfig::TAppConfig &config,
                        ui32 kind,
                        const TActorContext &ctx)
    {
        if (!kind) {
            Error(Ydb::StatusIds::BAD_REQUEST,
                  TStringBuilder() << "zero kind value is not allowed for config items",
                  ctx);
            return false;
        }

        std::vector<const ::google::protobuf::FieldDescriptor*> fields;
        auto *reflection = config.GetReflection();
        reflection->ListFields(config, &fields);
        for (auto field : fields) {
            if (field->number() != static_cast<int>(kind)) {
                Error(Ydb::StatusIds::BAD_REQUEST,
                      TStringBuilder() << "wrong config item: field '" << field->name()
                      << "' shouldn't be set for config item "
                      << TConfigItem::KindName(kind) << " (" << kind << ")",
                      ctx);
                return false;
            }
        }
        return true;
    }

    ui32 DetectConfigItemKind(const NKikimrConsole::TConfigItem &item)
    {
        std::vector<const ::google::protobuf::FieldDescriptor*> fields;
        auto *reflection = item.GetConfig().GetReflection();
        reflection->ListFields(item.GetConfig(), &fields);
        if (fields.size() != 1)
            return 0;
        return fields[0]->number();
    }

    void SplitConfigItem(const NKikimrConsole::TConfigItem &item,
                         TVector<TConfigItem::TPtr> &newItems,
                         const TActorContext &ctx)
    {
        NKikimrConfig::TAppConfig config;
        config.CopyFrom(item.GetConfig());
        NKikimrConsole::TConfigItem base;
        base.CopyFrom(item);
        base.ClearConfig();

        std::vector<const ::google::protobuf::FieldDescriptor*> fields;
        auto *reflection = config.GetReflection();
        reflection->ListFields(config, &fields);
        for (auto field : fields) {
            NKikimrConsole::TConfigItem copy;
            copy.CopyFrom(base);
            reflection->SwapFields(&config, copy.MutableConfig(), {field});
            TConfigItem::TPtr newItem = new TConfigItem(copy);
            newItem->Kind = field->number();
            newItems.push_back(newItem);

            LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS,
                        "Split config item: " << copy.ShortDebugString());
        }
    }

    bool IsAddConfigItemActionOk(const NKikimrConsole::TAddConfigItem &action,
                                 const TActorContext &ctx)
    {
        TVector<TConfigItem::TPtr> newItems;
        auto &item = action.GetConfigItem();

        if (item.HasId()) {
            Error(Ydb::StatusIds::BAD_REQUEST,
                  "cannot add config item: new items should have empty id",
                  ctx);
            return false;
        }

        if (!Self->IsSupportedMergeStrategy(item.GetMergeStrategy())) {
            Error(Ydb::StatusIds::BAD_REQUEST,
                  TStringBuilder() << "unsupported merge strategy ("
                  << item.GetMergeStrategy() << ")",
                  ctx);
            return false;
        }

        if (item.GetKind() == NKikimrConsole::TConfigItem::Auto) {
            ui32 kind = DetectConfigItemKind(item);
            if (kind) {
                TConfigItem::TPtr newItem = new TConfigItem(item);
                newItem->Kind = kind;
                newItems.push_back(newItem);
            } else {
                if (action.GetEnableAutoSplit()) {
                    SplitConfigItem(item, newItems, ctx);
                    if (newItems.empty()) {
                        Error(Ydb::StatusIds::BAD_REQUEST,
                              "cannot detect kind for new item with empty config",
                              ctx);
                        return false;
                    }
                } else {
                    Error(Ydb::StatusIds::BAD_REQUEST,
                          TStringBuilder() << "cannot detect kind for config item"
                          << " (auto split is disabled): " << item.ShortDebugString(),
                          ctx);
                    return false;
                }
            }
        } else {
            if (!IsConfigKindOk(item.GetConfig(), item.GetKind(), ctx))
                return false;

            newItems.push_back(new TConfigItem(item));
        }

        for (auto &newItem : newItems) {
            if (!Self->IsConfigItemScopeAllowed(newItem)) {
                Error(Ydb::StatusIds::BAD_REQUEST,
                      TStringBuilder() << "chosen usage scope for new item is disallowed: "
                      << newItem->ToString(),
                      ctx);
                return false;
            }
        }

        for (auto &newItem : newItems) {
            if (!newItem->UsageScope.Order)
                OrderingQueue.push_back(newItem);
            Self->PendingConfigModifications.AddedItems.push_back(newItem);
        }

        return true;
    }

    bool IsRemoveConfigItemActionOk(const NKikimrConsole::TRemoveConfigItem &action,
                                    const TActorContext &ctx)
    {
        TConfigItem::TPtr item = Self->ConfigIndex.GetItem(action.GetConfigItemId().GetId());
        if (!item) {
            Error(Ydb::StatusIds::BAD_REQUEST,
                  TStringBuilder() << "cannot remove unknown config item #" << action.GetConfigItemId().GetId(),
                  ctx);
            return false;
        }

        if (!IsGenerationOk(item, action.GetConfigItemId().GetGeneration(), ctx))
            return false;

        if (Self->PendingConfigModifications.ModifiedItems.contains(item->Id)) {
            Error(Ydb::StatusIds::BAD_REQUEST,
                  TStringBuilder() << "removal action for config item #"
                  << item->Id << " conflicts with its modification",
                  ctx);
            return false;
        }

        Self->PendingConfigModifications.RemovedItems.emplace(item->Id, item);

        return true;
    }

    bool IsRemoveConfigItemsByCookieActionOk(const NKikimrConsole::TCookies &cookies,
                                             const TActorContext &/*ctx*/)
    {
        TConfigItems items;
        for (auto &cookie : cookies.GetCookies())
            Self->ConfigIndex.CollectItemsByCookie(cookie, THashSet<ui32>(), items);
        for (auto &item : items)
            Self->PendingConfigModifications.RemovedItems.emplace(item->Id, item);
        return true;
    }

    bool IsRemoveConfigItemsActionOk(const NKikimrConsole::TRemoveConfigItems &action,
                                     const TActorContext &ctx)
    {
        switch (action.GetFilterCase()) {
        case NKikimrConsole::TRemoveConfigItems::kCookieFilter:
            return IsRemoveConfigItemsByCookieActionOk(action.GetCookieFilter(), ctx);
        case NKikimrConsole::TRemoveConfigItems::FILTER_NOT_SET:
            Error(Ydb::StatusIds::BAD_REQUEST, "empty filter in remove config items action", ctx);
            return false;
        default:
            Error(Ydb::StatusIds::BAD_REQUEST,
                  Sprintf("unsupported filter in action (%s)", action.ShortDebugString().data()),
                  ctx);
            return false;
        }
    }

    bool IsModifyConfigItemActionOk(const NKikimrConsole::TModifyConfigItem &action,
                                    const TActorContext &ctx)
    {
        auto &newItem = action.GetConfigItem();
        TConfigItem::TPtr item = Self->ConfigIndex.GetItem(newItem.GetId().GetId());
        if (!item) {
            Error(Ydb::StatusIds::BAD_REQUEST,
                  TStringBuilder() << "cannot modify unknown config item #" << newItem.GetId().GetId(),
                  ctx);
            return false;
        }

        if (!IsGenerationOk(item, newItem.GetId().GetGeneration(), ctx))
            return false;

        if (newItem.HasKind() && newItem.GetKind() != item->Kind) {
            Error(Ydb::StatusIds::BAD_REQUEST,
                  TStringBuilder() << "cannot modify kind for config item #" << item->Id,
                  ctx);
            return false;
        }

        if (!Self->IsSupportedMergeStrategy(newItem.GetMergeStrategy())) {
            Error(Ydb::StatusIds::BAD_REQUEST,
                  TStringBuilder() << "unsupported merge strategy ("
                  << newItem.GetMergeStrategy() << ")",
                  ctx);
            return false;
        }

        if (!IsConfigKindOk(newItem.GetConfig(), newItem.GetKind(), ctx))
            return false;

        if (Self->PendingConfigModifications.ModifiedItems.contains(item->Id)) {
            Error(Ydb::StatusIds::BAD_REQUEST,
                  TStringBuilder() << "multiple modification actions for config item #"
                  << item->Id << " are not allowed",
                  ctx);
            return false;
        } else if (Self->PendingConfigModifications.RemovedItems.contains(item->Id)) {
            Error(Ydb::StatusIds::BAD_REQUEST,
                  TStringBuilder() << "modification action conflicts with removal action"
                  " for config item #" << item->Id,
                  ctx);
            return false;
        }

        TConfigItem::TPtr newItemPtr = new TConfigItem(newItem);
        if (!Self->IsConfigItemScopeAllowed(newItemPtr)) {
            Error(Ydb::StatusIds::BAD_REQUEST,
                  TStringBuilder() << "modified usage scope for item #" << item->Id
                  << " is disallowed",
                  ctx);
            return false;
        }

        if (!newItemPtr->UsageScope.Order)
            OrderingQueue.push_back(newItemPtr);
        Self->PendingConfigModifications.ModifiedItems.emplace(item->Id, newItemPtr);

        return true;
    }

    bool IsActionOk(const NKikimrConsole::TConfigureAction &action,
                    const TActorContext &ctx)
    {
        switch (action.GetActionCase()) {
        case NKikimrConsole::TConfigureAction::kAddConfigItem:
            return IsAddConfigItemActionOk(action.GetAddConfigItem(), ctx);
        case NKikimrConsole::TConfigureAction::kRemoveConfigItem:
            return IsRemoveConfigItemActionOk(action.GetRemoveConfigItem(), ctx);
        case NKikimrConsole::TConfigureAction::kModifyConfigItem:
            return IsModifyConfigItemActionOk(action.GetModifyConfigItem(), ctx);
        case NKikimrConsole::TConfigureAction::kRemoveConfigItems:
            return IsRemoveConfigItemsActionOk(action.GetRemoveConfigItems(), ctx);
        case NKikimrConsole::TConfigureAction::ACTION_NOT_SET:
            Error(Ydb::StatusIds::BAD_REQUEST, "empty action", ctx);
            return false;
        default:
            Error(Ydb::StatusIds::BAD_REQUEST,
                  Sprintf("unsupported configure action (%s)", action.ShortDebugString().data()),
                  ctx);
            return false;
        }
    }

    TString ItemName(TConfigItem::TPtr item)
    {
        if (item->Id)
            return Sprintf("modified config item #%" PRIu64, item->Id);
        return "added config item";
    }

    bool CheckItemOrderConflict(TConfigItem::TPtr item,
                                TConfigItemsMap &newItems,
                                const TActorContext &ctx)
    {
        TConfigItems conflicts;
        Self->ConfigIndex.CollectItemsByConflictingScope(item->UsageScope, {item->Kind}, false, conflicts);
        for (auto &conflictItem : conflicts) {
            if (!Self->PendingConfigModifications.ModifiedItems.contains(conflictItem->Id)
                && !Self->PendingConfigModifications.RemovedItems.contains(conflictItem->Id)) {
                Error(Ydb::StatusIds::BAD_REQUEST,
                      TStringBuilder() << ItemName(item) << " (scope: " << item->UsageScope.ToString()
                      << ") has order conflict with config item #" << conflictItem->Id
                      << " (scope: " << conflictItem->UsageScope.ToString() << ")",
                      ctx);
                return false;
            }
        }

        for (auto &conflictItem : newItems[item->Kind]) {
            if (!conflictItem->UsageScope.HasConflict(item->UsageScope))
                continue;

            if (!conflictItem->Id && !item->Id)
                Error(Ydb::StatusIds::BAD_REQUEST,
                      TStringBuilder() << "two added items conflict by order (scope1: "
                      << item->UsageScope.ToString() << " scope2: "
                      << conflictItem->UsageScope.ToString() << ")",
                      ctx);
            else
                Error(Ydb::StatusIds::BAD_REQUEST,
                      TStringBuilder() << ItemName(item) << " (scope: " << item->UsageScope.ToString()
                      << ") has order conflict with " << ItemName(conflictItem) << " (scope: "
                      << conflictItem->UsageScope.ToString() << ")",
                      ctx);
            return false;
        }

        newItems[item->Kind].insert(item);
        return true;
    }

    bool DefineItemOrder(TConfigItem::TPtr item, const TActorContext &ctx)
    {
        Y_ABORT_UNLESS(!item->UsageScope.Order);

        TConfigItems conflicts;
        Self->ConfigIndex.CollectItemsByConflictingScope(item->UsageScope, {item->Kind}, true, conflicts);
        ui32 maxOrder = 0;
        for (auto &conflictItem : conflicts) {
            if (!Self->PendingConfigModifications.ModifiedItems.contains(conflictItem->Id)
                && !Self->PendingConfigModifications.RemovedItems.contains(conflictItem->Id))
                maxOrder = Max(maxOrder, conflictItem->UsageScope.Order);
        }

        for (auto &conflictItem : Self->PendingConfigModifications.AddedItems)
            if (conflictItem->UsageScope.Order
                && conflictItem->Kind == item->Kind
                && conflictItem->UsageScope.HasConflict(item->UsageScope, true))
                maxOrder = Max(maxOrder, conflictItem->UsageScope.Order);

        for (auto &pr : Self->PendingConfigModifications.ModifiedItems)
            if (pr.second->UsageScope.Order
                && pr.second->Kind == item->Kind
                && pr.second->UsageScope.HasConflict(item->UsageScope, true))
                maxOrder = Max(maxOrder, pr.second->UsageScope.Order);

        if (maxOrder == Max<ui32>()) {
            Error(Ydb::StatusIds::BAD_REQUEST,
                  TStringBuilder() << "Cannot auto order " << ItemName(item) << " (scope: "
                  << item->UsageScope.ToString() << " because max order value is used"
                  " by one of intersecting usage scopes.",
                  ctx);
            return false;
        }

        if (maxOrder <= (Max<ui32>() - 10))
            item->UsageScope.Order = maxOrder + 10;
        else
            item->UsageScope.Order = maxOrder + 1;

        return true;
    }

    TDynBitMap GetAffectedKinds(const ::google::protobuf::RepeatedPtrField<NKikimrConsole::TConfigureAction>& actions) {
        TDynBitMap kinds;

        for (auto &action : actions) {
            switch (action.GetActionCase()) {
            case NKikimrConsole::TConfigureAction::kAddConfigItem:
                kinds.Set(action.GetAddConfigItem().GetConfigItem().GetKind());
                break;
            case NKikimrConsole::TConfigureAction::kModifyConfigItem:
                kinds.Set(action.GetModifyConfigItem().GetConfigItem().GetKind());
                break;
            default:
                return TDynBitMap();
            }
        }

        return kinds;
    }

    bool ProcessAutoOrdering(const TActorContext &ctx)
    {
        for (auto &item : OrderingQueue)
            if (!DefineItemOrder(item, ctx))
                return false;

        OrderingQueue.clear();

        return true;
    }

    bool CheckOrderConflicts(const TActorContext &ctx)
    {
        TConfigItemsMap newItems;

        for (auto item : Self->PendingConfigModifications.AddedItems)
            if (!CheckItemOrderConflict(item, newItems, ctx))
                return false;

        for (auto &pr : Self->PendingConfigModifications.ModifiedItems)
            if (!CheckItemOrderConflict(pr.second, newItems, ctx))
                return false;

        return true;
    }

    bool Execute(TTransactionContext &txc,
                 const TActorContext &ctx) override
    {
        TString error;
        auto &rec = Request->Get()->Record;
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS, "TTxConfigure: " << rec.ShortDebugString());

        Y_ABORT_UNLESS(Self->PendingConfigModifications.IsEmpty());

        Response = new TEvConsole::TEvConfigureResponse;

        if (!rec.ActionsSize())
            return Error(Ydb::StatusIds::BAD_REQUEST,
                         "no actions specified", ctx);

        for (auto &action : rec.GetActions()) {
            if (!IsActionOk(action, ctx))
                return true;
        }

        if (!ProcessAutoOrdering(ctx))
            return true;

        if (!CheckOrderConflicts(ctx))
            return true;

        TModificationsValidator validator(Self->ConfigIndex,
                                          Self->PendingConfigModifications,
                                          Self->Config);
        if (!validator.ApplyValidators())
            return Error(Ydb::StatusIds::BAD_REQUEST,
                         validator.GetErrorMessage(), ctx);

        // Now configure command is known to be OK and we start to apply it.
        Response->Record.MutableStatus()->SetCode(Ydb::StatusIds::SUCCESS);

        // Fill affected configs.
        auto LogData = NKikimrConsole::TLogRecordData{};

        TConfigsConfig config(Self->Config);
        config.ValidationLevel = NKikimrConsole::VALIDATE_TENANTS_AND_NODE_TYPES;
        TModificationsValidator affectedChecker(Self->ConfigIndex,
                                                Self->PendingConfigModifications,
                                                config);
        auto affected = affectedChecker.ComputeAffectedConfigs(GetAffectedKinds(rec.GetActions()), false);
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS,
                    "affected.size() = " << affected.size());
        for (auto &item : affected) {
            auto &entry = *Response->Record.AddAffectedConfigs();
            entry.SetTenant(item.Tenant);
            entry.SetNodeType(item.NodeType);

            if (rec.GetFillAffectedConfigs())
                affectedChecker.BuildConfigs(TDynBitMap(),
                                             item.Tenant,
                                             item.NodeType,
                                             *entry.MutableOldConfig(),
                                             *entry.MutableNewConfig());
        }

        // Collect actually affected config kinds
        THashSet<ui32> AffectedKinds;

        // Dry run stops here. Further processing modifies internal state.
        if (rec.GetDryRun()) {
            Self->PendingConfigModifications.Clear();
            return true;
        }

        // Assign ids to new items.
        for (auto item : Self->PendingConfigModifications.AddedItems) {
            item->Id = Self->NextConfigItemId++;
            item->Generation = 1;
            Response->Record.AddAddedItemIds(item->Id);

            AffectedKinds.insert(item->Kind);
        }

        // Increment generation for modified items.
        for (auto &[_, item] : Self->PendingConfigModifications.ModifiedItems) {
            ++item->Generation;
            //
            AffectedKinds.insert(item->Kind);
        }

        for (auto &[_, item] : Self->PendingConfigModifications.RemovedItems) {
            AffectedKinds.insert(item->Kind);
        }

        // Get user sid for audit and cleanup message
        TString userSID;
        if (Request->Get()->Record.HasUserToken()) {
            NACLib::TUserToken userToken(Request->Get()->Record.GetUserToken());
            userSID = userToken.GetUserSID();
            Request->Get()->Record.ClearUserToken();
        }


        // Calculate actually affected configs for logging
        TModificationsValidator actualAffectedChecker(Self->ConfigIndex,
                                                      Self->PendingConfigModifications,
                                                      config);

        TDynBitMap kinds;

        for (auto kind : AffectedKinds) {
            kinds.Set(kind);
            LogData.AddAffectedKinds(kind);
        }

        auto actualAffected = actualAffectedChecker.ComputeAffectedConfigs(kinds, true);

        for (auto &item : actualAffected) {
            auto &logEntry = *LogData.AddAffectedConfigs();
            logEntry.SetTenant(item.Tenant);
            logEntry.SetNodeType(item.NodeType);

            actualAffectedChecker.BuildConfigs(kinds,
                                               item.Tenant,
                                               item.NodeType,
                                               *logEntry.MutableOldConfig(),
                                               *logEntry.MutableNewConfig());
        }

        // Update database.
        Self->DbApplyPendingConfigModifications(txc, ctx);
        Self->DbUpdateNextConfigItemId(txc, ctx);

        // Log command and changes
        LogData.MutableAction()->Swap(&Request->Get()->Record);
        Self->Logger.DbLogData(userSID, LogData, txc, ctx);

        return true;
    }

    void Complete(const TActorContext &executorCtx) override
    {
        auto ctx = executorCtx.MakeFor(Self->SelfId());
        LOG_DEBUG(ctx, NKikimrServices::CMS_CONFIGS, "TTxConfigure Complete");

        Y_ABORT_UNLESS(Response);

        if (!Self->PendingConfigModifications.IsEmpty()) {
            TAutoPtr<IEventHandle> ev = new IEventHandle(Request->Sender,
                                                         Self->SelfId(),
                                                         Response.Release(), 0,
                                                         Request->Cookie);
            Self->ApplyPendingConfigModifications(ctx, ev);
        } else {
            LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS,
                        "Send TEvConfigureResponse: " << Response->Record.ShortDebugString());
            ctx.Send(Request->Sender, Response.Release(), 0, Request->Cookie);
        }

        Self->TxProcessor->TxCompleted(this, ctx);
    }

private:
    TEvConsole::TEvConfigureRequest::TPtr Request;
    TAutoPtr<TEvConsole::TEvConfigureResponse> Response;
    TVector<TConfigItem::TPtr> OrderingQueue;
};

ITransaction *TConfigsManager::CreateTxConfigure(TEvConsole::TEvConfigureRequest::TPtr &ev)
{
    return new TTxConfigure(ev, this);
}

} // namespace NKikimr::NConsole
