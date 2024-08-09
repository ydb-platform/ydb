#include "schemeshard_impl.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/tx/datashard/range_ops.h>
#include <ydb/core/tx/tx_proxy/proxy.h>

#include <library/cpp/html/pcdata/pcdata.h>
#include <util/string/cast.h>

static ui64 TryParseTabletId(TStringBuf tabletIdParam) {
    if (tabletIdParam.StartsWith("0x"))
        return IntFromString<ui64, 16>(tabletIdParam.substr(2));
    else
        return FromStringWithDefault<ui64>(tabletIdParam, ui64(NKikimr::NSchemeShard::InvalidTabletId));
}

namespace NKikimr {
namespace NSchemeShard {

struct TCgi {
    struct TParam {
        const TStringBuf Name;

        inline TParam(const TStringBuf name) noexcept
            : Name(name)
        {}

        operator TStringBuf () const {
            return Name;
        }

        operator TString () const {
            return ToString(Name);
        }

        template <class TDefVal>
        TString AsHiddenInput(const TDefVal value) const {
            return TStringBuilder()
                << "<input type=\"hidden\" id=\"" << Name << "\" name=\"" << Name << "\" value=\"" << value << "\"/>";
        }

        template <class TDefVal>
        TString AsInput(const TDefVal defValue) const {
            return TStringBuilder()
                << "<div class=\"col-md-4\">"
                << "<label for=\"" << Name << "\"> " << Name << ": <input type=\"text\" class=\"form-control\" id=\"" << Name << "\" name=\"" << Name << "\" value=\"" << defValue << "\" /> </label>"
                << "</div>";
        }

        TString AsInput() const {
            return TStringBuilder()
                << "<div class=\"col-md-4\">"
                << "<label for=\"" << Name << "\"> " << Name << ": <input type=\"text\" class=\"form-control\" id=\"" << Name << "\" name=\"" << Name << "\" /> </label>"
                << "</div>";
        }

        template <class TVal>
        TString AsCgiParam(const TVal value) const {
            return TStringBuilder() << Name << "=" << value;
        }
    };

    static const TParam TabletID;
    static const TParam TxId;
    static const TParam PartId;
    static const TParam OperationId;
    static const TParam OwnerShardIdx;
    static const TParam LocalShardIdx;
    static const TParam ShardID;
    static const TParam OwnerPathId;
    static const TParam LocalPathId;
    static const TParam IsReadOnlyMode;
    static const TParam UpdateAccessDatabaseRights;
    static const TParam UpdateAccessDatabaseRightsDryRun;
    static const TParam FixAccessDatabaseInheritance;
    static const TParam FixAccessDatabaseInheritanceDryRun;
    static const TParam Page;
    static const TParam BuildIndexId;
    static const TParam UpdateCoordinatorsConfig;
    static const TParam UpdateCoordinatorsConfigDryRun;

    struct TPages {
        static constexpr TStringBuf MainPage = "Main";
        static constexpr TStringBuf AdminPage = "Admin";
        static constexpr TStringBuf AdminRequest = "AdminRequest";
        static constexpr TStringBuf TransactionList = "TxList";
        static constexpr TStringBuf TransactionInfo = "TxInfo";
        static constexpr TStringBuf PathInfo = "PathInfo";
        static constexpr TStringBuf ShardInfoByTabletId = "ShardInfoByTabletId";
        static constexpr TStringBuf ShardInfoByShardIdx = "ShardInfoByShardIdx";
        static constexpr TStringBuf BuildIndexInfo = "BuildIndexInfo";
    };
};

const TCgi::TParam TCgi::TabletID = TStringBuf("TabletID");
const TCgi::TParam TCgi::TxId = TStringBuf("TxId");
const TCgi::TParam TCgi::PartId = TStringBuf("PartId");
const TCgi::TParam TCgi::OperationId = TStringBuf("OperationId");
const TCgi::TParam TCgi::OwnerShardIdx = TStringBuf("OwnerShardIdx");
const TCgi::TParam TCgi::LocalShardIdx = TStringBuf("LocalShardIdx");
const TCgi::TParam TCgi::ShardID = TStringBuf("ShardID");
const TCgi::TParam TCgi::OwnerPathId = TStringBuf("OwnerPathId");
const TCgi::TParam TCgi::LocalPathId = TStringBuf("LocalPathId");
const TCgi::TParam TCgi::IsReadOnlyMode = TStringBuf("IsReadOnlyMode");
const TCgi::TParam TCgi::UpdateAccessDatabaseRights = TStringBuf("UpdateAccessDatabaseRights");
const TCgi::TParam TCgi::UpdateAccessDatabaseRightsDryRun = TStringBuf("UpdateAccessDatabaseRightsDryRun");
const TCgi::TParam TCgi::FixAccessDatabaseInheritance = TStringBuf("FixAccessDatabaseInheritance");
const TCgi::TParam TCgi::FixAccessDatabaseInheritanceDryRun = TStringBuf("FixAccessDatabaseInheritanceDryRun");
const TCgi::TParam TCgi::Page = TStringBuf("Page");
const TCgi::TParam TCgi::BuildIndexId = TStringBuf("BuildIndexId");
const TCgi::TParam TCgi::UpdateCoordinatorsConfig = TStringBuf("UpdateCoordinatorsConfig");
const TCgi::TParam TCgi::UpdateCoordinatorsConfigDryRun = TStringBuf("UpdateCoordinatorsConfigDryRun");


class TUpdateCoordinatorsConfigActor : public TActorBootstrapped<TUpdateCoordinatorsConfigActor> {
public:
    struct TItem {
        TPathId PathId;
        TString PathString;
        NKikimrSubDomains::TProcessingParams Params;
    };

    using TCallback = std::function<void(const TString&, const TActorContext&)>;

public:
    TUpdateCoordinatorsConfigActor(TVector<TItem> items, TCallback callback, bool dryRun)
        : Items(std::move(items))
        , Callback(std::move(callback))
        , DryRun(dryRun)
    { }

public:
    void Bootstrap(const TActorContext& ctx) {
        if (Items.empty()) {
            Finish(ctx);
            return;
        }

        ctx.Send(MakeTxProxyID(), new TEvTxUserProxy::TEvGetProxyServicesRequest(), IEventHandle::FlagTrackDelivery);
        Become(&TThis::StateWork);
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ev);
        Log << "One or more services is unavailable" << Endl;
        Finish(ctx);
    }

    void Handle(TEvTxUserProxy::TEvGetProxyServicesResponse::TPtr& ev, const TActorContext& ctx) {
        Services = ev->Get()->Services;

        for (const auto& item : Items) {
            for (ui64 coordinator : item.Params.GetCoordinators()) {
                if (DryRun) {
                    Log << item.PathString << " (" << coordinator << ") UPDATE SKIPPED (DRY RUN)" << Endl;
                    continue;
                }
                bool inserted = InFlight.emplace(coordinator, &item).second;
                if (inserted) {
                    ctx.Send(Services.LeaderPipeCache, new TEvPipeCache::TEvForward(
                            new TEvSubDomain::TEvConfigure(item.Params),
                            coordinator, true),
                        IEventHandle::FlagTrackDelivery);
                }
            }
        }

        if (!InFlight) {
            Finish(ctx);
        }
    }

    void Handle(TEvSubDomain::TEvConfigureStatus::TPtr& ev, const TActorContext& ctx) {
        auto status = ev->Get()->Record.GetStatus();
        ui64 tabletId = ev->Get()->Record.GetOnTabletId();
        auto it = InFlight.find(tabletId);
        if (it == InFlight.end()) {
            return; // already processed
        }

        const auto* item = it->second;
        Log << item->PathString << " (" << tabletId << ") " << NKikimrTx::TEvSubDomainConfigurationAck::EStatus_Name(status) << Endl;
        InFlight.erase(it);

        if (!InFlight) {
            Finish(ctx);
        }
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev, const TActorContext& ctx) {
        ui64 tabletId = ev->Get()->TabletId;
        auto it = InFlight.find(tabletId);
        if (it == InFlight.end()) {
            return; // already processed
        }

        const auto* item = it->second;
        Log << item->PathString << " (" << tabletId << ") DELIVERY PROBLEM" << Endl;
        InFlight.erase(it);

        if (!InFlight) {
            Finish(ctx);
        }
    }

    void Finish(const TActorContext& ctx) {
        if (Services.LeaderPipeCache) {
            ctx.Send(Services.LeaderPipeCache, new TEvPipeCache::TEvUnlink(0));
        }
        Callback(Log, ctx);
        Die(ctx);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvents::TEvUndelivered, Handle);
            HFunc(TEvTxUserProxy::TEvGetProxyServicesResponse, Handle);
            HFunc(TEvSubDomain::TEvConfigureStatus, Handle);
            HFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
        }
    }

private:
    TVector<TItem> Items;
    TCallback Callback;
    bool DryRun;

    TStringBuilder Log;
    NTxProxy::TTxProxyServices Services;
    THashMap<ui64, const TItem*> InFlight;
};

struct TSchemeShard::TTxMonitoring : public NTabletFlatExecutor::TTransactionBase<TSchemeShard> {
    NMon::TEvRemoteHttpInfo::TPtr Ev;
    TStringStream Answer;

public:
    TTxMonitoring(TSchemeShard *self, NMon::TEvRemoteHttpInfo::TPtr ev)
        : TBase(self)
        , Ev(ev)
    {
    }

    bool Execute(NTabletFlatExecutor::TTransactionContext &txc, const TActorContext &ctx) override {
        Y_UNUSED(txc);

        const TCgiParameters& cgi = Ev->Get()->Cgi();

        const TString page = cgi.Has(TCgi::Page) ? cgi.Get(TCgi::Page) : ToString(TCgi::TPages::MainPage);

        if (page ==  TCgi::TPages::AdminRequest) {
            NIceDb::TNiceDb db(txc.DB);
            db.NoMoreReadsForTx();

            LinkToMain(Answer);
            OutputAdminRequestPage(Answer, db, cgi, ctx);
            return true;
        }

        if (page == TCgi::TPages::MainPage) {
            OutputMainPage(Answer);
            return true;
        }

        LinkToMain(Answer);

        if (page == TCgi::TPages::TransactionList)
        {
            OutputTxListPage(Answer);
        }
        else if (page == TCgi::TPages::TransactionInfo)
        {
            auto txId = TTxId(FromStringWithDefault<ui64>(cgi.Get(TCgi::TxId), ui64(InvalidTxId)));
            auto partId = cgi.Has(TCgi::PartId)
                ? TSubTxId(FromStringWithDefault<ui64>(cgi.Get(TCgi::PartId), ui64(InvalidSubTxId)))
                : FirstSubTxId;
            OutputTxInfoPage(TOperationId(txId, partId), Answer);
        }
        else if (page == TCgi::TPages::ShardInfoByTabletId)
        {
            TTabletId tabletId = TTabletId(TryParseTabletId(cgi.Get(TCgi::ShardID)));
            OutputShardInfoPageByShardID(tabletId, Answer);
        }
        else if (page == TCgi::TPages::ShardInfoByShardIdx)
        {
            auto shardIdx = TShardIdx(
                FromStringWithDefault<ui64>(cgi.Get(TCgi::OwnerShardIdx), InvalidOwnerId),
                TLocalShardIdx(FromStringWithDefault<ui64>(cgi.Get(TCgi::LocalShardIdx), ui64(InvalidLocalShardIdx)))
                );
            OutputShardInfoPageByShardIdx(shardIdx, Answer);
        }
        else if (page == TCgi::TPages::PathInfo)
        {
            TLocalPathId ownerPathId = FromStringWithDefault<ui64>(cgi.Get(TCgi::OwnerPathId), InvalidOwnerId);
            TLocalPathId localPathId = FromStringWithDefault<ui64>(cgi.Get(TCgi::LocalPathId), InvalidLocalPathId);
            TPathId pathId(ownerPathId, localPathId);
            OutputPathInfoPage(pathId, Answer);
        }
        else if (page == TCgi::TPages::AdminPage)
        {
            OutputAdminPage(Answer);
        }
        else if (page == TCgi::TPages::BuildIndexInfo)
        {
            auto id = TIndexBuildId(FromStringWithDefault<ui64>(cgi.Get(TCgi::BuildIndexId), ui64(InvalidIndexBuildId)));
            BuildIndexInfoPage(id, Answer);
        }

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        if (Answer) {
            ctx.Send(Ev->Sender, new NMon::TEvRemoteHttpInfoRes(Answer.Str()));
        }
    }

private:
    void LinkToMain(TStringStream& str) const {
        str << "<a href='app?" << TCgi::TabletID.AsCgiParam(Self->TabletID())
                               << "&" << TCgi::Page.AsCgiParam(TCgi::TPages::MainPage) << "'>";
        str << "Back to main scheme shard page";
        str << "</a><br>";
    }

    void OutputAdminRequestPage(TStringStream& str, NIceDb::TNiceDb& db, const TCgiParameters& cgi, const TActorContext& ctx) const {
        if (cgi.Has(TCgi::IsReadOnlyMode)) {
            TString rowStr = cgi.Get(TCgi::IsReadOnlyMode);
            auto value = FromStringWithDefault<ui64>(rowStr, ui64(0));
            auto valueStr =  ToString(value);

            TStringBuilder debug;
            debug << "IsReadOnlyMode changed from " << ToString(Self->IsReadOnlyMode)
                  << " to " << valueStr;

            LOG_EMERG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "TSchemeShard::TTxMonitoring AdminRequest " << debug);
            str << debug;

            db.Table<Schema::SysParams>().Key(Schema::SysParam_IsReadOnlyMode).Update(
                    NIceDb::TUpdate<Schema::SysParams::Value>(valueStr));
            Self->IsReadOnlyMode = value;
        }

        if (cgi.Has(TCgi::UpdateAccessDatabaseRights)) {
            TString rowDryRunStr = cgi.Get(TCgi::UpdateAccessDatabaseRightsDryRun);
            auto valueDryRun = FromStringWithDefault<ui64>(rowDryRunStr, ui64(1));
            auto valueDryRunStr =  ToString(valueDryRun);

            TStringBuilder debug;
            debug << "Triggered UpdateAccessDatabaseRights with DryRunVal: " << valueDryRunStr;

            LOG_EMERG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "TSchemeShard::TTxMonitoring AdminRequest " << debug);
            str << debug;

            TStringStream templateAnswer = str;
            str.clear();

            OutputAdminPage(templateAnswer);

            auto func = [templateAnswer] (const TMap<TPathId, TSet<TString>>& done) -> NActors::IEventBase* {
                TStringStream str = templateAnswer;
                HTML(str) {
                    TABLE_SORTABLE_CLASS("table") {
                        TABLEHEAD() {
                            TABLER() {
                                TABLEH() {str << "DomainId";}
                                TABLEH() {str << "Sid";}
                            }
                            str << "\n";
                        }

                        for (const auto& item: done) {
                            const TPathId& domainId = item.first;

                            for (const auto& sid: item.second) {
                                TABLER() {
                                    TABLED() { str << domainId; }
                                    TABLED() { str << sid; }
                                }
                                str << "\n";
                            }
                        }
                    }
                }

                return new NMon::TEvRemoteHttpInfoRes(str.Str());
            };

            Self->Execute(Self->CreateTxUpgradeAccessDatabaseRights(Ev->Sender, bool(valueDryRun), func), ctx);

            return;
        }

        if (cgi.Has(TCgi::FixAccessDatabaseInheritance)) {
            TString rowDryRunStr = cgi.Get(TCgi::FixAccessDatabaseInheritanceDryRun);
            auto valueDryRun = FromStringWithDefault<ui64>(rowDryRunStr, ui64(1));
            auto valueDryRunStr =  ToString(valueDryRun);

            TStringBuilder debug;
            debug << "Triggered FixAccessDatabaseInheritance with DryRunVal: " << valueDryRunStr;

            LOG_EMERG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "TSchemeShard::TTxMonitoring AdminRequest " << debug);
            str << debug;

            TStringStream templateAnswer = str;
            str.clear();

            OutputAdminPage(templateAnswer);

            auto func = [templateAnswer] (const TMap<TPathId, TSet<TString>>& done) -> NActors::IEventBase* {
                TStringStream str = templateAnswer;
                HTML(str) {
                    TABLE_SORTABLE_CLASS("table") {
                        TABLEHEAD() {
                            TABLER() {
                                TABLEH() {str << "DomainId";}
                                TABLEH() {str << "Sid";}
                            }
                            str << "\n";
                        }

                        for (const auto& item: done) {
                            const TPathId& domainId = item.first;

                            for (const auto& sid: item.second) {
                                TABLER() {
                                    TABLED() { str << domainId; }
                                    TABLED() { str << sid; }
                                }
                                str << "\n";
                            }
                        }
                    }
                }

                return new NMon::TEvRemoteHttpInfoRes(str.Str());
            };

            Self->Execute(Self->CreateTxMakeAccessDatabaseNoInheritable(Ev->Sender, bool(valueDryRun), func), ctx);

            return;
        }

        if (cgi.Has(TCgi::UpdateCoordinatorsConfig)) {
            TString rawDryRunStr = cgi.Get(TCgi::UpdateCoordinatorsConfigDryRun);
            auto valueDryRun = FromStringWithDefault<ui64>(rawDryRunStr, ui64(1));

            TStringBuilder debug;
            debug << "Triggered UpdateCoordinatorsConfig, dryRun = " << valueDryRun;

            LOG_EMERG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "TSchemeShard::TTxMonitoring AdminRequest " << debug);
            str << "<pre>";
            str << debug << Endl;

            TVector<TUpdateCoordinatorsConfigActor::TItem> items;
            for (const auto& kv : Self->SubDomains) {
                auto pathId = kv.first;
                auto path = TPath::Init(pathId, Self);
                auto pathString = path.PathString();
                const auto& subDomain = kv.second;
                if (subDomain->GetAlter()) {
                    str << "Skipping " << pathString << ": active alter found" << Endl;
                    continue;
                }
                if (path.Base()->IsRoot()) {
                    str << "Skipping " << pathString << ": not updating root" << Endl;
                    continue;
                }
                auto params = subDomain->GetProcessingParams();
                if (params.GetVersion() <= 0) {
                    str << "Skipping " << pathString << ": processing params version is " << params.GetVersion() << Endl;
                    continue;
                }
                auto& item = items.emplace_back();
                item.PathId = pathId;
                item.PathString = pathString;
                item.Params = std::move(params);
            }

            str << "</pre>";
            OutputAdminPage(str);

            auto callback = [sender = Ev->Sender, str = std::move(str)] (const TString& log, const TActorContext& ctx) mutable {
                str << "<pre>" << log << "</pre>";

                ctx.Send(sender, new NMon::TEvRemoteHttpInfoRes(str.Str()));
            };
            str.clear();

            ctx.Register(new TUpdateCoordinatorsConfigActor(std::move(items), std::move(callback), valueDryRun));
            return;
        }

        OutputAdminPage(str);
    }

    TString SubmitButton(const TStringBuf value) const {
        return TStringBuilder()
            << "<div class=\"col-md-4\"><input class=\"btn btn-default\" type=\"submit\" value=\"" << value << "\"></div>" << Endl;
    }

    void OutputAdminPage(TStringStream& str) const {
        {
            str << "<form method=\"GET\" id=\"tblMonSSFrm\" name=\"tblMonSSFrm\" class=\"form-group\">" << Endl;
            str << "<legend> Settings to change: </legend>";
            str << TCgi::TabletID.AsHiddenInput(Self->TabletID());
            str << TCgi::Page.AsHiddenInput(TCgi::TPages::AdminRequest);
            str << "<div style=\"display: flex; align-items: center;\">";
            str << TCgi::IsReadOnlyMode.AsInput(Self->IsReadOnlyMode);
            str << SubmitButton("Set");
            str << "</div>";
            str << "</form>" << Endl;
        }
        {
            str << "<form method=\"GET\" id=\"tblMonSSFrm\" name=\"tblMonSSFrm\" class=\"form-group\">" << Endl;
            str << "<legend> Execute upgrade DB's ACL, grant ACCESS to all existed users: </legend>";
            str << TCgi::TabletID.AsHiddenInput(Self->TabletID());
            str << TCgi::Page.AsHiddenInput(TCgi::TPages::AdminRequest);
            str << TCgi::UpdateAccessDatabaseRights.AsHiddenInput("1");
            str << "<div style=\"display: flex; align-items: center;\">";
            str << TCgi::UpdateAccessDatabaseRightsDryRun.AsInput(1);
            str << SubmitButton("Run");
            str << "</div>";
            str << "</form>" << Endl;
        }
        {
            str << "<form method=\"GET\" id=\"tblMonSSFrm\" name=\"tblMonSSFrm\" class=\"form-group\">" << Endl;
            str << "<legend> Make all Access Database rights no inheritable at all database: </legend>";
            str << TCgi::TabletID.AsHiddenInput(Self->TabletID());
            str << TCgi::Page.AsHiddenInput(TCgi::TPages::AdminRequest);
            str << TCgi::FixAccessDatabaseInheritance.AsHiddenInput("1");
            str << "<div style=\"display: flex; align-items: center;\">";
            str << TCgi::FixAccessDatabaseInheritanceDryRun.AsInput(1);
            str << SubmitButton("Run");
            str << "</div>";
            str << "</form>" << Endl;
        }
        {
            str << "<form method=\"GET\" id=\"tblMonSSFrmUpdateCoordinatorsConfig\" name=\"tblMonSSFrmUpdateCoordinatorsConfig\" class=\"form-group\">" << Endl;
            str << "<legend> Send configuration update to all coordinators: </legend>";
            str << TCgi::TabletID.AsHiddenInput(Self->TabletID());
            str << TCgi::Page.AsHiddenInput(TCgi::TPages::AdminRequest);
            str << TCgi::UpdateCoordinatorsConfig.AsHiddenInput("1");
            str << "<div style=\"display: flex; align-items: center;\">";
            str << TCgi::UpdateCoordinatorsConfigDryRun.AsInput(1);
            str << SubmitButton("Run");
            str << "</div>";
            str << "</form>" << Endl;
        }
    }

    void OutputMainPage(TStringStream& str) const {
        HTML(str) {
            TAG(TH3) {str << "SchemeShard main page:";}

            {
                str << "<legend>";
                str << "<a href='app?"
                    << TCgi::TabletID.AsCgiParam(Self->TabletID())
                    << "&" << TCgi::Page.AsCgiParam(TCgi::TPages::AdminPage)
                    << "'> Administration settings </a>";
                str << "</legend>";
            }

            {
                str << "<form method=\"GET\" id=\"tblMonSSFrm\" name=\"tblMonSSFrm\" class=\"form-group\">" << Endl;
                str << "<legend> Hierarchy of SS items: </legend>";
                str << TCgi::TabletID.AsHiddenInput(Self->TabletID());
                str << TCgi::Page.AsHiddenInput(TCgi::TPages::PathInfo);
                str << TCgi::OwnerPathId.AsHiddenInput(Self->TabletID());
                str << TCgi::LocalPathId.AsHiddenInput("1");
                str << "<div style=\"display: flex; align-items: center;\">";
                str << SubmitButton("Watch");
                str << "</div>";
                str << "</form>" << Endl;
            }

            {
                str << "<form method=\"GET\" id=\"tblMonSSFrm\" name=\"tblMonSSFrm\" class=\"form-group\">" << Endl;
                str << "<legend> Path info by Path Id: </legend>" << Endl;
                str << TCgi::TabletID.AsHiddenInput(Self->TabletID());
                str << TCgi::Page.AsHiddenInput(TCgi::TPages::PathInfo);
                str << "<div style=\"display: flex; align-items: center;\">";
                str << TCgi::OwnerPathId.AsInput(Self->TabletID());
                str << TCgi::LocalPathId.AsInput();
                str << SubmitButton("Watch");
                str << "</div>";
                str << "</form>" << Endl;
            }

            {
                str << "<form method=\"GET\" id=\"tblMonSSFrm\" name=\"tblMonSSFrm\" class=\"form-group\">" << Endl;
                str << "<legend> Shard info by ID: </legend>";
                str << TCgi::TabletID.AsHiddenInput(Self->TabletID());
                str << TCgi::Page.AsHiddenInput(TCgi::TPages::ShardInfoByTabletId);
                str << "<div style=\"display: flex; align-items: center;\">";
                str << TCgi::ShardID.AsInput();
                str << SubmitButton("Watch");
                str << "</div>";
                str << "</form>" << Endl;
            }
            {
                str << "<form method=\"GET\" id=\"tblMonSSFrm\" name=\"tblMonSSFrm\" class=\"form-group\">" << Endl;
                str << "<legend> Shard info by Idx: </legend>";
                str << TCgi::TabletID.AsHiddenInput(Self->TabletID());
                str << TCgi::Page.AsHiddenInput(TCgi::TPages::ShardInfoByShardIdx);
                str << "<div style=\"display: flex; align-items: center;\">";
                str << TCgi::OwnerShardIdx.AsInput(Self->TabletID());
                str << TCgi::LocalShardIdx.AsInput();
                str << SubmitButton("Watch");
                str << "</div>";
                str << "</form>" << Endl;
            }

            {
                str << "<form method=\"GET\" id=\"tblMonSSFrm\" name=\"tblMonSSFrm\" class=\"form-group\">" << Endl;
                str << "<fieldset>";
                str << "<legend>TxInFly info by OperationId: </legend>";
                str << TCgi::TabletID.AsHiddenInput(Self->TabletID());
                str << TCgi::Page.AsHiddenInput(TCgi::TPages::TransactionInfo);
                str << "<div style=\"display: flex; align-items: center;\">";
                str << TCgi::TxId.AsInput();
                str << TCgi::PartId.AsInput();
                str << SubmitButton("Watch");
                str << "</div>";
                str << "</fieldset>";
                str << "</form>" << Endl;
            }

            {
                str << "<legend>";
                TAG(TH3) {str << "Transactions in flight:"; }
                str << "</legend>";
                TableTxInfly(str);
            }

            {
                str << "<legend>";
                TAG(TH3) {str << "Active Build Indexes in flight:"; }
                str << "</legend>";
                BuildIndexesInfly(str, /*forActive=*/ true);
            }

            {
                str << "<legend>";
                TAG(TH3) {str << "Finished Build Indexes:"; }
                str << "</legend>";
                BuildIndexesInfly(str, /*forActive=*/ false);
            }
        }
    }

    void BuildIndexesInfly(TStringStream& str, bool forActive) const {
        HTML(str) {
            TABLE_SORTABLE_CLASS("table") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() {str << "Id";}
                        TABLEH() {str << "State";}
                    }
                    str << "\n";
                }

                for (const auto& item : Self->IndexBuilds) {
                    TIndexBuildId buildIndexId = item.first;
                    const auto& info = *item.second;

                    bool print = false;
                    if (forActive) {
                        print = !info.IsFinished();
                    } else {
                        print = info.IsFinished();
                    }

                    if (print) {
                        TABLER() {
                            TABLED() {
                                str << "<a href='app?" << TCgi::Page.AsCgiParam(TCgi::TPages::BuildIndexInfo)
                                    << "&" << TCgi::TabletID.AsCgiParam(Self->TabletID())
                                    << "&" << TCgi::BuildIndexId.AsCgiParam(ui64(buildIndexId))
                                    << "'>" << buildIndexId << "</a>"; }
                            TABLED() { str << info.State; }
                        }
                        str << "\n";
                    }
                }
            }
        }
    }

    void BuildIndexInfoPage(TIndexBuildId buildIndexId, TStringStream& str) const {
        HTML(str) {
            TAG(TH3) {str << "Build index id " << buildIndexId;}

            const auto* indexInfoPtr = Self->IndexBuilds.FindPtr(buildIndexId);
            if (!indexInfoPtr) {
                PRE() {
                    str << "Unknown Tx\n";
                }
                return;
            }
            const auto& info = **indexInfoPtr;
            TAG(TH4) {str << "Fields";}
            PRE () {
                str << "BuildInfoId:                   " << info.Id << Endl
                    << "Uid:                           " << info.Uid << Endl

                    << "CancelRequested:               " << (info.CancelRequested ? "YES" : "NO") << Endl

                    << "State:                         " << info.State << Endl
                    << "Issue:                         " << info.Issue << Endl

                    << "Shards.size:                  " << info.Shards.size() << Endl
                    << "ToUploadShards.size:          " << info.ToUploadShards.size() << Endl
                    << "DoneShards.size:              " << info.DoneShardsSize << Endl
                    << "InProgressShards.size:        " << info.InProgressShards.size() << Endl

                    << "DomainPathId:                  " << LinkToPathInfo(info.DomainPathId) << Endl
                    << "DomainPath:                    " << TPath::Init(info.DomainPathId, Self).PathString() << Endl

                    << "TablePathId:                   " << LinkToPathInfo(info.TablePathId) << Endl
                    << "TablePath:                     " << TPath::Init(info.TablePathId, Self).PathString() << Endl

                    << "IndexType:                     " <<  NKikimrSchemeOp::EIndexType_Name(info.IndexType) << Endl

                    << "IndexName:                     " << info.IndexName << Endl;

                    for (const auto& column: info.IndexColumns) {
                        str << "IndexColumns:          " << column << Endl;
                    }

                str << "Subscribers.size:             " << info.Subscribers.size() << Endl

                    << "AlterMainTableTxId:            " << info.AlterMainTableTxId << Endl
                    << "AlterMainTableTxStatus:        " << NKikimrScheme::EStatus_Name(info.AlterMainTableTxStatus) << Endl
                    << "AlterMainTableTxDone:          " << (info.AlterMainTableTxDone ? "DONE": "not done") << Endl

                    << "LockTxId:                      " << info.LockTxId << Endl
                    << "LockTxStatus:                  " << NKikimrScheme::EStatus_Name(info.LockTxStatus) << Endl
                    << "LockTxDone                     " << (info.LockTxDone ? "DONE" : "not done") << Endl

                    << "InitiateTxId:                  " << info.InitiateTxId << Endl
                    << "InitiateTxStatus:              " << NKikimrScheme::EStatus_Name(info.InitiateTxStatus) << Endl
                    << "InitiateTxDone                 " << (info.InitiateTxDone ? "DONE" : "not done") << Endl

                    << "ApplyTxId:                     " << info.ApplyTxId << Endl
                    << "ApplyTxStatus:                 " << NKikimrScheme::EStatus_Name(info.ApplyTxStatus) << Endl
                    << "ApplyTxDone                    " << (info.ApplyTxDone ? "DONE" : "not done") << Endl

                    << "UnlockTxId:                    " << info.UnlockTxId << Endl
                    << "UnlockTxStatus:                " << NKikimrScheme::EStatus_Name(info.UnlockTxStatus) << Endl
                    << "UnlockTxDone                   " << (info.UnlockTxDone ? "DONE" : "not done") << Endl

                    << "SnapshotStep:                  " << info.SnapshotStep << Endl
                    << "SnapshotTxId:                  " << info.SnapshotTxId << Endl

                    << "Processed:                     " << info.Processed << Endl
                    << "Billed:                        " << info.Billed << Endl;

            }

            TVector<NScheme::TTypeInfo> keyTypes;
            if (Self->Tables.contains(info.TablePathId)) {
                TTableInfo::TPtr tableInfo = Self->Tables.at(info.TablePathId);
                for (ui32 keyPos: tableInfo->KeyColumnIds) {
                    keyTypes.push_back(tableInfo->Columns.at(keyPos).PType);
                }
            }

            {
                TAG(TH3) {str << "Shards : " << info.Shards.size() << "\n";}
                TABLE_SORTABLE_CLASS("table") {
                    TABLEHEAD() {
                        TABLER() {
                            TABLEH() {str << "ShardIdx";}
                            TABLEH() {str << "DatashardId";}
                            TABLEH() {str << "Range";}
                            TABLEH() {str << "LastKeyAck";}
                            TABLEH() {str << "Status";}
                            TABLEH() {str << "UploadStatus";}
                            TABLEH() {str << "DebugMessage";}
                            TABLEH() {str << "SeqNo";}
                            TABLEH() {str << "Processed";}
                            TABLEH() {str << "Billed";}
                        }
                    }
                    for (auto item : info.Shards) {
                        TShardIdx idx = item.first;
                        const TIndexBuildInfo::TShardStatus& status = item.second;
                        TABLER() {
                            TABLED() {
                                str << idx;
                            }
                            TABLED() {
                                if (Self->ShardInfos.contains(idx)) {
                                    str << Self->ShardInfos.at(idx).TabletID;
                                } else {
                                    str << "shard " << idx << " has been dropped";
                                }
                            }
                            TABLED() {
                                if (keyTypes) {
                                    str << DebugPrintRange(keyTypes, status.Range.ToTableRange(), *AppData()->TypeRegistry);
                                } else {
                                    str << "table has been dropped";
                                }
                            }
                            TABLED() {
                                if (keyTypes) {
                                    TSerializedCellVec vec;
                                    vec.Parse(status.LastKeyAck);
                                    str << DebugPrintPoint(keyTypes, vec.GetCells(), *AppData()->TypeRegistry);
                                } else {
                                    str << "table has been dropped";
                                }
                            }
                            TABLED() {
                                str << NKikimrTxDataShard::TEvBuildIndexProgressResponse_EStatus_Name(status.Status);
                            }
                            TABLED() {
                                str << Ydb::StatusIds::StatusCode_Name(status.UploadStatus);
                            }
                            TABLED() {
                                str << status.DebugMessage;
                            }
                            TABLED() {
                                str << Self->Generation() << ":" << status.SeqNoRound;
                            }
                            TABLED() {
                                str << status.Processed;
                            }
                        }
                        str << "\n";
                    }
                }
            }
        }
    }

    void TableTxInfly(TStringStream& str) const {
        HTML(str) {
            TABLE_SORTABLE_CLASS("table") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() {str << "OpId";}
                        TABLEH() {str << "Type";}
                        TABLEH() {str << "State";}
                        TABLEH() {str << "Shards in progress";}
                    }
                    str << "\n";
                }

                for (const auto& tx : Self->TxInFlight) {
                    TOperationId opId = tx.first;
                    const TTxState txState = tx.second;
                    TABLER() {
                        TABLED() { str << "<a href='app?" << TCgi::Page.AsCgiParam(TCgi::TPages::TransactionInfo)
                                                          << "&" << TCgi::TabletID.AsCgiParam(Self->TabletID())
                                                          << "&" << TCgi::TxId.AsCgiParam(opId.GetTxId())
                                                          << "&" << TCgi::PartId.AsCgiParam(opId.GetSubTxId())
                                            << "'>" << opId << "</a>"; }
                        TABLED() { str << TTxState::TypeName(txState.TxType); }
                        TABLED() { str << TTxState::StateName(txState.State); }
                        TABLED() { str << txState.ShardsInProgress.size(); }
                    }
                    str << "\n";
                }
            }
        }
    }

    void OutputTxListPage(TStringStream& str) const {
        HTML(str) {
            TAG(TH3) {str << "Transactions in flight:";}

            TableTxInfly(str);
        }
    }

    void OutputTxInfoPage(TOperationId operationId, TStringStream& str) const {
        HTML(str) {
            TAG(TH3) {str << "Transaction " << operationId;}

            auto txInfo = Self->FindTx(operationId);
            if (!txInfo) {
                PRE() {
                    str << "Unknown Tx\n";
                }
            } else {
                const TTxState txState = *txInfo;
                TAG(TH3) {str << "Shards in progress : " << txState.ShardsInProgress.size() << "\n";}
                TABLE_SORTABLE_CLASS("table") {
                    TABLEHEAD() {
                        TABLER() {
                            TABLEH() {str << "OwnerShardIdx";}
                            TABLEH() {str << "LocalShardIdx";}
                            TABLEH() {str << "TabletId";}
                        }
                    }
                    for (auto shardIdx :  txState.ShardsInProgress) {
                        TABLER() {
                            TABLED() {
                                str << "<a href='../tablets/app?" << TCgi::TabletID.AsCgiParam(Self->TabletID())
                                    << "&" << TCgi::Page.AsCgiParam(TCgi::TPages::ShardInfoByShardIdx)
                                    << "&" << TCgi::OwnerShardIdx.AsCgiParam(shardIdx.GetOwnerId())
                                    << "&" << TCgi::LocalShardIdx.AsCgiParam(shardIdx.GetLocalId())
                                    << "'>" << shardIdx <<"</a>";
                            }
                            TABLED() {
                                if (Self->ShardInfos.contains(shardIdx)) {
                                    TTabletId tabletId = Self->ShardInfos.FindPtr(shardIdx)->TabletID;
                                    str << "<a href='../tablets?"
                                        << TCgi::TabletID.AsCgiParam(tabletId)
                                        << "'>" << tabletId <<"</a>";
                                } else {
                                    str << "UNKNOWN_TABLET!";
                                }
                            }
                        }
                        str << "\n";
                    }
                }
            }
        }
    }

    void OutputShardInfo(TShardIdx shardIdx, TStringStream& str) const {
        HTML(str) {
            if (!Self->ShardInfos.contains(shardIdx)) {
                TAG(TH4) {
                    str << "No shard item for shard " << shardIdx << "</a>";
                }
                return;
            }

            const TShardInfo& shard = Self->ShardInfos.at(shardIdx);

            TAG(TH4) {str << "Shard idx " << shardIdx << "</a>";}
            PRE () {
                str << "TabletID:                     " << shard.TabletID<< Endl
                    << "CurrentTxId:                  " << shard.CurrentTxId << Endl
                    << "PathId:                       " << LinkToPathInfo(shard.PathId) << Endl
                    << "TabletType:                   " << TTabletTypes::TypeToStr(shard.TabletType) << Endl;
            }

            TAG(TH4) {str << "BindedChannels for shard idx " << shardIdx << "</a>";}
            TABLE_SORTABLE_CLASS("BindedChannels") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() {str << "ChannelId";}
                        TABLEH() {str << "PoolName";}
                    }
                }
                ui32 channelId = 0;
                for (auto& bind: shard.BindedChannels) {
                    TABLER() {
                        TABLED() { str << channelId; }
                        TABLED() { str << bind.GetStoragePoolName(); }
                    }
                    ++channelId;
                }
            }
        }
    }

    TString LinkToPathInfo(TPathId pathId) const {
        return TStringBuilder()
            << "<a href='../tablets/app?" << TCgi::TabletID.AsCgiParam(Self->TabletID())
            << "&" << TCgi::Page.AsCgiParam(TCgi::TPages::PathInfo)
            << "&" << TCgi::OwnerPathId.AsCgiParam(pathId.OwnerId)
            << "&" << TCgi::LocalPathId.AsCgiParam(pathId.LocalPathId)
            << "'>" << pathId <<"</a>";
    }

    TString LinkToShardInfo(TShardIdx shardIdx) const {
        return TStringBuilder()
            << "<a href='../tablets/app?" << TCgi::TabletID.AsCgiParam(Self->TabletID())
            << "&" << TCgi::Page.AsCgiParam(TCgi::TPages::ShardInfoByShardIdx)
            << "&" << TCgi::OwnerShardIdx.AsCgiParam(shardIdx.GetOwnerId())
            << "&" << TCgi::LocalShardIdx.AsCgiParam(shardIdx.GetLocalId())
             << "'>" << shardIdx <<"</a>";
    }

    TString LinkToTablet(TShardIdx shardIdx) const {
        if (Self->ShardInfos.contains(shardIdx)) {
            TTabletId tabletId = Self->ShardInfos.FindPtr(shardIdx)->TabletID;
            return TStringBuilder()
                << "<a href='../tablets?"
                << TCgi::TabletID.AsCgiParam(tabletId)
                << "'>" << tabletId <<"</a>";
        } else {
            return TStringBuilder()
                    << "UNKNOWN_TABLET!";
        }
    }

    void OutputPathInfo(TPathId pathId, TStringStream& str) const {
        HTML(str) {
            if (!Self->PathsById.contains(pathId)) {
                TAG(TH4) {
                    str << "No path item for shard " << pathId << "</a>";
                }
                return;
            }

            auto& path = Self->PathsById.at(pathId);

            auto localACL = TSecurityObject(path->Owner, path->ACL, path->IsContainer());
            auto effectiveACL = TSecurityObject(path->Owner, path->CachedEffectiveACL.GetForSelf(), path->IsContainer());

            TAG(TH4) {str << "Path info " << pathId << "</a>";}
            PRE () {
                str << "Path:                     " << Self->PathToString(path) << Endl
                    << "PathId:                   " << pathId << Endl
                    << "Parent Path Id:           " << LinkToPathInfo(path->ParentPathId) << Endl
                    << "Name:                     " << path->Name << Endl
                    << "Owner:                    " << path->Owner << Endl
                    << "ACL:                      " << localACL.ToString() << Endl
                    << "ACLVersion:               " << path->ACLVersion << Endl
                    << "EffectiveACL:             " << effectiveACL.ToString() << Endl
                    << "Path Type:                " << NKikimrSchemeOp::EPathType_Name(path->PathType) << Endl
                    << "Path State:               " << NKikimrSchemeOp::EPathState_Name(path->PathState) << Endl
                    << "Created step:             " << path->StepCreated << Endl
                    << "Dropped step:             " << path->StepDropped << Endl
                    << "Created tx:               " << path->CreateTxId << Endl
                    << "Dropped tx:               " << path->DropTxId << Endl
                    << "Last tx:                  " << path->LastTxId << Endl
                    << "Has PreSerializedChildrenListing: " << !path->PreSerializedChildrenListing.empty() << Endl
                    << "Children count:           " << path->GetChildren().size() << Endl
                    << "Alive children count:     " << path->GetAliveChildren() << Endl
                    << "Dir alter version:        " << path->DirAlterVersion << Endl
                    << "User attrs alter version  " << path->UserAttrs->AlterVersion << Endl
                    << "User attrs count          " << path->UserAttrs->Attrs.size() << Endl
                    << "DbRefCount count          " << path->DbRefCount << Endl
                    << "ShardsInside count        " << path->GetShardsInside() << Endl;
            }

            if (path->UserAttrs->Attrs) {
                TAG(TH4) {str << "UserAttrs for pathId " << pathId << "</a>";}
                TABLE_SORTABLE_CLASS("UserAttrs") {
                    TABLEHEAD() {
                        TABLER() {
                            TABLEH() {str << "Name";}
                            TABLEH() {str << "Value";}
                        }
                    }
                    for (auto& item: path->UserAttrs->Attrs) {
                        TABLER() {
                            TABLED() { str << item.first; }
                            TABLED() { str << item.second; }
                        }
                    }
                }
            }

            if (path->GetChildren().size()) {
                TAG(TH4) {str << "Childrens for pathId " << pathId << "</a>";}
                TABLE_SORTABLE_CLASS("UserAttrs") {
                    TABLEHEAD() {
                        TABLER() {
                            TABLEH() {str << "Name";}
                            TABLEH() {str << "PathId";}
                            TABLEH() {str << "IsAlive";}
                        }
                    }
                    for (auto& item: path->GetChildren()) {
                        auto& child = Self->PathsById.at(item.second);

                        TABLER() {
                            TABLED() { str << item.first; }
                            TABLED() {
                                str << LinkToPathInfo(item.second);
                            }
                            TABLED() {
                                str << (child->Dropped() ? "Deleted" : "Alive");
                            }
                        }
                    }
                }
            }

            auto shards = Self->CollectAllShards({pathId});

            TAG(TH4) {str << "Shards for pathId " << pathId << "</a>";}
            TABLE_SORTABLE_CLASS("ShardForPath") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() {str << "ShardIdx";}
                        TABLEH() {str << "TableId";}
                        TABLEH() {str << "IsActive";}
                    }
                }

                TPath path_ = TPath::Init(pathId, Self);

                for (const auto& shardIdx: shards) {
                    TABLER() {
                        TABLED() {
                            str << LinkToShardInfo(shardIdx);
                        }
                        TABLED() {
                            str << LinkToTablet(shardIdx);
                        }
                        TABLED() {
                            if (path->Dropped() || !path->IsTable() || !Self->Tables.contains(pathId)) {
                                str << "path is dropped or is not a table";
                            } else {
                                const TTableInfo::TPtr table = Self->Tables.at(pathId);
                                str << (table->GetShard2PartitionIdx().contains(shardIdx) ? "Active" : "Inactive");
                            }
                        }
                    }
                }
                str << "\n";
            }
        }
    }

    void OutputOperationPartInfo(TOperationId opId, TStringStream& str) const {
        HTML(str) {
            if (!Self->Operations.contains(opId.GetTxId())) {
                TAG(TH4) {
                    str << "No operations for tx id " << opId << "</a>";
                }
                return;
            }

            TOperation::TPtr operation = Self->Operations.at(opId.GetTxId());

            if (ui64(opId.GetSubTxId()) >= operation->Parts.size()) {
                TAG(TH4) {
                    str << "No part operations for operation id " << opId << "</a>";
                }
                return;
            }

            if (!Self->TxInFlight.contains(opId)) {
                TAG(TH4) {
                    str << "No txInfly record for operation id " << opId << "</a>";
                }
                return;
            }

            const TTxState& txState = Self->TxInFlight.at(opId);


            TAG(TH4) {str << "TxState info " << opId << "</a>";}
            PRE () {
                str << "TxType:                                  " << TTxState::TypeName(txState.TxType) << Endl
                    << "TargetPathId:                            " << LinkToPathInfo(txState.TargetPathId) << Endl
                    << "SourcePathId:                            " << LinkToPathInfo(txState.SourcePathId) << Endl
                    << "State:                                   " << TTxState::StateName(txState.State) << Endl
                    << "MinStep:                                 " << txState.MinStep << Endl
                    << "ReadyForNotifications:                   " << txState.ReadyForNotifications << Endl
                    << "DataTotalSize:                           " << txState.DataTotalSize << Endl
                    << "TxShardsListFinalized:                   " << txState.TxShardsListFinalized << Endl
                    << "SchemeOpSeqNo:                           " << txState.SchemeOpSeqNo.Generation << ":" << txState.SchemeOpSeqNo.Round << Endl
                    << "StartTime:                               " << txState.StartTime << Endl
                    << "Shards count:                            " << txState.Shards.size() << Endl
                    << "Shards in progress count:                " << txState.ShardsInProgress.size() << Endl
                    << "SchemeChangeNotificationReceived count:  " << txState.SchemeChangeNotificationReceived.size() << Endl
                    << "SplitDescription:                        " << (txState.SplitDescription ? txState.SplitDescription->ShortDebugString() : "") << Endl
                    << "Dependent operations count:              " << operation->DependentOperations.size() << Endl
                    << "Wait operations count:                   " << operation->WaitOperations.size() << Endl;
            }

            TAG(TH4) {str << "Dependent operations for txId " << opId.GetTxId() << "</a>";}
            TABLE_SORTABLE_CLASS("DependentTxId") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() {str << "TxId";}
                    }
                }
                for (auto& txId: operation->DependentOperations) {
                    TABLER() {
                        TABLED() { str << txId; }
                    }
                }
            }

            TAG(TH4) {str << "Wait operations for txId " << opId.GetTxId() << "</a>";}
            TABLE_SORTABLE_CLASS("WaitTxId") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() {str << "TxId";}
                    }
                }
                for (auto& txId: operation->WaitOperations) {
                    TABLER() {
                        TABLED() { str << txId; }
                    }
                }
            }


            TAG(TH4) {str << "Shards for opId " << opId << "</a>";}
            TABLE_SORTABLE_CLASS("Shards") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() {str << "ShardId";}
                        TABLEH() {str << "TabletType";}
                        TABLEH() {str << "Operation";}
                        TABLEH() {str << "RangeEnd";}
                    }
                }
                ui32 maxItems = 100;
                for (auto& item: txState.Shards) {
                    if (0 == maxItems) { break; }
                    --maxItems;
                    TABLER() {
                        TABLED() { str << item.Idx; }
                        TABLED() { str << TTabletTypes::TypeToStr(item.TabletType); }
                        TABLED() { str << TTxState::StateName(item.Operation); }
                        TABLED() { str << item.RangeEnd; }
                    }
                }
            }

            TAG(TH4) {str << "Shards in progress for opId " << opId << "</a>";}
            TABLE_SORTABLE_CLASS("Shards") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() {str << "ShardId";}
                    }
                }
                ui32 maxItems = 100;
                for (auto& shardId: txState.ShardsInProgress) {
                    if (0 == maxItems) { break; }
                    --maxItems;
                    TABLER() {
                        TABLED() { str << shardId; }
                    }
                }
            }

            TAG(TH4) {str << "SchemeChangeNotificationReceived for opId " << opId << "</a>";}
            TABLE_SORTABLE_CLASS("SchemeChangeNotificationReceived") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() {str << "ShardId";}
                    }
                }
                ui32 maxItems = 100;
                for (auto& item: txState.SchemeChangeNotificationReceived) {
                    if (0 == maxItems) { break; }
                    --maxItems;
                    TABLER() {
                        TABLED() { str << item.first; }
                    }
                }
            }

            TAG(TH4) {str << "ShardStatuses for opId " << opId << "</a>";}
            TABLE_SORTABLE_CLASS("ShardStatuses") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() {str << "ShardId";}
                        TABLEH() {str << "Success";}
                        TABLEH() {str << "Error";}
                        TABLEH() {str << "BytesProcessed";}
                        TABLEH() {str << "RowsProcessed";}
                    }
                }
                ui32 maxItems = 10;
                for (auto& item: txState.ShardStatuses) {
                    if (0 == maxItems) { break; }
                    --maxItems;
                    TABLER() {
                        TABLED() { str << item.first; }
                        TABLED() { str << item.second.Success; }
                        TABLED() { str << item.second.Error; }
                        TABLED() { str << item.second.BytesProcessed; }
                        TABLED() { str << item.second.RowsProcessed; }
                    }
                }
            }
        }
    }

    void OutputOperationInfo(TTxId txId, TStringStream& str) const {
        HTML(str) {
            if (!Self->Operations.contains(txId)) {
                TAG(TH4) {
                    str << "No operations for tx id " << txId << "</a>";
                }
                return;
            }

            TOperation::TPtr operation = Self->Operations.at(txId);
            for (ui32 partId = 0; partId < operation->Parts.size(); ++partId) {
                OutputOperationPartInfo(TOperationId(txId, partId), str);
            }
        }
    }

    void OutputPathInfoPage(TPathId pathId, TStringStream& str) {
        HTML(str) {
            if (!Self->PathsById.contains(pathId)) {
                TAG(TH4) {
                    str << "No path item for tablet " << pathId << "</a>";
                }
                return;
            }

            OutputPathInfo(pathId, str);

            auto& path = Self->PathsById.at(pathId);

            if (Self->Operations.contains(path->LastTxId)) {
                OutputOperationInfo(path->LastTxId, str);
            }

            //add path specific object
        }
    }
    void OutputShardInfoPageByShardIdx(TShardIdx shardIdx, TStringStream& str) const {
        HTML(str) {
            if (!Self->ShardInfos.contains(shardIdx)) {
                TAG(TH4) {
                    str << "No shard info for shard idx " << shardIdx;
                }
                return;
            }

            OutputShardInfo(shardIdx, str);

            const TShardInfo& shard = Self->ShardInfos[shardIdx];

            if (!Self->PathsById.contains(shard.PathId)) {
                TAG(TH4) {
                    str << "No path item path id " << shard.PathId;
                }
                return;
            }

            OutputPathInfo(shard.PathId, str);

            if (Self->Operations.contains(shard.CurrentTxId)) {
                OutputOperationInfo(shard.CurrentTxId, str);
            }
        }
    }

    void OutputShardInfoPageByShardID(TTabletId tabletId, TStringStream& str) const {
        HTML(str) {
            if (!Self->TabletIdToShardIdx.contains(tabletId)) {
                TAG(TH4) {
                    str << "No shard info for shard ID "
                        << "<a href='../tablets?TabletID=" << tabletId << "'>" << tabletId << "</a>";
                }
                return;
            }

            TShardIdx shardIdx = Self->TabletIdToShardIdx[tabletId];
            OutputShardInfoPageByShardIdx(shardIdx, str);
        }
    }

    TTxType GetTxType() const override { return TXTYPE_MONITORING; }
};

bool TSchemeShard::OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext &ctx) {
    if (!Executor() || !Executor()->GetStats().IsActive)
        return false;

    if (!ev)
        return true;

    LOG_DEBUG(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Handle TEvRemoteHttpInfo: %s", ev->Get()->Query.data());
    Execute(new TTxMonitoring(this, ev), ctx);

    return true;
}

}}
