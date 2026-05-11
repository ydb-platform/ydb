#include "schemeshard_impl.h"

#include <ydb/core/metering/metering.h>
#include <ydb/core/metering/time_grid.h>

#include <library/cpp/json/json_writer.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDBLOG_THIS_FILE_COMPONENT NKikimrServices::FLAT_TX_SCHEMESHARD

namespace NKikimr {
namespace NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TTxServerlessStorageBilling : public TTransactionBase<TSchemeShard> {
    TSideEffects SideEffects;
    const TTimeGrid TimeGrid = TTimeGrid(TDuration::Minutes(1));

    TInstant TimeToNextBill;

    TTxServerlessStorageBilling(TSelf* self)
        : TTransactionBase<TSchemeShard>(self)
    {}

    TTxType GetTxType() const override {
        return TXTYPE_SERVERLESS_STORAGE_BILLING;
    }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override {
        YDBLOG_CTX_DEBUG(ctx, "TTxServerlessStorageBilling.Execute");

        const TPathElement::TPtr dbRootEl = Self->PathsById.at(Self->RootPathId());
        const TSubDomainInfo::TPtr domainDescr = Self->SubDomains.at(Self->RootPathId());
        const TSubDomainInfo::TDiskSpaceUsage& spaceUsage = domainDescr->GetDiskSpaceUsage();

        if (!Self->IsServerlessDomain(TPath::Init(Self->RootPathId(), Self))) {
            YDBLOG_CTX_INFO(ctx, "TTxServerlessStorageBilling: unable to make a bill, domain is not a serverless db, schemeshardId: , domainId: ",
                {"schemeshardId", Self->SelfTabletId()},
                {"domainId", Self->ParentDomainId});
            return true;
        }

        auto now = ctx.Now();
        auto cur = TimeGrid.Get(now);

        //whatever happens after, we want to repeat this transaction at TimeToNextBill
        TimeToNextBill = TimeGrid.GetNext(cur).Start;

        if (!Self->AllowServerlessStorageBilling) {
            YDBLOG_CTX_INFO(ctx, "TTxServerlessStorageBilling: unable to make a bill, AllowServerlessStorageBilling is false, schemeshardId: , domainId: , next retry at: ",
                {"schemeshardId", Self->SelfTabletId()},
                {"domainId", Self->ParentDomainId},
                {"at", TimeToNextBill});
            return true;
        }

        TString cloud_id;
        if (dbRootEl->UserAttrs->Attrs.contains("cloud_id")) {
            cloud_id = dbRootEl->UserAttrs->Attrs.at("cloud_id");
        }
        TString folder_id;
        if (dbRootEl->UserAttrs->Attrs.contains("folder_id")) {
            folder_id = dbRootEl->UserAttrs->Attrs.at("folder_id");
        }
        TString database_id;
        if (dbRootEl->UserAttrs->Attrs.contains("database_id")) {
            database_id = dbRootEl->UserAttrs->Attrs.at("database_id");
        }

        if (!cloud_id || !folder_id || !database_id) {
            YDBLOG_CTX_INFO(ctx, "TTxServerlessStorageBilling: unable to make a bill, neither cloud_id and nor folder_id nor database_id have found in user attributes at the domain, schemeshardId: , domainId: , next retry at: ",
                {"schemeshardId", Self->SelfTabletId()},
                {"domainId", Self->ParentDomainId},
                {"at", TimeToNextBill});
            return true;
        }

        if (!spaceUsage.Tables.TotalSize) {
            YDBLOG_CTX_INFO(ctx, "TTxServerlessStorageBilling: nothing to bill, schemeshardId: , domainId: , next retry at: ",
                {"schemeshardId", Self->SelfTabletId()},
                {"domainId", Self->ParentDomainId},
                {"at", TimeToNextBill});
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);

        if (!Self->ServerlessStorageLastBillTime) {
            // this is the first run
            // let's make bill time periods according to the grid
            // for that we just skip current grid period
            YDBLOG_CTX_NOTICE(ctx, "TTxServerlessStorageBilling: initiate at first time, schemeshardId: , domainId: , now: , set LastBillTime: , next retry at: ",
                {"schemeshardId", Self->SelfTabletId()},
                {"domainId", Self->ParentDomainId},
                {"now", now},
                {"LastBillTime", cur.Start},
                {"at", TimeToNextBill});

            Self->ServerlessStorageLastBillTime = cur.Start;
            Self->PersistStorageBillingTime(db);

            return true;
        }

        auto last = Self->ServerlessStorageLastBillTime;

        if (now < last) {
            YDBLOG_CTX_DEBUG(ctx, "TTxServerlessStorageBilling: unable do anything from the past, schemeshardId: , domainId: , now: , LastBillTime: , next retry at: ",
                {"schemeshardId", Self->SelfTabletId()},
                {"domainId", Self->ParentDomainId},
                {"now", now},
                {"LastBillTime", last},
                {"at", TimeToNextBill});
            return true;
        }

        auto lastBilled = TimeGrid.Get(last);
        auto toBill = TimeGrid.GetPrev(cur);

        if (now <= lastBilled.End || toBill.Start <= lastBilled.End) {
            YDBLOG_CTX_DEBUG(ctx, "TTxServerlessStorageBilling: too soon call, wait until current period ends, schemeshardId: , domainId: , now: , LastBillTime: , lastBilled: --, toBill: --, next retry at: ",
                {"schemeshardId", Self->SelfTabletId()},
                {"domainId", Self->ParentDomainId},
                {"now", now},
                {"LastBillTime", last},
                {"lastBilled", lastBilled.Start},
                {"#_lastBilled.End", lastBilled.End},
                {"toBill", toBill.Start},
                {"#_toBill.End", toBill.End},
                {"at", TimeToNextBill});
            return true;
        }

        if (now > TimeGrid.GetNext(TimeGrid.GetNext(lastBilled)).End) {
            // it seems like there is a gap in our billing
            // may be SS were offline
            // skip that gap, just bill the last grid period
            YDBLOG_CTX_INFO(ctx, "TTxServerlessStorageBilling: too late call, there are could be gaps in the metric, schemeshardId: , domainId: , now: , LastBillTime: , lastBilled: --, toBill: --, next retry at: ",
                {"schemeshardId", Self->SelfTabletId()},
                {"domainId", Self->ParentDomainId},
                {"now", now},
                {"LastBillTime", last},
                {"lastBilled", lastBilled.Start},
                {"#_lastBilled.End", lastBilled.End},
                {"toBill", toBill.Start},
                {"#_toBill.End", toBill.End},
                {"at", TimeToNextBill});
        }

        Self->ServerlessStorageLastBillTime = toBill.Start;
        Self->PersistStorageBillingTime(db);

        TString id = TStringBuilder()
            << Self->ParentDomainId.OwnerId
            << "-" << Self->ParentDomainId.LocalPathId
            << "-" << toBill.Start.Seconds()
            << "-" << toBill.End.Seconds()
            << "-" << spaceUsage.Tables.TotalSize;

        auto json = NJson::TJsonMap{
            {"version", "1.0.0"},
            {"id", id},
            {"schema", "ydb.serverless.v1"},
            {"cloud_id", cloud_id},
            {"folder_id", folder_id},
            {"resource_id", database_id},
            {"source_id", "sless-docapi-ydb-storage"},
            {"source_wt", ctx.Now().Seconds()},
            {"tags", NJson::TJsonMap {
                 {"ydb_size", spaceUsage.Tables.TotalSize}
            }},
            {"usage", NJson::TJsonMap {
                 {"quantity", toBill.End.Seconds() - toBill.Start.Seconds()},
                 {"unit", "byte*second"},
                 {"type", "delta"},
                 {"start", toBill.Start.Seconds()},
                 {"finish", toBill.End.Seconds()}
             }},
             {"labels", NJson::TJsonMap {
                 {"Category", "Table"},
             }},
        };

        for (const auto& [k, v] : dbRootEl->UserAttrs->Attrs) {
            auto label = TStringBuf(k);
            if (!label.SkipPrefix("label_")) {
                continue;
            }

            json["labels"][label] = v;
        }

        TStringBuilder billRecord;
        NJson::WriteJson(&billRecord.Out, &json, /*formatOutput=*/false, /*sortkeys=*/false);
        billRecord << Endl;

        YDBLOG_CTX_DEBUG(ctx, "TTxServerlessStorageBilling: make a bill, record: '', schemeshardId: , domainId: , now: , LastBillTime: , lastBilled: --, toBill: --, next retry at: ",
            {"#_billRecord", billRecord},
            {"schemeshardId", Self->SelfTabletId()},
            {"domainId", Self->ParentDomainId},
            {"now", now},
            {"LastBillTime", last},
            {"lastBilled", lastBilled.Start},
            {"#_lastBilled.End", lastBilled.End},
            {"toBill", toBill.Start},
            {"#_toBill.End", toBill.End},
            {"at", TimeToNextBill});

        auto request = MakeHolder<NMetering::TEvMetering::TEvWriteMeteringJson>(billRecord);
        // send message at Complete stage
        SideEffects.Send(NMetering::MakeMeteringServiceID(), std::move(request));

        SideEffects.ApplyOnExecute(Self, txc, ctx);
        return true;
    }

    void Complete(const TActorContext &ctx) override {
        YDBLOG_CTX_DEBUG(ctx, "TTxServerlessStorageBilling.Complete");

        if (TimeToNextBill) {
            ctx.Schedule(
                TimeToNextBill,
                new TEvPrivate::TEvServerlessStorageBilling());
        }

        SideEffects.ApplyOnComplete(Self, ctx);
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxServerlessStorageBilling() {
    return new TTxServerlessStorageBilling(this);
}

} // NSchemeShard
} // NKikimr
