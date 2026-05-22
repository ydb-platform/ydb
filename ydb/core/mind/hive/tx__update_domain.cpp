#include "hive_impl.h"
#include "hive_log.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::HIVE

namespace NKikimr {
namespace NHive {

class TTxUpdateDomain : public TTransactionBase<THive> {
    const TSubDomainKey SubdomainKey;
    const TEvHive::TEvUpdateDomain::TPtr Request;    
    TSideEffects SideEffects;

public:
    TTxUpdateDomain(TSubDomainKey subdomainKey, TEvHive::TEvUpdateDomain::TPtr request, THive* hive)
        : TBase(hive)
        , SubdomainKey(subdomainKey)
        , Request(std::move(request))
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_UPDATE_DOMAIN; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        SideEffects.Reset(Self->SelfId());

        YDB_LOG_DEBUG("THive::TTxUpdateDomain( )::Execute",
            {"GetLogPrefix", GetLogPrefix()},
            {"SubdomainKey", SubdomainKey});
        const TDomainInfo* domain = Self->FindDomain(SubdomainKey);
        if (domain == nullptr) {
            YDB_LOG_WARN("THive::TTxUpdateDomain( )::Execute - unknown subdomain",
                {"GetLogPrefix", GetLogPrefix()},
                {"SubdomainKey", SubdomainKey});
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::SubDomain>()
                .Key(SubdomainKey.first, SubdomainKey.second)
                .Update<Schema::SubDomain::Path>(domain->Path)
                .Update<Schema::SubDomain::HiveId>(domain->HiveId);
        if (domain->ServerlessComputeResourcesMode) {
            db.Table<Schema::SubDomain>()
                .Key(SubdomainKey.first, SubdomainKey.second)
                .Update<Schema::SubDomain::ServerlessComputeResourcesMode>(*domain->ServerlessComputeResourcesMode);
        } else {
            db.Table<Schema::SubDomain>()
                .Key(SubdomainKey.first, SubdomainKey.second)
                .UpdateToNull<Schema::SubDomain::ServerlessComputeResourcesMode>();
        }

        if (Request) {
            auto response = std::make_unique<TEvHive::TEvUpdateDomainReply>();
            response->Record.SetTxId(Request->Get()->Record.GetTxId());
            response->Record.SetOrigin(Self->TabletID());
            SideEffects.Send(Request->Sender, response.release(), 0, Request->Cookie);
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        YDB_LOG_DEBUG("THive::TTxUpdateDomain( )::Complete",
            {"GetLogPrefix", GetLogPrefix()},
            {"SubdomainKey", SubdomainKey});
        SideEffects.Complete(ctx);
    }
};

ITransaction* THive::CreateUpdateDomain(TSubDomainKey subdomainKey, TEvHive::TEvUpdateDomain::TPtr event) {
    return new TTxUpdateDomain(subdomainKey, std::move(event), this);
}

} // NHive
} // NKikimr
