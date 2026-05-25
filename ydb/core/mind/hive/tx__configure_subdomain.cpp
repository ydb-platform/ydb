#include "hive_impl.h"
#include "hive_log.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::HIVE

namespace NKikimr {
namespace NHive {

class TTxConfigureSubdomain : public TTransactionBase<THive> {
private:
    TEvHive::TEvConfigureHive::TPtr Event;

public:
    TTxConfigureSubdomain(TEvHive::TEvConfigureHive::TPtr event, THive* hive)
        : TBase(hive)
        , Event(std::move(event))
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_CONFIGURE_SUBDOMAIN; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        YDB_LOG_DEBUG("THive::TTxConfigureSubdomain::Execute",
            {"GetLogPrefix", GetLogPrefix()});

        const auto& domain(Event->Get()->Record.GetDomain());
        Self->PrimaryDomainKey = TSubDomainKey(domain);

        YDB_LOG_DEBUG("Switching primary domain to",
            {"GetLogPrefix", GetLogPrefix()},
            {"domain", domain});

        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::SubDomain>().Key(domain.GetSchemeShard(), domain.GetPathId()).Update<Schema::SubDomain::Primary>(true);

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        YDB_LOG_DEBUG("THive::TTxConfigureSubdomain::Complete",
            {"GetLogPrefix", GetLogPrefix()});
        ctx.Send(Event->Sender, new TEvSubDomain::TEvConfigureStatus(NKikimrTx::TEvSubDomainConfigurationAck::SUCCESS, Self->TabletID()));
    }
};

ITransaction* THive::CreateConfigureSubdomain(TEvHive::TEvConfigureHive::TPtr event) {
    return new TTxConfigureSubdomain(std::move(event), this);
}

} // NHive
} // NKikimr
