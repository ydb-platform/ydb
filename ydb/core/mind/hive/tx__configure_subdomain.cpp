#include "hive_impl.h"
#include "hive_log.h"

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
        BLOG_D("THive::TTxConfigureSubdomain::Execute");

        const auto& domain(Event->Get()->Record.GetDomain());
        Self->PrimaryDomainKey = TSubDomainKey(domain);

        BLOG_D("Switching primary domain to " << domain);

        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::SubDomain>().Key(domain.GetSchemeShard(), domain.GetPathId()).Update<Schema::SubDomain::Primary>(true);

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        BLOG_D("THive::TTxConfigureSubdomain::Complete");
        ctx.Send(Event->Sender, new TEvSubDomain::TEvConfigureStatus(NKikimrTx::TEvSubDomainConfigurationAck::SUCCESS, Self->TabletID()));
    }
};

ITransaction* THive::CreateConfigureSubdomain(TEvHive::TEvConfigureHive::TPtr event) {
    return new TTxConfigureSubdomain(std::move(event), this);
}

} // NHive
} // NKikimr
