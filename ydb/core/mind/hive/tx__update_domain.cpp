#include "hive_impl.h"
#include "hive_log.h"

namespace NKikimr {
namespace NHive {

class TTxUpdateDomain : public TTransactionBase<THive> {
    TSubDomainKey SubdomainKey;

public:
    TTxUpdateDomain(TSubDomainKey subdomainKey, THive* hive)
        : TBase(hive)
        , SubdomainKey(subdomainKey)
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_UPDATE_DOMAIN; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        BLOG_D("THive::TTxUpdateDomain(" << SubdomainKey << ")::Execute");
        TDomainInfo* domain = Self->FindDomain(SubdomainKey);
        if (domain != nullptr) {
            NIceDb::TNiceDb db(txc.DB);
            db.Table<Schema::SubDomain>()
                    .Key(SubdomainKey.first, SubdomainKey.second)
                    .Update<Schema::SubDomain::Path, Schema::SubDomain::HiveId>(domain->Path, domain->HiveId);
        }
        return true;
    }

    void Complete(const TActorContext&) override {
        BLOG_D("THive::TTxUpdateDomain(" << SubdomainKey << ")::Complete");
    }
};

ITransaction* THive::CreateUpdateDomain(TSubDomainKey subdomainKey) {
    return new TTxUpdateDomain(subdomainKey, this);
}

}
}
