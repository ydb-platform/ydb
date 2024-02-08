#include "hive_impl.h"
#include "hive_log.h"
#include <ydb/library/actors/interconnect/interconnect.h>

namespace NKikimr {
namespace NHive {

class TTxInitScheme : public TTransactionBase<THive> {
public:
    TTxInitScheme(THive *hive)
        : TBase(hive)
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_INIT_SCHEME; }

    bool Execute(TTransactionContext &txc, const TActorContext&) override {
        BLOG_D("THive::TTxInitScheme::Execute");
        bool wasEmpty = txc.DB.GetScheme().IsEmpty();
        NIceDb::TNiceDb(txc.DB).Materialize<Schema>();
        if (!wasEmpty) {
            auto row = NIceDb::TNiceDb(txc.DB).Table<Schema::State>().Key(TSchemeIds::State::DatabaseVersion).Select<Schema::State::Value>();
            if (!row.IsReady())
                return false;
            auto version = row.GetValue<Schema::State::Value>();
            switch (version) {
            case 3:
            case 4:
            case 5:
            case 6:
            case 7:
            case 8:
            case 9:
            case 10: {
                break;
            }
            case 11: {
                NIceDb::TNiceDb db(txc.DB);
                auto tabletRowset = db.Table<Schema::OldTablet>().Range().Select<
                        Schema::OldTablet::ID,
                        Schema::OldTablet::CrossDataCenterFollowers,
                        Schema::OldTablet::FollowerCount,
                        Schema::OldTablet::AllowFollowerPromotion>();
                if (!tabletRowset.IsReady())
                    return false;
                while (!tabletRowset.EndOfSet()) {
                    TTabletId tabletId = tabletRowset.GetValue<Schema::OldTablet::ID>();
                    ui32 followerCount = tabletRowset.GetValueOrDefault<Schema::OldTablet::FollowerCount>(0);
                    bool crossDataCenterFollowers = tabletRowset.GetValueOrDefault<Schema::OldTablet::CrossDataCenterFollowers>(false);
                    if (followerCount == 0 && crossDataCenterFollowers) {
                        followerCount = Self->GetDataCenters();
                    }
                    bool allowFollowerPromotion = tabletRowset.GetValueOrDefault<Schema::OldTablet::AllowFollowerPromotion>();
                    if (followerCount > 0) {
                        db.Table<Schema::TabletFollowerGroup>().Key(tabletId, 1).Update(
                                    NIceDb::TUpdate<Schema::TabletFollowerGroup::FollowerCount>(followerCount),
                                    NIceDb::TUpdate<Schema::TabletFollowerGroup::AllowLeaderPromotion>(allowFollowerPromotion),
                                    NIceDb::TUpdate<Schema::TabletFollowerGroup::AllowClientRead>(true));
                    }
                    if (!tabletRowset.Next())
                        return false;
                }
                break;
            }
            case 12:
            case 13: {
                NIceDb::TNiceDb db(txc.DB);
                auto tabletRowset = db.Table<Schema::OldTablet>().Range().Select<
                        Schema::OldTablet::ID,
                        Schema::OldTablet::CrossDataCenterFollowerCount,
                        Schema::OldTablet::FollowerCount,
                        Schema::OldTablet::AllowFollowerPromotion>();
                if (!tabletRowset.IsReady())
                    return false;
                while (!tabletRowset.EndOfSet()) {
                    TTabletId tabletId = tabletRowset.GetValue<Schema::OldTablet::ID>();
                    ui32 crossDataCenterFollowerCount = tabletRowset.GetValueOrDefault<Schema::OldTablet::CrossDataCenterFollowerCount>(0);
                    ui32 followerCount = tabletRowset.GetValueOrDefault<Schema::OldTablet::FollowerCount>(Self->GetDataCenters() * crossDataCenterFollowerCount);
                    bool allowFollowerPromotion = tabletRowset.GetValueOrDefault<Schema::OldTablet::AllowFollowerPromotion>();
                    if (followerCount > 0) {
                        db.Table<Schema::TabletFollowerGroup>().Key(tabletId, 1).Update(
                                    NIceDb::TUpdate<Schema::TabletFollowerGroup::FollowerCount>(followerCount),
                                    NIceDb::TUpdate<Schema::TabletFollowerGroup::AllowLeaderPromotion>(allowFollowerPromotion),
                                    NIceDb::TUpdate<Schema::TabletFollowerGroup::AllowClientRead>(true));
                    }
                    if (!tabletRowset.Next())
                        return false;
                }
                break;
            }
            }
        }
        NIceDb::TNiceDb(txc.DB).Table<Schema::State>().Key(TSchemeIds::State::DatabaseVersion).Update(NIceDb::TUpdate<Schema::State::Value>(19));
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        BLOG_D("THive::TTxInitScheme::Complete");
        const TActorId nameserviceId = GetNameserviceActorId();
        ctx.Send(nameserviceId, new TEvInterconnect::TEvListNodes());
    }
};

ITransaction* THive::CreateInitScheme() {
    return new TTxInitScheme(this);
}

} // NHive
} // NKikimr

