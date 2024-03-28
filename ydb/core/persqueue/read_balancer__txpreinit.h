#pragma once

#include "read_balancer.h"
#include "read_balancer__txinit.h"

#include <ydb/core/tablet_flat/tablet_flat_executed.h>

namespace NKikimr {
namespace NPQ {

using namespace NTabletFlatExecutor;
using namespace NPQRBPrivate;

struct TPersQueueReadBalancer::TTxPreInit : public ITransaction {
    TPersQueueReadBalancer * const Self;

    TTxPreInit(TPersQueueReadBalancer *self)
        : Self(self)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        NIceDb::TNiceDb(txc.DB).Materialize<Schema>();
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        Self->Execute(new TTxInit(Self), ctx);
    }
};

}
}
