#include "cms_impl.h" 
#include "scheme.h" 
 
namespace NKikimr { 
namespace NCms { 
 
class TCms::TTxUpdateDowntimes : public TTransactionBase<TCms> { 
public: 
    TTxUpdateDowntimes(TCms *self) 
        : TBase(self) 
    { 
    } 
 
    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override 
    { 
        LOG_DEBUG_S(ctx, NKikimrServices::CMS, 
                    "TTxUpdateDowntimes Execute"); 
 
        Self->State->Downtimes.DbStoreState(txc, ctx); 
        Self->State->Downtimes.CleanupEmpty(); 
 
        return true; 
    } 
 
    void Complete(const TActorContext &ctx) override 
    { 
        LOG_DEBUG(ctx, NKikimrServices::CMS, "TTxUpdateDowntimes Complete"); 
    } 
 
private: 
}; 
 
ITransaction *TCms::CreateTxUpdateDowntimes() 
{ 
    return new TTxUpdateDowntimes(this); 
} 
 
} // NCms 
} // NKikimr 
