#include "helpers.h"

class TFailingMtpQueue: public TSimpleThreadPool {
private:
    bool FailOnAdd_ = false;
public:
    void SetFailOnAdd(bool fail = true) {
        FailOnAdd_ = fail;
    }
    bool Add(IObjectInQueue* pObj) override Y_WARN_UNUSED_RESULT {
        if (FailOnAdd_) {
            return false;
        }

        return TSimpleThreadPool::Add(pObj);
    }
    TFailingMtpQueue() = default;
    TFailingMtpQueue(IThreadFactory* pool)
        : TSimpleThreadPool(pool)
    {
    }
};

using TFailingServerMtpQueue =
    TThreadPoolBinder<TFailingMtpQueue, THttpServer::ICallBack>;

namespace NSchemeShardUT_Private {

TSimpleSharedPtr<IThreadPool> CreateFailingServerMtpQueue(THttpServer::ICallBack* serverCB) {
    return new TFailingServerMtpQueue(serverCB, SystemThreadFactory());
}

}
