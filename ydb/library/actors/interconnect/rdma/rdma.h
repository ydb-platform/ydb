#pragma once

#include <util/generic/noncopyable.h>
#include <util/generic/vector.h>
#include <util/stream/output.h>

extern "C" {
struct ibv_qp;
struct ibv_cq; 
struct ibv_wc;
union ibv_gid; 
}

namespace NActors {
    class TActorSystem;
}

class IOutputStream;

namespace NInterconnect::NRdma {

class TRdmaCtx;
class TCqCommon;
class TCqActor;
struct TEvRdmaIoDone;

class ICq {
    ICq() {};
    friend class TCqCommon;
    friend class TCqActor;
public:
    class IWr {
    public:
        virtual ~IWr() = default;
        virtual ui64 GetId() noexcept = 0; // Returns id to post WR
        virtual void Release() noexcept = 0;
    };
    using TPtr = std::shared_ptr<ICq>;
    virtual ~ICq() = default;
    virtual ibv_cq* GetCq() noexcept = 0;

    struct TBusy {};
    struct TErr {};
    using TAllocResult = std::variant<IWr*, TBusy, TErr>;

    // Alloc ibv work request and set callback to notify complition.
    // returns TBusy in case of no prepare requests
    // returns TErr in case of fatal CQ error. NOTE!!! The callback might be called in this case with TCqErr 
    virtual TAllocResult AllocWr(std::function<void(NActors::TActorSystem* as, TEvRdmaIoDone*)> cb) noexcept = 0;
    static bool IsWrSuccess(const TAllocResult& ar) {
        return std::holds_alternative<IWr*>(ar);
    }
    static bool IsWrBusy(const TAllocResult& ar) {
        return std::holds_alternative<TBusy>(ar);
    }
    static bool IsWrErr(const TAllocResult& ar) {
        return std::holds_alternative<TErr>(ar);
    }
private:
    virtual void NotifyErr() noexcept = 0;

    static TPtr MakeSimpleCq(const TRdmaCtx* ctx, NActors::TActorSystem* as, int maxCqe) noexcept; 
};

class TQueuePair: public NNonCopyable::TMoveOnly {
public:
    TQueuePair() = default;
    ~TQueuePair();
    int Init(TRdmaCtx* ctx, ICq* cq, int maxWr) noexcept;
    int ToRtsState(TRdmaCtx* ctx, ui32 qpNum, const ibv_gid& gid, int mtuIndex) noexcept;
    int SendRdmaReadWr(ui64 wrId, void* mrAddr, ui32 mrlKey, void* dstAddr, ui32 dstRkey, ui32 dstSize) noexcept;
    ui32 GetQpNum() const noexcept;
    void Output(IOutputStream&) const noexcept;
    TRdmaCtx* GetCtx() const noexcept;

private:
    ibv_qp* Qp = nullptr;
    TRdmaCtx* Ctx = nullptr;
};

}
