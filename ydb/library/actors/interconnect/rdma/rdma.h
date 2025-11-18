#pragma once

#include <util/generic/noncopyable.h>
#include <util/generic/vector.h>
#include <util/stream/output.h>

#include <optional>

struct ibv_qp;
struct ibv_cq;
struct ibv_wc;
union ibv_gid;
struct ibv_send_wr;


namespace NActors {
    class TActorSystem;
}

namespace NMonitoring {
    struct TDynamicCounters;
}

class IOutputStream;

namespace NInterconnect::NRdma {

class TRdmaCtx;
class TCqCommon;
class TCqActor;
struct TEvRdmaIoDone;

class TQueuePair;
class IIbVerbsBuilder;


// Wrapper for ibv Completion Queue
// Hides logic to controll work request count
// https://www.rdmamojo.com/2012/11/03/ibv_create_cq/
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

    struct TBusy {}; // try later
    struct TErr {};  // fatal error, cq must be recreated. All associated qp failed.

    using TAllocResult = std::variant<IWr*, TBusy, TErr>;

    struct TWrStats {
        ui32 Total; // Total number of work requests
        ui32 Ready; // Number of work requests ready to allocate
    };

    virtual std::optional<TErr> DoWrBatchAsync(std::shared_ptr<TQueuePair> qp, std::unique_ptr<IIbVerbsBuilder> builder) noexcept = 0;
    virtual TWrStats GetWrStats() const noexcept = 0;

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
};

ICq::TPtr CreateSimpleCq(const TRdmaCtx* ctx, NActors::TActorSystem* as, int maxCqe, int maxWr, NMonitoring::TDynamicCounters* counter) noexcept;

struct THandshakeData {
    ui32 QpNum;
    ui64 SubnetPrefix;
    ui64 InterfaceId;
    ui32 MtuIndex;
};

// Wrapper for ibv Queue Pair
// https://www.rdmamojo.com/2012/12/21/ibv_create_qp/
class TQueuePair: public NNonCopyable::TMoveOnly {
public:
    using TPtr = std::shared_ptr<TQueuePair>;
    struct TQpS {
        int State;
    };
    static bool IsRtsState(TQpS state);
    struct TQpErr {
        int Err;
    };
    using TQpState = std::variant<TQpS, TQpErr>;
    TQueuePair() = default;
    ~TQueuePair();
    int Init(TRdmaCtx* ctx, ICq* cq, int maxWr) noexcept;
    int ToErrorState() noexcept;
    int ToResetState() noexcept;
    // IBV_QPS_RTS - Ready To Send state
    // https://www.rdmamojo.com/2013/01/12/ibv_modify_qp/
    int ToRtsState(const THandshakeData& hd) noexcept;
    int PostSend(struct ::ibv_send_wr *wr, struct ::ibv_send_wr **bad_wr) noexcept;
    ui32 GetQpNum() const noexcept;
    THandshakeData GetHandshakeData() const noexcept;
    void Output(IOutputStream&) const noexcept;
    TQpState GetState(bool forseUpdate) const noexcept;
    TRdmaCtx* GetCtx() const noexcept;
    size_t GetDeviceIndex() const noexcept;
    ui32 GetMinMtuIndex(ui32 mtuIndex) const noexcept;

private:
    static const int UnknownQpState;
    ibv_qp* Qp = nullptr;
    TRdmaCtx* Ctx = nullptr;
    mutable int LastState = UnknownQpState;
};

class TIbVerbsBuilderImpl;

class IIbVerbsBuilder : public NNonCopyable::TNonCopyable {
    // Do not allow other implementations
    friend class TIbVerbsBuilderImpl;
public:
    virtual ~IIbVerbsBuilder() = default;
    virtual void AddReadVerb(void* mrAddr, ui32 mrlKey, void* dstAddr, ui32 dstRkey, ui32 dstSize,
        std::function<void(NActors::TActorSystem* as, TEvRdmaIoDone*)> ioCb) noexcept = 0;
private:
    IIbVerbsBuilder() noexcept = default;
};

std::unique_ptr<IIbVerbsBuilder> CreateIbVerbsBuilder(size_t hint) noexcept;


}
