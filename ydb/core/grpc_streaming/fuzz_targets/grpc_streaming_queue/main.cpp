#include <ydb/core/grpc_streaming/grpc_streaming.h>
#include <ydb/library/grpc/server/grpc_request.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <google/protobuf/empty.pb.h>

#include <util/generic/deque.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/yassert.h>

#include <algorithm>
#include <type_traits>

namespace {

constexpr size_t MaxOps = 192;
constexpr size_t MaxCallbacks = 768;
constexpr size_t MaxPayload = 64;
constexpr ui8 MaxNestedPerCallback = 3;
constexpr ui8 MaxNestedDepth = 4;
constexpr size_t MaxDrain = 1024;

struct TFuzzStreamingGrpcService {
    class AsyncService;

    static const char* service_full_name() {
        return "ydb.fuzz.StreamingService";
    }
};

struct TFuzzStreamingServer {
    using TCurrentGRpcService = TFuzzStreamingGrpcService;
};

using TFuzzStreamingRequest = NKikimr::NGRpcServer::TGRpcStreamingRequest<
    google::protobuf::Empty,
    google::protobuf::Empty,
    TFuzzStreamingServer,
    413>;

static_assert(std::is_base_of<TThrRefBase, TFuzzStreamingRequest>::value,
    "TGRpcStreamingRequest must remain intrusive-refcounted");
static_assert(std::is_same<
    typename TFuzzStreamingRequest::IContext,
    NKikimr::NGRpcServer::IGRpcStreamingContext<google::protobuf::Empty, google::protobuf::Empty>>::value,
    "TGRpcStreamingRequest facade context type changed");

struct TTask {
    ui64 Id = 0;
    bool Urgent = false;
    ui8 Depth = 0;
    ui8 NestedCount = 0;
    ui8 NestedUrgentMask = 0;
    ui8 ChainBudget = 0;
    ui8 Weight = 0;
    TString Payload;
};

class TAdaptorModel {
public:
    explicit TAdaptorModel(NYdbGrpc::IStreamAdaptor& adaptor)
        : Adaptor(adaptor)
    {}

    void EnqueueRoot(FuzzedDataProvider& fdp) {
        if (!CanCreate()) {
            return;
        }

        TTask task;
        task.Id = NextId++;

        const ui8 flags = fdp.ConsumeIntegral<ui8>();
        task.Urgent = flags & 1;
        task.NestedCount = std::min<ui8>((flags >> 1) % (MaxNestedPerCallback + 1), MaxNestedPerCallback);
        task.ChainBudget = std::min<ui8>((flags >> 4) % (MaxNestedDepth + 1), MaxNestedDepth);
        task.NestedUrgentMask = fdp.ConsumeIntegral<ui8>();
        task.Weight = fdp.ConsumeIntegral<ui8>();

        const ui8 wantedLen = fdp.ConsumeIntegral<ui8>();
        const size_t len = std::min<size_t>(wantedLen % (MaxPayload + 1), fdp.remaining_bytes());
        task.Payload = fdp.ConsumeBytesAsString(len);

        Enqueue(std::move(task));
    }

    void ProcessNext() {
        CheckInvariants();

        if (!Busy) {
            return;
        }

        TMaybe<TTask> expected;
        if (!UrgentQueue.empty()) {
            expected = std::move(UrgentQueue.front());
            UrgentQueue.pop_front();
        } else if (!NormalQueue.empty()) {
            expected = std::move(NormalQueue.front());
            NormalQueue.pop_front();
        }

        if (!expected.Defined()) {
            Busy = false;
            const size_t left = Adaptor.ProcessNext();
            Y_ABORT_UNLESS(left == 0);
            CheckInvariants();
            return;
        }

        const size_t expectedLeft = QueueSize();
        ExpectedId = expected->Id;
        const size_t left = Adaptor.ProcessNext();

        Y_ABORT_UNLESS(left == expectedLeft);
        Y_ABORT_UNLESS(!ExpectedId.Defined());
        Y_ABORT_UNLESS(Busy);
        CheckInvariants();
    }

    void ProcessBurst(size_t budget) {
        budget = std::min(budget, MaxDrain);
        for (size_t i = 0; i < budget && Busy; ++i) {
            ProcessNext();
        }
    }

    void Drain() {
        for (size_t i = 0; i < MaxDrain && Busy; ++i) {
            ProcessNext();
        }
    }

private:
    bool CanCreate() const {
        return NextId < MaxCallbacks;
    }

    size_t QueueSize() const {
        return UrgentQueue.size() + NormalQueue.size();
    }

    void Enqueue(TTask&& task) {
        CheckInvariants();

        const bool runsNow = !Busy;
        const bool urgent = task.Urgent;
        const ui64 beforeExecuted = Executed;

        if (runsNow) {
            Busy = true;
            ExpectedId = task.Id;
        } else if (urgent) {
            UrgentQueue.push_back(task);
        } else {
            NormalQueue.push_back(task);
        }

        Adaptor.Enqueue(MakeCallback(std::move(task)), urgent);

        if (runsNow) {
            Y_ABORT_UNLESS(!ExpectedId.Defined());
        } else {
            Y_ABORT_UNLESS(Executed == beforeExecuted);
        }

        CheckInvariants();
    }

    std::function<void()> MakeCallback(TTask&& task) {
        return [this, task = std::move(task)]() mutable {
            Execute(task);
        };
    }

    void Execute(const TTask& task) {
        Y_ABORT_UNLESS(Busy);
        Y_ABORT_UNLESS(ExpectedId.Defined());
        Y_ABORT_UNLESS(*ExpectedId == task.Id);
        Y_ABORT_UNLESS(ActiveCallbacks == 0);

        ExpectedId.Clear();
        ++ActiveCallbacks;
        ++Executed;
        Checksum = Checksum * 1315423911u + task.Id + task.Weight + task.Payload.size();

        const ui8 nestedCount = std::min<ui8>(task.NestedCount, MaxNestedPerCallback);
        for (ui8 i = 0; i < nestedCount && task.Depth < MaxNestedDepth && CanCreate(); ++i) {
            TTask nested;
            nested.Id = NextId++;
            nested.Urgent = (task.NestedUrgentMask >> i) & 1;
            nested.Depth = task.Depth + 1;
            nested.NestedUrgentMask = static_cast<ui8>(task.NestedUrgentMask * 33u + task.Weight + i);
            nested.ChainBudget = task.ChainBudget ? task.ChainBudget - 1 : 0;
            nested.NestedCount = nested.ChainBudget ? 1 + ((task.NestedUrgentMask >> (i + 3)) & 1) : 0;
            nested.Weight = static_cast<ui8>(task.Weight + i + task.Payload.size());
            nested.Payload = task.Payload.substr(0, std::min<size_t>(task.Payload.size(), 16));

            Enqueue(std::move(nested));
        }

        --ActiveCallbacks;
        CheckInvariants();
    }

    void CheckInvariants() const {
        Y_ABORT_UNLESS(ActiveCallbacks <= 1);
        Y_ABORT_UNLESS(QueueSize() <= MaxCallbacks);
        Y_ABORT_UNLESS(Busy || QueueSize() == 0);
        Y_ABORT_UNLESS(Executed <= NextId);
        (void)Checksum;
    }

private:
    NYdbGrpc::IStreamAdaptor& Adaptor;
    TDeque<TTask> UrgentQueue;
    TDeque<TTask> NormalQueue;
    TMaybe<ui64> ExpectedId;
    ui64 NextId = 0;
    ui64 Executed = 0;
    ui64 Checksum = 0;
    size_t ActiveCallbacks = 0;
    bool Busy = false;
};

void ExerciseAdaptor(FuzzedDataProvider& fdp) {
    auto adaptor = NYdbGrpc::CreateStreamAdaptor();
    if (!adaptor) {
        return;
    }

    TAdaptorModel model(*adaptor);

    const size_t ops = fdp.ConsumeIntegralInRange<ui8>(0, MaxOps);
    for (size_t i = 0; i < ops && fdp.remaining_bytes(); ++i) {
        const ui8 command = fdp.ConsumeIntegral<ui8>();
        switch (command % 8) {
            case 0:
            case 1:
            case 2:
            case 3:
                model.EnqueueRoot(fdp);
                break;
            case 4:
            case 5:
                model.ProcessNext();
                break;
            case 6:
                model.ProcessBurst(fdp.ConsumeIntegralInRange<ui8>(0, 16));
                break;
            case 7:
                model.Drain();
                break;
        }
    }

    model.ProcessBurst(fdp.ConsumeIntegralInRange<ui16>(0, MaxDrain));
    if (fdp.ConsumeBool()) {
        model.Drain();
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    FuzzedDataProvider fdp(data, size);
    ExerciseAdaptor(fdp);
    return 0;
}
