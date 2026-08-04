#include <ydb/library/actors/util/rope.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/string.h>
#include <util/system/event.h>
#include <util/system/thread.h>
#include <util/system/yassert.h>

#include <algorithm>
#include <atomic>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

namespace {

constexpr size_t MaxOps = 128;
constexpr size_t MaxStateSize = 4096;
constexpr size_t MaxPieceSize = 192;
constexpr size_t MaxRoom = 64;
constexpr size_t ArenaChunkSize = 65536;
constexpr size_t MaxThreads = 4;
constexpr size_t MaxThreadSteps = 8;

class TStartLatch {
public:
    explicit TStartLatch(size_t count)
        : Left_(count)
    {}

    void Wait() {
        if (Left_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            Event_.Signal();
        }
        Event_.Wait();
    }

private:
    std::atomic<size_t> Left_;
    TSystemEvent Event_;
};

class TFuzzChunk final : public IContiguousChunk {
    TString Buffer;

public:
    explicit TFuzzChunk(TString buffer)
        : Buffer(std::move(buffer))
    {}

    TContiguousSpan GetData() const override {
        return {Buffer.data(), Buffer.size()};
    }

    TMutableContiguousSpan UnsafeGetDataMut() override {
        return {const_cast<char*>(Buffer.data()), Buffer.size()};
    }

    size_t GetOccupiedMemorySize() const override {
        return Buffer.capacity();
    }

    IContiguousChunk::TPtr Clone() override {
        auto clone = MakeIntrusive<TFuzzChunk>(Buffer);
        clone->Buffer.Detach();
        return clone;
    }
};

TString ToTString(std::string value) {
    return TString(value.data(), value.size());
}

TString ConsumeBytes(FuzzedDataProvider& fdp, size_t maxLen) {
    const size_t limit = std::min(maxLen, fdp.remaining_bytes());
    const size_t len = fdp.ConsumeIntegralInRange<size_t>(0, limit);
    return ToTString(fdp.ConsumeBytesAsString(len));
}

size_t ConsumeRoom(FuzzedDataProvider& fdp) {
    return fdp.ConsumeIntegralInRange<size_t>(0, MaxRoom);
}

bool CanAppend(size_t current, size_t add) {
    return add <= MaxStateSize && current <= MaxStateSize - add;
}

template <class TCallable>
void RunThreads(size_t threads, TCallable&& callable) {
    TStartLatch latch(threads);
    std::vector<std::unique_ptr<TThread>> workers;
    workers.reserve(threads);

    for (size_t thread = 0; thread < threads; ++thread) {
        workers.push_back(std::make_unique<TThread>([&, thread] {
            latch.Wait();
            callable(thread);
        }));
    }

    for (auto& worker : workers) {
        worker->Start();
    }
    for (auto& worker : workers) {
        worker->Join();
    }
}

TRcBuf MakeCopiedRcBuf(const TString& bytes, size_t headroom, size_t tailroom) {
    return TRcBuf::Copy(bytes.data(), bytes.size(), headroom, tailroom);
}

TRcBuf MakeRcBuf(FuzzedDataProvider& fdp, const TString& bytes) {
    switch (fdp.ConsumeIntegralInRange<int>(0, 4)) {
        case 0:
            return TRcBuf(TString(bytes));
        case 1:
            return MakeCopiedRcBuf(bytes, ConsumeRoom(fdp), ConsumeRoom(fdp));
        case 2: {
            TRcBuf buf = TRcBuf::Uninitialized(bytes.size(), ConsumeRoom(fdp), ConsumeRoom(fdp));
            if (bytes) {
                std::memcpy(buf.UnsafeGetDataMut(), bytes.data(), bytes.size());
            }
            return buf;
        }
        case 3:
            return bytes
                ? TRcBuf(NActors::TSharedData::Copy(bytes.data(), bytes.size()))
                : TRcBuf(TString());
        default:
            return bytes
                ? TRcBuf(MakeIntrusive<TFuzzChunk>(TString(bytes)))
                : TRcBuf(TString());
    }
}

std::pair<TRcBuf, TString> MakeRcBufWithOptionalSlice(FuzzedDataProvider& fdp, size_t maxLen) {
    TString bytes = ConsumeBytes(fdp, maxLen);
    TRcBuf buf = MakeRcBuf(fdp, bytes);

    if (bytes && fdp.ConsumeBool()) {
        const size_t pos = fdp.ConsumeIntegralInRange<size_t>(0, bytes.size() - 1);
        const size_t len = fdp.ConsumeIntegralInRange<size_t>(1, bytes.size() - pos);
        buf = TRcBuf(TRcBuf::Piece, buf.data() + pos, len, buf);
        bytes = bytes.substr(pos, len);
    }

    return {std::move(buf), std::move(bytes)};
}

TRope MakeRope(FuzzedDataProvider& fdp, const TString& bytes, size_t depth = 0) {
    if (!bytes) {
        return TRope(TString());
    }

    switch (fdp.ConsumeIntegralInRange<int>(0, 5)) {
        case 0:
            return TRope(TString(bytes));
        case 1:
            return TRope(MakeRcBuf(fdp, bytes));
        case 2:
            return TRope(MakeIntrusive<TFuzzChunk>(TString(bytes)));
        case 3:
            return TRope(NActors::TSharedData::Copy(bytes.data(), bytes.size()));
        case 4: {
            if (depth >= 3) {
                return TRope(TString(bytes));
            }
            TRope rope;
            size_t pos = 0;
            while (pos < bytes.size()) {
                const size_t remain = bytes.size() - pos;
                const size_t chunk = fdp.ConsumeIntegralInRange<size_t>(1, std::min<size_t>(remain, 32));
                rope.Insert(rope.End(), MakeRope(fdp, bytes.substr(pos, chunk), depth + 1));
                pos += chunk;
            }
            return rope;
        }
        default: {
            TString prefix = ConsumeBytes(fdp, 16);
            TString suffix = ConsumeBytes(fdp, 16);
            TString backing = prefix + bytes + suffix;
            TRcBuf buf = MakeRcBuf(fdp, backing);
            return TRope(TRcBuf(TRcBuf::Piece, buf.data() + prefix.size(), bytes.size(), buf));
        }
    }
}

TRope MakeFragmentedRope(FuzzedDataProvider& fdp, const TString& bytes, bool includeEmptyChunks = false) {
    TRope rope;
    size_t pos = 0;
    while (pos < bytes.size()) {
        const size_t remain = bytes.size() - pos;
        const size_t chunk = fdp.ConsumeIntegralInRange<size_t>(1, std::min<size_t>(remain, 64));
        rope.Insert(rope.End(), TRope(TRcBuf::Copy(bytes.data() + pos, chunk)));
        pos += chunk;
        if (includeEmptyChunks && fdp.ConsumeBool()) {
            rope.Insert(rope.End(), TRope(TString()));
        }
    }
    return rope;
}

TString RopeByIterator(const TRope& rope) {
    TString out;
    for (auto it = rope.Begin(); it != rope.End(); it.AdvanceToNextContiguousBlock()) {
        if (it.ContiguousSize() == 0) {
            continue;
        }
        out.append(it.ContiguousData(), it.ContiguousSize());
    }
    return out;
}

bool HasZeroContiguousBlock(const TRope& rope) {
    for (auto it = rope.Begin(); it != rope.End(); it.AdvanceToNextContiguousBlock()) {
        if (it.ContiguousSize() == 0) {
            return true;
        }
    }
    return false;
}

void CheckRope(const TRope& rope, const TString& model) {
    Y_ABORT_UNLESS(rope.GetSize() == model.size());
    Y_ABORT_UNLESS(rope.size() == model.size());
    Y_ABORT_UNLESS(rope.IsEmpty() == model.empty());

    const TString iter = RopeByIterator(rope);
    Y_ABORT_UNLESS(iter == model);
    Y_ABORT_UNLESS(rope.ConvertToString() == model);
    if (!HasZeroContiguousBlock(rope)) {
        Y_ABORT_UNLESS(TRope::Compare(rope, TContiguousSpan(model)) == 0);
        Y_ABORT_UNLESS(TRope::Compare(TContiguousSpan(model), rope) == 0);
    }

    TString copied = TString::Uninitialized(model.size());
    if (model) {
        rope.Begin().ExtractPlainDataAndAdvance(copied.Detach(), copied.size());
    }
    Y_ABORT_UNLESS(copied == model);

    const size_t positions[] = {0, model.size() / 2, model.size()};
    for (size_t pos : positions) {
        auto it = rope.Position(pos);
        while (it != rope.End() && it.ContiguousSize() == 0) {
            it.AdvanceToNextContiguousBlock();
        }
        if (pos == model.size()) {
            Y_ABORT_UNLESS(it == rope.End());
        } else {
            Y_ABORT_UNLESS(it != rope.End());
            Y_ABORT_UNLESS(*it.ContiguousData() == model[pos]);
        }
    }
}

void CheckRcBuf(const TRcBuf& buf, const TString& model) {
    Y_ABORT_UNLESS(buf.GetSize() == model.size());
    Y_ABORT_UNLESS(buf.size() == model.size());
    Y_ABORT_UNLESS(TContiguousSpan(buf) == TContiguousSpan(model));
    if (!model) {
        return;
    }

    Y_ABORT_UNLESS(TString(buf.Slice()) == model);
    Y_ABORT_UNLESS(std::memcmp(buf.GetData(), model.data(), model.size()) == 0);
    const size_t pos = model.size() / 2;
    const size_t len = model.size() - pos;
    Y_ABORT_UNLESS(TString(buf.Slice(pos, len)) == model.substr(pos, len));
    Y_ABORT_UNLESS(buf.Headroom() <= buf.UnsafeHeadroom());
    Y_ABORT_UNLESS(buf.Tailroom() <= buf.UnsafeTailroom());
}

void ExerciseRopeCow(const TRope& rope, const TString& model, char byte) {
    if (!model) {
        return;
    }

    TRope original = rope;
    TRope mutated = rope;
    TString mutatedModel = model;
    const size_t pos = static_cast<unsigned char>(byte) % model.size();

    auto span = mutated.GetContiguousSpanMut();
    Y_ABORT_UNLESS(span.size() == model.size());
    Y_ABORT_UNLESS(static_cast<TContiguousSpan>(span) == TContiguousSpan(model));
    span.data()[pos] = byte;
    mutatedModel[pos] = byte;

    CheckRope(original, model);
    CheckRope(mutated, mutatedModel);
}

void ExerciseRcBufCowNoFdp(const TRcBuf& buf, const TString& model, char byte) {
    if (!model) {
        return;
    }

    TRcBuf original = buf;
    TRcBuf mutated = buf;
    TString mutatedModel = model;
    const size_t pos = static_cast<unsigned char>(byte) % model.size();

    char* data = mutated.GetDataMut();
    data[pos] = byte;
    mutatedModel[pos] = byte;

    CheckRcBuf(original, model);
    CheckRcBuf(mutated, mutatedModel);
}

void ExerciseUnsafeTemporaries(const TString& model, char byte) {
    if (!model) {
        return;
    }

    const size_t pos = static_cast<unsigned char>(byte) % model.size();

    TRcBuf rc = MakeCopiedRcBuf(model, 4, 4);
    rc.UnsafeGetDataMut()[pos] = byte;
    TString rcModel = model;
    rcModel[pos] = byte;
    CheckRcBuf(rc, rcModel);

    TRope rope(TRcBuf::Copy(model.data(), model.size()));
    auto span = rope.UnsafeGetContiguousSpanMut();
    span.data()[pos] = byte;
    TString ropeModel = model;
    ropeModel[pos] = byte;
    CheckRope(rope, ropeModel);
}

void InsertRope(TRope& rope, TString& model, size_t pos, TRope&& add, const TString& bytes) {
    Y_ABORT_UNLESS(CanAppend(model.size(), bytes.size()));
    rope.Insert(rope.Position(pos), std::move(add));
    model.insert(pos, bytes);
}

TRopeArena MakeArena(FuzzedDataProvider& fdp) {
    static constexpr size_t ChunkSizes[] = {
        1, 2, 3, 5, 7, 8, 15, 16, 31, 32, 63, 64, 127, 256, 4096, ArenaChunkSize
    };
    const size_t chunkSize = fdp.PickValueInArray(ChunkSizes);
    return TRopeArena([chunkSize]() -> TIntrusivePtr<IContiguousChunk> {
        return TRopeAlignedBuffer::Allocate(chunkSize);
    });
}

TRope MakeShortLivedArenaRope(FuzzedDataProvider& fdp, const TString& bytes) {
    TRope rope;
    {
        TRopeArena arena = MakeArena(fdp);
        rope = arena.CreateRope(bytes.data(), bytes.size());
        CheckRope(rope, bytes);
    }
    return rope;
}

void ExerciseArena(FuzzedDataProvider& fdp, TRopeArena& arena, TRope& rope, TString& model) {
    TString bytes = ConsumeBytes(fdp, std::min(MaxPieceSize, MaxStateSize - model.size()));
    if (!CanAppend(model.size(), bytes.size())) {
        return;
    }

    TRope made = fdp.ConsumeBool()
        ? arena.CreateRope(bytes.data(), bytes.size())
        : MakeShortLivedArenaRope(fdp, bytes);
    CheckRope(made, bytes);

    const size_t pos = fdp.ConsumeIntegralInRange<size_t>(0, model.size());
    InsertRope(rope, model, pos, std::move(made), bytes);
    CheckRope(rope, model);

    if (model && fdp.ConsumeBool()) {
        TRope origin = MakeRope(fdp, model);
        TRope compacted = TRope::CopySpaceOptimized(std::move(origin),
            fdp.ConsumeIntegralInRange<size_t>(0, 1024), arena);
        CheckRope(compacted, model);
        CheckRope(origin, TString());
    }
}

void ExerciseZeroCopyInput(FuzzedDataProvider& fdp, const TRope& base, const TString& model) {
    TRope rope = fdp.ConsumeBool() ? MakeFragmentedRope(fdp, model, true) : base;
    TRopeZeroCopyInput input(rope.Begin());
    size_t offset = 0;

    const size_t ops = fdp.ConsumeIntegralInRange<size_t>(1, 24);
    for (size_t i = 0; i < ops; ++i) {
        switch (fdp.ConsumeIntegralInRange<int>(0, 3)) {
            case 0: {
                const char* ptr = nullptr;
                const size_t requested = fdp.ConsumeIntegralInRange<size_t>(1, MaxPieceSize);
                const size_t len = input.Next(&ptr, requested);
                Y_ABORT_UNLESS(len <= requested);
                Y_ABORT_UNLESS(len <= model.size() - offset);
                if (len) {
                    Y_ABORT_UNLESS(ptr);
                    Y_ABORT_UNLESS(TStringBuf(ptr, len) == TStringBuf(model.data() + offset, len));
                } else {
                    Y_ABORT_UNLESS(offset == model.size());
                }
                offset += len;
                break;
            }
            case 1: {
                char scratch[MaxPieceSize];
                const size_t requested = fdp.ConsumeIntegralInRange<size_t>(1, sizeof(scratch));
                const size_t len = input.Read(scratch, requested);
                Y_ABORT_UNLESS(len <= requested);
                Y_ABORT_UNLESS(len <= model.size() - offset);
                Y_ABORT_UNLESS(TStringBuf(scratch, len) == TStringBuf(model.data() + offset, len));
                offset += len;
                break;
            }
            case 2: {
                const size_t requested = fdp.ConsumeIntegralInRange<size_t>(1, MaxPieceSize);
                const size_t len = input.Skip(requested);
                Y_ABORT_UNLESS(len <= requested);
                Y_ABORT_UNLESS(len <= model.size() - offset);
                offset += len;
                break;
            }
            default: {
                TString rest = input.ReadAll();
                Y_ABORT_UNLESS(rest == model.substr(offset));
                offset = model.size();
                break;
            }
        }
    }

    const char* ptr = nullptr;
    while (size_t len = input.Next(&ptr)) {
        Y_ABORT_UNLESS(len <= model.size() - offset);
        Y_ABORT_UNLESS(TStringBuf(ptr, len) == TStringBuf(model.data() + offset, len));
        offset += len;
    }
    Y_ABORT_UNLESS(offset == model.size());
}

template <size_t Block>
void ExerciseSlideViewBlock(FuzzedDataProvider& fdp, const TString& model) {
    if (!model) {
        return;
    }

    TRope rope = MakeFragmentedRope(fdp, model);
    TString expected = model;
    size_t pos = fdp.ConsumeIntegralInRange<size_t>(0, model.size() - 1);
    TRopeSlideView<Block> view(rope.Position(pos));

    const size_t ops = fdp.ConsumeIntegralInRange<size_t>(1, 16);
    for (size_t i = 0; i < ops && pos < expected.size(); ++i) {
        const size_t available = std::min(view.ContiguousSize(), expected.size() - pos);
        Y_ABORT_UNLESS(available);
        Y_ABORT_UNLESS(std::memcmp(view.GetHead(), expected.data() + pos, available) == 0);

        const size_t writeLen = fdp.ConsumeIntegralInRange<size_t>(0, std::min<size_t>(available, 16));
        for (size_t j = 0; j < writeLen; ++j) {
            const char byte = fdp.ConsumeIntegral<char>();
            view.GetHead()[j] = byte;
            expected[pos + j] = byte;
        }
        view.FlushBlock();
        CheckRope(rope, expected);

        const size_t remainAfterCurrent = expected.size() - pos - 1;
        if (!remainAfterCurrent) {
            break;
        }
        const size_t step = fdp.ConsumeIntegralInRange<size_t>(1, std::min<size_t>(remainAfterCurrent, Block));
        pos += step;
        view += step;
    }
}

void ExerciseSlideView(FuzzedDataProvider& fdp, const TString& model) {
    switch (fdp.ConsumeIntegralInRange<int>(0, 2)) {
        case 0:
            ExerciseSlideViewBlock<8>(fdp, model);
            break;
        case 1:
            ExerciseSlideViewBlock<16>(fdp, model);
            break;
        default:
            ExerciseSlideViewBlock<32>(fdp, model);
            break;
    }
}

void ExercisePageAlignedRcBuf(FuzzedDataProvider& fdp) {
    TString bytes = ConsumeBytes(fdp, MaxPieceSize);
    const size_t tailroom = ConsumeRoom(fdp);
    TRcBuf buf = TRcBuf::UninitializedPageAligned(bytes.size(), tailroom);
    Y_ABORT_UNLESS(buf.GetSize() == bytes.size());

    if (bytes) {
        std::memcpy(buf.UnsafeGetDataMut(), bytes.data(), bytes.size());
    }
    CheckRcBuf(buf, bytes);

    const size_t pageSize = NSystemInfo::GetPageSize();
    Y_ABORT_UNLESS(pageSize);
    Y_ABORT_UNLESS((reinterpret_cast<uintptr_t>(buf.data()) % pageSize) == 0);
    Y_ABORT_UNLESS(buf.UnsafeTailroom() >= tailroom);

    if (bytes) {
        TRcBuf copy = buf;
        TString copyModel = bytes;

        const size_t backSize = fdp.ConsumeIntegralInRange<size_t>(0, copyModel.size());
        copy.TrimBack(backSize);
        copyModel.resize(backSize);
        CheckRcBuf(copy, copyModel);

        const size_t frontSize = fdp.ConsumeIntegralInRange<size_t>(0, copyModel.size());
        copy.TrimFront(frontSize);
        copyModel = copyModel.substr(copyModel.size() - frontSize);
        CheckRcBuf(copy, copyModel);
    }
}

struct TThreadStep {
    ui8 Kind = 0;
    size_t Begin = 0;
    size_t End = 0;
};

std::vector<std::vector<TThreadStep>> MakeThreadScripts(FuzzedDataProvider& fdp, size_t threads, size_t modelSize) {
    std::vector<std::vector<TThreadStep>> scripts(threads);
    for (size_t thread = 0; thread < threads; ++thread) {
        const size_t steps = fdp.ConsumeIntegralInRange<size_t>(1, MaxThreadSteps);
        scripts[thread].reserve(steps);
        for (size_t i = 0; i < steps; ++i) {
            TThreadStep step;
            step.Kind = fdp.ConsumeIntegral<ui8>();
            if (modelSize) {
                step.Begin = fdp.ConsumeIntegralInRange<size_t>(0, modelSize);
                step.End = fdp.ConsumeIntegralInRange<size_t>(step.Begin, modelSize);
            }
            scripts[thread].push_back(step);
        }
    }
    return scripts;
}

void ExerciseThreadedReadOnly(FuzzedDataProvider& fdp, const TRope& rope, const TString& ropeModel, const TRcBuf& rcBuf, const TString& rcModel) {
    const size_t threads = fdp.ConsumeIntegralInRange<size_t>(2, MaxThreads);
    const auto ropeScripts = MakeThreadScripts(fdp, threads, ropeModel.size());
    const auto rcScripts = MakeThreadScripts(fdp, threads, rcModel.size());

    RunThreads(threads, [&](size_t thread) {
        TRope localRope = rope;
        TRcBuf localRcBuf = rcBuf;

        CheckRope(localRope, ropeModel);
        CheckRcBuf(localRcBuf, rcModel);

        for (const auto& step : ropeScripts[thread]) {
            switch (step.Kind % 5) {
                case 0: {
                    TRope copy = localRope;
                    CheckRope(copy, ropeModel);
                    break;
                }
                case 1: {
                    TRope range(localRope.Position(step.Begin), localRope.Position(step.End));
                    CheckRope(range, ropeModel.substr(step.Begin, step.End - step.Begin));
                    break;
                }
                case 2:
                    Y_ABORT_UNLESS(localRope.ConvertToString() == ropeModel);
                    break;
                case 3:
                    if (!HasZeroContiguousBlock(localRope)) {
                        Y_ABORT_UNLESS(TRope::Compare(localRope, TContiguousSpan(ropeModel)) == 0);
                    }
                    break;
                default: {
                    TRope mutableCopy = localRope;
                    TString mutableModel = ropeModel;
                    mutableCopy.Erase(mutableCopy.Position(step.Begin), mutableCopy.Position(step.End));
                    mutableModel.erase(step.Begin, step.End - step.Begin);
                    CheckRope(mutableCopy, mutableModel);
                    CheckRope(localRope, ropeModel);
                    break;
                }
            }
        }

        for (const auto& step : rcScripts[thread]) {
            switch (step.Kind % 4) {
                case 0: {
                    TRcBuf copy = localRcBuf;
                    CheckRcBuf(copy, rcModel);
                    break;
                }
                case 1:
                    Y_ABORT_UNLESS(TString(localRcBuf.Slice()) == rcModel);
                    break;
                case 2:
                    if (step.Begin < step.End) {
                        TRcBuf piece(TRcBuf::Piece, localRcBuf.data() + step.Begin, step.End - step.Begin, localRcBuf);
                        CheckRcBuf(piece, rcModel.substr(step.Begin, step.End - step.Begin));
                    }
                    break;
                default:
                    if (rcModel) {
                        const size_t pos = step.Begin == rcModel.size() ? rcModel.size() - 1 : step.Begin;
                        Y_ABORT_UNLESS(localRcBuf[pos] == rcModel[pos]);
                    }
                    break;
            }
        }
    });
}

void RunRcBufOperation(FuzzedDataProvider& fdp, TRcBuf& buf, TString& model) {
    switch (fdp.ConsumeIntegralInRange<int>(0, 9)) {
        case 0: {
            auto made = MakeRcBufWithOptionalSlice(fdp, MaxPieceSize);
            buf = std::move(made.first);
            model = std::move(made.second);
            break;
        }
        case 1: {
            if (!buf.HasBuffer() || !model) {
                break;
            }
            const size_t head = ConsumeRoom(fdp);
            buf.ReserveHeadroom(head);
            CheckRcBuf(buf, model);
            Y_ABORT_UNLESS(buf.Headroom() >= std::min(head, buf.UnsafeHeadroom()));
            break;
        }
        case 2: {
            if (!buf.HasBuffer() || !model) {
                break;
            }
            const size_t tail = ConsumeRoom(fdp);
            buf.ReserveTailroom(tail);
            CheckRcBuf(buf, model);
            Y_ABORT_UNLESS(buf.Tailroom() >= std::min(tail, buf.UnsafeTailroom()));
            break;
        }
        case 3: {
            if (!buf.HasBuffer() || !model) {
                break;
            }
            const size_t head = ConsumeRoom(fdp);
            const size_t tail = ConsumeRoom(fdp);
            buf.ReserveBidi(head, tail);
            CheckRcBuf(buf, model);
            break;
        }
        case 4: {
            TString add = ConsumeBytes(fdp, std::min(MaxPieceSize, MaxStateSize - model.size()));
            if (!CanAppend(model.size(), add.size())) {
                break;
            }
            if (!add) {
                break;
            }
            if (!buf.HasBuffer() || !model) {
                buf = MakeRcBuf(fdp, add);
                model = add + model;
                break;
            }
            const size_t old = model.size();
            buf.GrowFront(add.size());
            if (add) {
                std::memcpy(buf.GetDataMut(), add.data(), add.size());
            }
            model = add + model;
            Y_ABORT_UNLESS(buf.size() == old + add.size());
            break;
        }
        case 5: {
            TString add = ConsumeBytes(fdp, std::min(MaxPieceSize, MaxStateSize - model.size()));
            if (!CanAppend(model.size(), add.size())) {
                break;
            }
            if (!add) {
                break;
            }
            if (!buf.HasBuffer() || !model) {
                buf = MakeRcBuf(fdp, add);
                model += add;
                break;
            }
            const size_t old = model.size();
            buf.GrowBack(add.size());
            if (add) {
                std::memcpy(buf.GetDataMut() + old, add.data(), add.size());
            }
            model += add;
            break;
        }
        case 6:
            if (model) {
                const size_t newSize = fdp.ConsumeIntegralInRange<size_t>(0, model.size());
                buf.TrimBack(newSize);
                model.resize(newSize);
            }
            break;
        case 7:
            if (model) {
                const size_t newSize = fdp.ConsumeIntegralInRange<size_t>(0, model.size());
                buf.TrimFront(newSize);
                model = model.substr(model.size() - newSize);
            }
            break;
        case 8:
            if (model) {
                const size_t pos = fdp.ConsumeIntegralInRange<size_t>(0, model.size() - 1);
                const size_t len = fdp.ConsumeIntegralInRange<size_t>(1, model.size() - pos);
                TRcBuf piece(TRcBuf::Piece, buf.data() + pos, len, buf);
                TString pieceModel = model.substr(pos, len);
                CheckRcBuf(piece, pieceModel);
                ExerciseRcBufCowNoFdp(piece, pieceModel, fdp.ConsumeIntegral<char>());
            }
            break;
        default:
            ExerciseRcBufCowNoFdp(buf, model, fdp.ConsumeIntegral<char>());
            break;
    }
}

void RunRopeOperation(FuzzedDataProvider& fdp, TRope& rope, TString& model, const TRcBuf& rcBuf, const TString& rcModel) {
    switch (fdp.ConsumeIntegralInRange<int>(0, 15)) {
        case 0: {
            TString bytes = ConsumeBytes(fdp, std::min(MaxPieceSize, MaxStateSize - model.size()));
            InsertRope(rope, model, model.size(), MakeRope(fdp, bytes), bytes);
            break;
        }
        case 1: {
            TString bytes = ConsumeBytes(fdp, std::min(MaxPieceSize, MaxStateSize - model.size()));
            const size_t pos = fdp.ConsumeIntegralInRange<size_t>(0, model.size());
            InsertRope(rope, model, pos, MakeRope(fdp, bytes), bytes);
            break;
        }
        case 2:
            if (rcModel && CanAppend(model.size(), rcModel.size())) {
                const size_t pos = fdp.ConsumeIntegralInRange<size_t>(0, model.size());
                InsertRope(rope, model, pos, TRope(rcBuf), rcModel);
            }
            break;
        case 3:
            if (model) {
                const size_t begin = fdp.ConsumeIntegralInRange<size_t>(0, model.size());
                const size_t end = fdp.ConsumeIntegralInRange<size_t>(begin, model.size());
                rope.Erase(rope.Position(begin), rope.Position(end));
                model.erase(begin, end - begin);
            }
            break;
        case 4:
            if (model) {
                const size_t begin = fdp.ConsumeIntegralInRange<size_t>(0, model.size());
                const size_t end = fdp.ConsumeIntegralInRange<size_t>(begin, model.size());
                TRope extracted = rope.Extract(rope.Position(begin), rope.Position(end));
                TString extractedModel = model.substr(begin, end - begin);
                model.erase(begin, end - begin);
                CheckRope(extracted, extractedModel);
                if (CanAppend(model.size(), extractedModel.size()) && fdp.ConsumeBool()) {
                    const size_t pos = fdp.ConsumeIntegralInRange<size_t>(0, model.size());
                    InsertRope(rope, model, pos, std::move(extracted), extractedModel);
                }
            }
            break;
        case 5:
            if (model) {
                const size_t len = fdp.ConsumeIntegralInRange<size_t>(0, model.size());
                rope.EraseFront(len);
                model.erase(0, len);
            }
            break;
        case 6:
            if (model) {
                const size_t len = fdp.ConsumeIntegralInRange<size_t>(0, model.size());
                rope.EraseBack(len);
                model.resize(model.size() - len);
            }
            break;
        case 7:
            if (model) {
                const size_t len = fdp.ConsumeIntegralInRange<size_t>(0, model.size());
                TRope out;
                rope.ExtractFront(len, &out);
                TString prefix = model.substr(0, len);
                model.erase(0, len);
                CheckRope(out, prefix);
            }
            break;
        case 8:
            if (model) {
                const size_t len = fdp.ConsumeIntegralInRange<size_t>(0, model.size());
                TString out = TString::Uninitialized(len);
                Y_ABORT_UNLESS(rope.ExtractFrontPlain(out.Detach(), len));
                Y_ABORT_UNLESS(out == model.substr(0, len));
                model.erase(0, len);
            } else {
                char byte = 0;
                Y_ABORT_UNLESS(!rope.ExtractFrontPlain(&byte, 1));
            }
            break;
        case 9: {
            const size_t begin = fdp.ConsumeIntegralInRange<size_t>(0, model.size());
            const size_t end = fdp.ConsumeIntegralInRange<size_t>(begin, model.size());
            TRope range(rope.Position(begin), rope.Position(end));
            CheckRope(range, model.substr(begin, end - begin));
            break;
        }
        case 10: {
            rope.Compact(ConsumeRoom(fdp), ConsumeRoom(fdp));
            CheckRope(rope, model);
            if (model) {
                TRcBuf compacted = static_cast<TRcBuf>(rope);
                CheckRcBuf(compacted, model);
            }
            break;
        }
        case 11:
            if (model) {
                auto span = rope.GetContiguousSpan();
                Y_ABORT_UNLESS(span == TContiguousSpan(model));
            }
            break;
        case 12:
            ExerciseRopeCow(rope, model, fdp.ConsumeIntegral<char>());
            break;
        case 13:
            ExerciseUnsafeTemporaries(model, fdp.ConsumeIntegral<char>());
            break;
        case 14: {
            TRope copy = rope;
            CheckRope(copy, model);
            TRope moved = std::move(copy);
            CheckRope(moved, model);
            CheckRope(copy, TString());
            break;
        }
        default:
            if (model) {
                char scratch[64];
                char* ptr = scratch;
                size_t remain = fdp.ConsumeIntegralInRange<size_t>(0, sizeof(scratch));
                const size_t requested = remain;
                TRope copy = rope;
                TString copyModel = model;
                const bool ok = copy.FetchFrontPlain(&ptr, &remain);
                const size_t consumed = std::min(requested, copyModel.size());
                Y_ABORT_UNLESS(static_cast<size_t>(ptr - scratch) == consumed);
                Y_ABORT_UNLESS(remain == requested - consumed);
                Y_ABORT_UNLESS(ok == (requested <= copyModel.size()));
                Y_ABORT_UNLESS(TStringBuf(scratch, consumed) == copyModel.substr(0, consumed));
                copyModel.erase(0, consumed);
                CheckRope(copy, copyModel);
            }
            break;
    }
}

void RunExtendedOperation(FuzzedDataProvider& fdp, TRopeArena& arena, TRope& rope, TString& ropeModel, const TRcBuf& rcBuf, const TString& rcModel) {
    switch (fdp.ConsumeIntegralInRange<int>(0, 5)) {
        case 0:
            ExerciseArena(fdp, arena, rope, ropeModel);
            break;
        case 1:
            ExerciseZeroCopyInput(fdp, rope, ropeModel);
            break;
        case 2:
            ExerciseSlideView(fdp, ropeModel);
            break;
        case 3:
            ExercisePageAlignedRcBuf(fdp);
            break;
        case 4:
            ExerciseZeroCopyInput(fdp, rcModel ? TRope(rcBuf) : TRope(TString()), rcModel);
            break;
        default:
            ExerciseThreadedReadOnly(fdp, rope, ropeModel, rcBuf, rcModel);
            break;
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    FuzzedDataProvider fdp(data, size);

    TRope rope;
    TString ropeModel;
    TRcBuf rcBuf;
    TString rcModel;
    TRopeArena arena = MakeArena(fdp);

    const size_t ops = fdp.ConsumeIntegralInRange<size_t>(0, MaxOps);
    for (size_t i = 0; i < ops && fdp.remaining_bytes(); ++i) {
        switch (fdp.ConsumeIntegralInRange<int>(0, 2)) {
            case 0:
                RunRopeOperation(fdp, rope, ropeModel, rcBuf, rcModel);
                break;
            case 1:
                RunRcBufOperation(fdp, rcBuf, rcModel);
                break;
            default:
                RunExtendedOperation(fdp, arena, rope, ropeModel, rcBuf, rcModel);
                break;
        }
        CheckRcBuf(rcBuf, rcModel);
        CheckRope(rope, ropeModel);
    }

    return 0;
}
