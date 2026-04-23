#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/event.h>
#include <ydb/library/actors/core/event_load.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/dnsresolver/dnsresolver.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/library/actors/interconnect/interconnect_common.h>
#include <ydb/library/actors/interconnect/interconnect_tcp_proxy.h>
#include <ydb/library/actors/interconnect/interconnect_tcp_server.h>
#include <ydb/library/actors/interconnect/poller/poller_actor.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/algorithm.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/printf.h>
#include <util/system/unaligned_mem.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <csignal>
#include <future>
#include <limits>

using namespace NActors;
using namespace NActors::NDnsResolver;

namespace {

enum EProbeEvents {
    EvFlatV2SectionProbe = EventSpaceBegin(TEvents::ES_PRIVATE),
};

constexpr ui32 Node1 = 1;
constexpr ui32 Node2 = 2;
constexpr size_t DefaultChunkBytes = 8 * 1024;
constexpr ui8 ProbeVersion = 1;
constexpr ui32 ProbeMarker = 0xABCD1234u;

TActorId MakeProbeServiceId(ui32 nodeId) {
    return TActorId(nodeId, TStringBuf("flatv2pr"));
}

template <class T>
void AppendPod(TString& out, T value) {
    char buf[sizeof(T)];
    WriteUnaligned<T>(buf, value);
    out.append(buf, sizeof(buf));
}

template <class T>
T ReadPod(const char*& ptr, const char* end) {
    Y_ABORT_UNLESS(end - ptr >= static_cast<ptrdiff_t>(sizeof(T)));
    const T value = ReadUnaligned<T>(ptr);
    ptr += sizeof(T);
    return value;
}

struct TProbeConfig {
    ui32 Threads = 2;
    ui16 PortBase = 19600;
    size_t ExternalBytesSize = 6000;
    size_t ArrayElements = 5000;
    size_t ChunkBytes = DefaultChunkBytes;
    TString InlineBytes = "mini-bytes";
    TVector<ui32> InlineArray = {7, 11, 13, 17};
};

struct TProbeResult {
    ui8 Version = 0;
    ui32 Marker = 0;
    bool UsedSectionMetadata = false;
    TString InlineBytes;
    TVector<ui32> InlineArray;
    ui16 ExternalBytesPayloadId = 0;
    size_t ExternalBytesSize = 0;
    ui16 ArrayFirstSectionId = 0;
    ui16 ArraySectionCount = 0;
    ui32 ArrayElementCount = 0;
    TVector<size_t> ArraySectionSizes;
    TVector<bool> ArraySectionAligned;
    ui64 ArrayChecksum = 0;
    TEventSerializationInfo SerializationInfo;

    template <class T>
    static TString FormatVector(const TVector<T>& values) {
        TStringBuilder out;
        out << '[';
        for (size_t i = 0; i < values.size(); ++i) {
            if (i) {
                out << ", ";
            }
            out << values[i];
        }
        out << ']';
        return out;
    }

    static TString FormatBoolVector(const TVector<bool>& values) {
        TStringBuilder out;
        out << '[';
        for (size_t i = 0; i < values.size(); ++i) {
            if (i) {
                out << ", ";
            }
            out << (values[i] ? "true" : "false");
        }
        out << ']';
        return out;
    }

    TString Format() const {
        TStringBuilder out;
        out << "version=" << static_cast<ui32>(Version)
            << " marker=0x" << Sprintf("%08x", Marker)
            << " used_sections=" << (UsedSectionMetadata ? "true" : "false")
            << " inline_bytes=\"" << InlineBytes << "\""
            << " inline_array=" << FormatVector(InlineArray)
            << " payload_id=" << ExternalBytesPayloadId
            << " external_bytes_size=" << ExternalBytesSize
            << " array_first_section_id=" << ArrayFirstSectionId
            << " array_section_count=" << ArraySectionCount
            << " array_element_count=" << ArrayElementCount
            << " array_section_sizes=" << FormatVector(ArraySectionSizes)
            << " array_section_aligned=" << FormatBoolVector(ArraySectionAligned)
            << " array_checksum=" << ArrayChecksum
            << " sections=" << SerializeEventSections(SerializationInfo);
        return out;
    }
};

class TChunkedArrayWriter {
public:
    explicit TChunkedArrayWriter(size_t chunkBytes)
        : ChunkBytes(chunkBytes)
    {
        Y_ABORT_UNLESS(ChunkBytes >= sizeof(ui32));
        Y_ABORT_UNLESS(ChunkBytes % sizeof(ui32) == 0);
        ElementsPerChunk = ChunkBytes / sizeof(ui32);
    }

    void Init(size_t totalElements) {
        Buffers.clear();
        Sizes.clear();

        size_t remaining = totalElements;
        while (remaining) {
            const size_t elems = Min(remaining, ElementsPerChunk);
            const size_t bytes = elems * sizeof(ui32);
            TRcBuf buffer = TRcBuf(TRopeAlignedBuffer::Allocate(bytes));
            std::memset(buffer.GetDataMut(), 0, bytes);
            Buffers.push_back(std::move(buffer));
            Sizes.push_back(bytes);
            remaining -= elems;
        }
    }

    void Set(size_t index, ui32 value) {
        const size_t chunkIndex = index / ElementsPerChunk;
        const size_t offset = index % ElementsPerChunk;
        Y_ABORT_UNLESS(chunkIndex < Buffers.size());
        Y_ABORT_UNLESS((offset + 1) * sizeof(ui32) <= Sizes[chunkIndex]);
        WriteUnaligned<ui32>(Buffers[chunkIndex].GetDataMut() + offset * sizeof(ui32), value);
    }

    TVector<TRope> BuildRopes() const {
        TVector<TRope> ropes;
        ropes.reserve(Buffers.size());
        for (size_t i = 0; i < Buffers.size(); ++i) {
            ropes.emplace_back(Buffers[i]);
        }
        return ropes;
    }

    const TVector<size_t>& GetChunkSizes() const {
        return Sizes;
    }

private:
    const size_t ChunkBytes;
    size_t ElementsPerChunk = 0;
    TVector<TRcBuf> Buffers;
    TVector<size_t> Sizes;
};

class TEvFlatV2SectionProbe : public TEventBase<TEvFlatV2SectionProbe, EvFlatV2SectionProbe> {
public:
    struct TSectionRefs {
        ui16 ExternalBytesPayloadId = 0;
        ui32 ExternalBytesSize = 0;
        ui16 ArrayFirstSectionId = 0;
        ui16 ArraySectionCount = 0;
        ui32 ArrayElementCount = 0;
    };

    struct TParsedHeader {
        ui8 Version = 0;
        TSectionRefs Refs;
        TVector<ui32> ArraySectionSizes;
        ui32 Marker = 0;
        TString InlineBytes;
        TVector<ui32> InlineArray;
        size_t HeaderSize = 0;
    };

    static TEvFlatV2SectionProbe* MakeSample(const TProbeConfig& config) {
        THolder<TEvFlatV2SectionProbe> ev(new TEvFlatV2SectionProbe());
        ev->InlineBytes = config.InlineBytes;
        ev->InlineArray = config.InlineArray;
        ev->ExternalBytes = TRope(TString(config.ExternalBytesSize, 'P'));

        TChunkedArrayWriter writer(config.ChunkBytes);
        writer.Init(config.ArrayElements);
        for (size_t i = 0; i < config.ArrayElements; ++i) {
            writer.Set(i, static_cast<ui32>(1000 + i));
        }
        ev->ArraySections = writer.BuildRopes();
        ev->ArraySectionSizes = writer.GetChunkSizes();
        ev->Header = ev->BuildHeader();
        return ev.Release();
    }

    static TEvFlatV2SectionProbe* Load(const TEventSerializedData* input) {
        THolder<TEvFlatV2SectionProbe> ev(new TEvFlatV2SectionProbe());
        Y_ABORT_UNLESS(input);

        const auto& info = input->GetSerializationInfo();
        ev->SerializationInfo = info;
        ev->UsedSectionMetadata = !info.Sections.empty();

        if (!info.Sections.empty()) {
            TRope::TConstIterator iter = input->GetBeginIter();
            for (size_t i = 0; i < info.Sections.size(); ++i) {
                const auto& section = info.Sections[i];
                TRope::TConstIterator begin = iter;
                iter += section.Size;
                TRope rope(begin, iter);
                if (i == 0) {
                    ev->Header = rope.ExtractUnderlyingContainerOrCopy<TString>();
                } else if (i == 1) {
                    ev->ExternalBytes = std::move(rope);
                } else {
                    ev->ArraySections.push_back(std::move(rope));
                }
            }
        } else {
            const TString data = input->GetString();
            const TParsedHeader header = ParseHeader(data);
            ev->Header.assign(data.data(), header.HeaderSize);

            size_t offset = header.HeaderSize;
            ev->ExternalBytes = TRope(TString(data.data() + offset, header.Refs.ExternalBytesSize));
            offset += header.Refs.ExternalBytesSize;

            const size_t sectionCount = header.Refs.ArraySectionCount;
            Y_ABORT_UNLESS(header.ArraySectionSizes.size() == sectionCount);
            for (size_t i = 0; i < sectionCount; ++i) {
                const size_t chunkSize = header.ArraySectionSizes[i];
                ev->ArraySections.push_back(TRope(TString(data.data() + offset, chunkSize)));
                offset += chunkSize;
            }
        }

        const TParsedHeader parsed = ParseHeader(ev->Header);
        ev->Version = parsed.Version;
        ev->ExternalBytesPayloadId = parsed.Refs.ExternalBytesPayloadId;
        ev->ArrayFirstSectionId = parsed.Refs.ArrayFirstSectionId;
        ev->ArraySectionCount = parsed.Refs.ArraySectionCount;
        ev->ArrayElementCount = parsed.Refs.ArrayElementCount;
        ev->ArraySectionSizes.reserve(parsed.ArraySectionSizes.size());
        for (ui32 size : parsed.ArraySectionSizes) {
            ev->ArraySectionSizes.push_back(size);
        }
        ev->Marker = parsed.Marker;
        ev->InlineBytes = parsed.InlineBytes;
        ev->InlineArray = parsed.InlineArray;

        return ev.Release();
    }

    TString ToStringHeader() const override {
        return "TEvFlatV2SectionProbe";
    }

    TString ToString() const override {
        return CollectResult().Format();
    }

    bool SerializeToArcadiaStream(TChunkSerializer* serializer) const override {
        if (!serializer->WriteString(&Header)) {
            return false;
        }
        if (!serializer->WriteRope(&ExternalBytes)) {
            return false;
        }
        for (const TRope& rope : ArraySections) {
            if (!serializer->WriteRope(&rope)) {
                return false;
            }
        }
        return true;
    }

    std::optional<TRope> SerializeToRope(IRcBufAllocator* allocator) const override {
        TRope result;

        auto addString = [&](const TString& str) -> bool {
            if (!str) {
                return true;
            }
            TRcBuf buf = allocator->AllocRcBuf(str.size(), 0, 0);
            if (!buf) {
                return false;
            }
            std::memcpy(buf.GetDataMut(), str.data(), str.size());
            result.Insert(result.End(), TRope(std::move(buf)));
            return true;
        };

        if (!addString(Header)) {
            return {};
        }
        result.Insert(result.End(), TRope(ExternalBytes));
        for (const TRope& rope : ArraySections) {
            result.Insert(result.End(), TRope(rope));
        }
        return result;
    }

    bool IsSerializable() const override {
        return true;
    }

    ui32 CalculateSerializedSize() const override {
        ui32 size = Header.size() + ExternalBytes.GetSize();
        for (const TRope& rope : ArraySections) {
            size += rope.GetSize();
        }
        return size;
    }

    TEventSerializationInfo CreateSerializationInfo(bool allowExternalDataChannel) const override {
        TEventSerializationInfo info;
        info.IsExtendedFormat = allowExternalDataChannel;
        if (!allowExternalDataChannel) {
            return info;
        }

        info.Sections.push_back(TEventSectionInfo{0, Header.size(), 0, 1, true, false});
        info.Sections.push_back(TEventSectionInfo{0, ExternalBytes.GetSize(), 0, 1, false, false});
        for (const TRope& rope : ArraySections) {
            info.Sections.push_back(TEventSectionInfo{0, rope.GetSize(), 0, alignof(ui32), false, false});
        }
        return info;
    }

    TProbeResult CollectResult() const {
        TProbeResult result;
        result.Version = Version;
        result.Marker = Marker;
        result.UsedSectionMetadata = UsedSectionMetadata;
        result.InlineBytes = InlineBytes;
        result.InlineArray = InlineArray;
        result.ExternalBytesPayloadId = ExternalBytesPayloadId;
        result.ExternalBytesSize = ExternalBytes.GetSize();
        result.ArrayFirstSectionId = ArrayFirstSectionId;
        result.ArraySectionCount = ArraySections.size();
        result.ArrayElementCount = GetArrayElementCount();
        result.SerializationInfo = SerializationInfo;

        for (const TRope& rope : ArraySections) {
            result.ArraySectionSizes.push_back(rope.GetSize());

            const auto it = rope.Begin();
            const bool aligned = it.Valid()
                && it.ContiguousSize() >= rope.GetSize()
                && reinterpret_cast<uintptr_t>(it.ContiguousData()) % alignof(ui32) == 0;
            result.ArraySectionAligned.push_back(aligned);

            for (auto iter = rope.Begin(); iter.Valid(); iter.AdvanceToNextContiguousBlock()) {
                const char* data = iter.ContiguousData();
                const size_t size = iter.ContiguousSize();
                for (size_t offset = 0; offset + sizeof(ui32) <= size; offset += sizeof(ui32)) {
                    result.ArrayChecksum += ReadUnaligned<ui32>(data + offset);
                }
            }
        }

        return result;
    }

    static ui64 ExpectedArrayChecksum(size_t elements) {
        ui64 checksum = 0;
        for (size_t i = 0; i < elements; ++i) {
            checksum += static_cast<ui32>(1000 + i);
        }
        return checksum;
    }

private:
    static TParsedHeader ParseHeader(TStringBuf data) {
        TParsedHeader header;
        const char* ptr = data.data();
        const char* const end = ptr + data.size();

        Y_ABORT_UNLESS(ptr != end);
        header.Version = static_cast<ui8>(*ptr++);

        header.Refs.ExternalBytesPayloadId = ReadPod<ui16>(ptr, end);
        header.Refs.ExternalBytesSize = ReadPod<ui32>(ptr, end);
        header.Refs.ArrayFirstSectionId = ReadPod<ui16>(ptr, end);
        header.Refs.ArraySectionCount = ReadPod<ui16>(ptr, end);
        header.Refs.ArrayElementCount = ReadPod<ui32>(ptr, end);
        header.ArraySectionSizes.reserve(header.Refs.ArraySectionCount);
        for (size_t i = 0; i < header.Refs.ArraySectionCount; ++i) {
            header.ArraySectionSizes.push_back(ReadPod<ui32>(ptr, end));
        }

        header.Marker = ReadPod<ui32>(ptr, end);

        Y_ABORT_UNLESS(ptr != end);
        const bool hasInlineBytes = *ptr++;
        Y_ABORT_UNLESS(hasInlineBytes);
        const size_t inlineBytesSize = ReadPod<ui16>(ptr, end);
        Y_ABORT_UNLESS(end - ptr >= static_cast<ptrdiff_t>(inlineBytesSize));
        header.InlineBytes.assign(ptr, inlineBytesSize);
        ptr += inlineBytesSize;

        Y_ABORT_UNLESS(ptr != end);
        const bool hasInlineArray = *ptr++;
        Y_ABORT_UNLESS(hasInlineArray);
        const size_t inlineArraySize = ReadPod<ui16>(ptr, end);
        Y_ABORT_UNLESS(end - ptr >= static_cast<ptrdiff_t>(inlineArraySize * sizeof(ui32)));
        header.InlineArray.reserve(inlineArraySize);
        for (size_t i = 0; i < inlineArraySize; ++i) {
            header.InlineArray.push_back(ReadPod<ui32>(ptr, end));
        }

        header.HeaderSize = ptr - data.data();
        return header;
    }

    TString BuildHeader() const {
        TString header;
        header.push_back(static_cast<char>(ProbeVersion));
        AppendPod<ui16>(header, 1); // bytes payload id
        AppendPod<ui32>(header, ExternalBytes.GetSize());
        AppendPod<ui16>(header, 2); // first array section id
        Y_ABORT_UNLESS(ArraySections.size() <= std::numeric_limits<ui16>::max());
        AppendPod<ui16>(header, static_cast<ui16>(ArraySections.size()));
        Y_ABORT_UNLESS(GetArrayElementCount() <= std::numeric_limits<ui32>::max());
        AppendPod<ui32>(header, static_cast<ui32>(GetArrayElementCount()));
        for (const TRope& rope : ArraySections) {
            Y_ABORT_UNLESS(rope.GetSize() <= std::numeric_limits<ui32>::max());
            AppendPod<ui32>(header, static_cast<ui32>(rope.GetSize()));
        }
        AppendPod<ui32>(header, ProbeMarker);

        header.push_back(1); // inline bytes flag
        Y_ABORT_UNLESS(InlineBytes.size() <= std::numeric_limits<ui16>::max());
        AppendPod<ui16>(header, static_cast<ui16>(InlineBytes.size()));
        header.append(InlineBytes);

        header.push_back(1); // inline array flag
        Y_ABORT_UNLESS(InlineArray.size() <= std::numeric_limits<ui16>::max());
        AppendPod<ui16>(header, static_cast<ui16>(InlineArray.size()));
        for (ui32 value : InlineArray) {
            AppendPod<ui32>(header, value);
        }

        return header;
    }

    ui64 GetArrayElementCount() const {
        ui64 result = 0;
        for (const TRope& rope : ArraySections) {
            Y_ABORT_UNLESS(rope.GetSize() % sizeof(ui32) == 0);
            result += rope.GetSize() / sizeof(ui32);
        }
        return result;
    }

private:
    TString Header;
    TRope ExternalBytes;
    TVector<TRope> ArraySections;
    TVector<size_t> ArraySectionSizes;
    TEventSerializationInfo SerializationInfo;
    bool UsedSectionMetadata = false;

    ui8 Version = ProbeVersion;
    ui32 Marker = ProbeMarker;
    TString InlineBytes;
    TVector<ui32> InlineArray;
    ui16 ExternalBytesPayloadId = 1;
    ui16 ArrayFirstSectionId = 2;
    ui16 ArraySectionCount = 0;
    ui32 ArrayElementCount = 0;
};

class TSendOnceActor : public TActorBootstrapped<TSendOnceActor> {
public:
    TSendOnceActor(TActorId recipient, IEventBase* event)
        : Recipient(recipient)
        , Event(event)
    {}

    void Bootstrap() {
        Become(&TSendOnceActor::StateWork);
        Send(TActivationContext::InterconnectProxy(Recipient.NodeId()), new TEvInterconnect::TEvConnectNode, IEventHandle::FlagTrackDelivery);
    }

    void HandleConnected(TEvInterconnect::TEvNodeConnected::TPtr& ev) {
        if (ev->Get()->NodeId == Recipient.NodeId()) {
            Send(Recipient, Event.Release(), IEventHandle::FlagTrackDelivery);
            PassAway();
        }
    }

    void HandleDisconnected(TEvInterconnect::TEvNodeDisconnected::TPtr&) {
        PassAway();
    }

    void HandleUndelivered(TEvents::TEvUndelivered::TPtr&) {
        PassAway();
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvInterconnect::TEvNodeConnected, HandleConnected);
            hFunc(TEvInterconnect::TEvNodeDisconnected, HandleDisconnected);
            hFunc(TEvents::TEvUndelivered, HandleUndelivered);
        }
    }

private:
    const TActorId Recipient;
    THolder<IEventBase> Event;
};

class TReceiverActor : public TActorBootstrapped<TReceiverActor> {
public:
    explicit TReceiverActor(std::promise<TProbeResult> promise)
        : Promise(std::move(promise))
    {}

    void Bootstrap() {
        Become(&TReceiverActor::StateWork);
    }

    void Handle(TEvFlatV2SectionProbe::TPtr& ev) {
        Promise.set_value(ev->Get()->CollectResult());
        PassAway();
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvFlatV2SectionProbe, Handle);
        }
    }

private:
    std::promise<TProbeResult> Promise;
};

THolder<TActorSystemSetup> BuildInterconnectActorSystemSetup(
        ui32 nodeId,
        ui32 threads,
        ui16 portBase,
        NMonitoring::TDynamicCounters& counters)
{
    auto setup = MakeHolder<TActorSystemSetup>();

    setup->NodeId = nodeId;
    setup->ExecutorsCount = 1;
    setup->Executors.Reset(new TAutoPtr<IExecutorPool>[1]);
    setup->Executors[0] = new TBasicExecutorPool(0, threads, 50);
    setup->Scheduler = new TBasicSchedulerThread(TSchedulerConfig(512, 0));

    setup->LocalServices.emplace_back(MakePollerActorId(), TActorSetupCmd(CreatePollerActor(), TMailboxType::ReadAsFilled, 0));
    setup->LocalServices.emplace_back(MakeDnsResolverActorId(), TActorSetupCmd(CreateOnDemandDnsResolver(), TMailboxType::ReadAsFilled, 0));

    TIntrusivePtr<TTableNameserverSetup> nameserverTable = new TTableNameserverSetup();
    nameserverTable->StaticNodeTable[Node1] = std::make_pair("127.0.0.1", portBase + Node1);
    nameserverTable->StaticNodeTable[Node2] = std::make_pair("127.0.0.1", portBase + Node2);
    setup->LocalServices.emplace_back(GetNameserviceActorId(), TActorSetupCmd(CreateNameserverTable(nameserverTable), TMailboxType::ReadAsFilled, 0));

    TIntrusivePtr<TInterconnectProxyCommon> icCommon = new TInterconnectProxyCommon();
    icCommon->NameserviceId = GetNameserviceActorId();
    icCommon->MonCounters = counters.GetSubgroup("counters", "interconnect");
    icCommon->TechnicalSelfHostName = "127.0.0.1";

    setup->Interconnect.ProxyActors.resize(Node2 + 1);
    for (ui32 peerNodeId : {Node1, Node2}) {
        if (peerNodeId == nodeId) {
            IActor* listener = new TInterconnectListenerTCP("127.0.0.1", portBase + nodeId, icCommon);
            setup->LocalServices.emplace_back(
                MakeInterconnectListenerActorId(false),
                TActorSetupCmd(listener, TMailboxType::ReadAsFilled, 0));
        } else {
            setup->Interconnect.ProxyActors[peerNodeId] = TActorSetupCmd(
                new TInterconnectProxyTCP(peerNodeId, icCommon),
                TMailboxType::ReadAsFilled,
                0);
        }
    }

    return setup;
}

bool ParseArgs(int argc, char** argv, TProbeConfig& config) {
    for (int i = 1; i < argc; ++i) {
        TStringBuf arg(argv[i]);
        const size_t pos = arg.find('=');
        const TStringBuf key = pos == TStringBuf::npos ? arg : arg.Head(pos);
        const TStringBuf value = pos == TStringBuf::npos ? TStringBuf() : arg.Skip(pos + 1);

        if (key == "--threads" && value) {
            config.Threads = FromString<ui32>(value);
        } else if (key == "--port-base" && value) {
            config.PortBase = FromString<ui16>(value);
        } else if (key == "--bytes-size" && value) {
            config.ExternalBytesSize = FromString<size_t>(value);
        } else if (key == "--array-elements" && value) {
            config.ArrayElements = FromString<size_t>(value);
        } else if (key == "--chunk-bytes" && value) {
            config.ChunkBytes = FromString<size_t>(value);
        } else if (key == "--inline-bytes" && value) {
            config.InlineBytes = TString(value);
        } else if (key == "--help") {
            return false;
        } else {
            Cerr << "Unknown argument: " << arg << Endl;
            return false;
        }
    }

    Y_ABORT_UNLESS(config.Threads > 0);
    Y_ABORT_UNLESS(config.ChunkBytes >= sizeof(ui32));
    Y_ABORT_UNLESS(config.ChunkBytes % sizeof(ui32) == 0);
    return true;
}

void PrintUsage(const char* argv0) {
    Cout
        << "Usage: " << argv0
        << " [--threads=N] [--port-base=N] [--bytes-size=N] [--array-elements=N] [--chunk-bytes=N] [--inline-bytes=TEXT]"
        << Endl;
}

TProbeResult RunProbe(const TProbeConfig& config) {
    TIntrusivePtr<NMonitoring::TDynamicCounters> counters1 = new NMonitoring::TDynamicCounters();
    TIntrusivePtr<NMonitoring::TDynamicCounters> counters2 = new NMonitoring::TDynamicCounters();

    THolder<TActorSystemSetup> setup2 = BuildInterconnectActorSystemSetup(Node2, config.Threads, config.PortBase, *counters2);
    THolder<TActorSystemSetup> setup1 = BuildInterconnectActorSystemSetup(Node1, config.Threads, config.PortBase, *counters1);
    TActorSystem node2(setup2);
    TActorSystem node1(setup1);

    node2.Start();
    node1.Start();

    std::promise<TProbeResult> promise;
    std::future<TProbeResult> future = promise.get_future();
    const TActorId receiverId = node2.Register(new TReceiverActor(std::move(promise)));
    node2.RegisterLocalService(MakeProbeServiceId(Node2), receiverId);

    node1.Register(new TSendOnceActor(MakeProbeServiceId(Node2), TEvFlatV2SectionProbe::MakeSample(config)));

    const auto status = future.wait_for(std::chrono::seconds(30));
    Y_ABORT_UNLESS(status == std::future_status::ready);
    TProbeResult result = future.get();

    node1.Stop();
    node2.Stop();
    node1.Cleanup();
    node2.Cleanup();

    return result;
}

void ValidateResult(const TProbeConfig& config, const TProbeResult& result) {
    Y_ABORT_UNLESS(result.Version == ProbeVersion);
    Y_ABORT_UNLESS(result.Marker == ProbeMarker);
    Y_ABORT_UNLESS(result.InlineBytes == config.InlineBytes);
    Y_ABORT_UNLESS(result.InlineArray == config.InlineArray);
    Y_ABORT_UNLESS(result.ExternalBytesPayloadId == 1);
    Y_ABORT_UNLESS(result.ExternalBytesSize == config.ExternalBytesSize);
    Y_ABORT_UNLESS(result.ArrayFirstSectionId == 2);
    Y_ABORT_UNLESS(!result.ArraySectionSizes.empty());
    Y_ABORT_UNLESS(result.ArraySectionCount == result.ArraySectionSizes.size());
    Y_ABORT_UNLESS(result.ArrayElementCount == config.ArrayElements);
    Y_ABORT_UNLESS(std::all_of(result.ArraySectionAligned.begin(), result.ArraySectionAligned.end(), [](bool value) { return value; }));
    Y_ABORT_UNLESS(result.ArrayChecksum == TEvFlatV2SectionProbe::ExpectedArrayChecksum(config.ArrayElements));
}

} // namespace

int main(int argc, char** argv) {
#ifdef _unix_
    signal(SIGPIPE, SIG_IGN);
#endif

    TProbeConfig config;
    if (!ParseArgs(argc, argv, config)) {
        PrintUsage(argv[0]);
        return 1;
    }

    const TProbeResult result = RunProbe(config);
    ValidateResult(config, result);

    Cout
        << "flat_v2_sections_probe_ok"
        << " " << result.Format()
        << Endl;

    return 0;
}
