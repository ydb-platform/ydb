#include "hnsw_index.h"
#include "hnsw/serialization.h"

#include <ydb/core/base/kmeans_clusters.h>
#include <ydb/library/yql/udfs/common/knn/knn-defines.h>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wsign-compare"
#pragma clang diagnostic ignored "-Wunused-parameter"
#pragma clang diagnostic ignored "-Wunused-but-set-variable"
#include <knnquery.h>
#include <knnqueue.h>
#include <method/hnsw.h>
#include <space/space_lp.h>
#include <space/space_scalar.h>
#pragma clang diagnostic pop

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/algorithm.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>

#include <tuple>
#include <cstring>
#include <istream>
#include <ostream>
#include <streambuf>

namespace NKikimr::NDataShard {

namespace {

using Ydb::Table::VectorIndexSettings;

// Rough fixed per-node overhead of the HNSW graph, excluding friend ids.
constexpr size_t EstimatedBytesPerNodeOverhead = 512;
// HNSW construction runs in a build actor registered in the actor system's
// batch pool. Keep NMSLIB single-threaded so it does not create unmanaged
// std::threads outside that pool.
constexpr unsigned BuildThreadsPerIndex = 1;
constexpr ui32 DefaultHnswM = 16;
constexpr ui32 DefaultHnswEfConstruction = 200;
constexpr ui64 DefaultHnswMinRows = 10000;
constexpr ui64 DefaultHnswDeltaRows = 10000;

std::unique_ptr<similarity::Space<float>> CreateSpace(VectorIndexSettings::Metric metric, TString& error) {
    switch (metric) {
        case VectorIndexSettings::DISTANCE_COSINE:
        case VectorIndexSettings::SIMILARITY_COSINE:
            return std::make_unique<similarity::SpaceCosineSimilarity<float>>();
        case VectorIndexSettings::DISTANCE_EUCLIDEAN:
            return std::make_unique<similarity::SpaceLp<float>>(2);
        case VectorIndexSettings::DISTANCE_MANHATTAN:
            return std::make_unique<similarity::SpaceLp<float>>(1);
        case VectorIndexSettings::SIMILARITY_INNER_PRODUCT:
            return std::make_unique<similarity::SpaceNegativeScalarProduct<float>>();
        default:
            error = TStringBuilder() << "Unsupported metric for HNSW: " << static_cast<int>(metric);
            return nullptr;
    }
}

// View of a wire-format float vector: raw float data plus its dimension.
// Returns an invalid view (Data == nullptr) if the bytes are not a
// well-formed FloatVector.
struct TFloatVectorView {
    const void* Data = nullptr;
    size_t Dimension = 0;

    static TFloatVectorView FromSerialized(TStringBuf serialized) {
        TFloatVectorView result;
        if (serialized.size() <= HeaderLen) {
            return result;
        }

        const ui8 formatByte = static_cast<ui8>(serialized.back());
        if (formatByte != EFormat::FloatVector) {
            return result;
        }

        const size_t dataSize = serialized.size() - HeaderLen;
        if (dataSize % sizeof(float) != 0 || dataSize == 0) {
            return result;
        }

        // similarity::Object copies raw bytes into its own aligned storage;
        // keep this as an untyped pointer because TStringBuf itself does not
        // guarantee float alignment.
        result.Data = serialized.data();
        result.Dimension = dataSize / sizeof(float);
        return result;
    }

    bool IsValid() const {
        return Data != nullptr && Dimension > 0;
    }
};

using NHnswSerialization::TSnapshotStreamBuf;

template <class T>
void WriteSnapshotWord(std::ostream& out, T value) {
    char bytes[sizeof(T)];
    for (size_t i = 0; i < sizeof(T); ++i)
        bytes[i] = static_cast<char>(value >> (8 * i));
    out.write(bytes, sizeof(bytes));
}

template <class T>
T ReadSnapshotWord(std::istream& in) {
    unsigned char bytes[sizeof(T)];
    if (!in.read(reinterpret_cast<char*>(bytes), sizeof(bytes)))
        ythrow yexception() << "Truncated HNSW snapshot";
    T result = 0;
    for (size_t i = 0; i < sizeof(T); ++i)
        result |= T(bytes[i]) << (8 * i);
    return result;
}

} // namespace

ui64 GetHnswMinRows(const VectorIndexSettings& settings) {
    return settings.has_min_rows() ? settings.min_rows() : DefaultHnswMinRows;
}

ui64 GetHnswDeltaRows(const VectorIndexSettings& settings) {
    return settings.has_delta_rows()
        ? settings.delta_rows() : DefaultHnswDeltaRows;
}

bool AreHnswIndexSettingsCompatible(
        const VectorIndexSettings& cached,
        const VectorIndexSettings& requested) {
    const auto connectivity = [](const auto& settings) {
        return settings.has_m()
            ? settings.m() : DefaultHnswM;
    };
    const auto constructionCandidates = [](const auto& settings) {
        return settings.has_ef_construction()
            ? settings.ef_construction() : DefaultHnswEfConstruction;
    };
    return cached.metric() == requested.metric()
        && cached.vector_type() == requested.vector_type()
        && cached.vector_dimension() == requested.vector_dimension()
        && connectivity(cached) == connectivity(requested)
        && constructionCandidates(cached) == constructionCandidates(requested);
}

void THnswIndexChanges::Set(TString key, TRowVersion version,
        std::optional<TString> vector, std::shared_ptr<void> reservation) {
    auto& history = Rows[key];
    for (auto& [base, count] : ChangedRowCounts) {
        if (version > base && (history.empty() || history.back().Version <= base)) {
            ++count;
        }
    }
    auto it = std::lower_bound(history.begin(), history.end(), version,
        [](const TVersion& item, TRowVersion value) { return item.Version < value; });
    TVersion item{version, std::move(vector), std::move(reservation)};
    const size_t bytes = EstimateBytes(key, item.Vector ? item.Vector->size() : 0);
    if (it != history.end() && it->Version == version) {
        EstimatedBytes -= EstimateBytes(key, it->Vector ? it->Vector->size() : 0);
        *it = std::move(item);
    } else {
        history.insert(it, std::move(item));
        ++VersionCount;
    }
    EstimatedBytes += bytes;
}

const THnswIndexChanges::TVersion* THnswIndexChanges::Find(
        const THistory& history, TRowVersion version, TRowVersion base) {
    const auto it = std::upper_bound(history.begin(), history.end(), version,
        [](TRowVersion value, const TVersion& item) { return value < item.Version; });
    if (it == history.begin() || std::prev(it)->Version <= base) {
        return nullptr;
    }
    return &*std::prev(it);
}

const THnswIndexChanges::TVersion* THnswIndexChanges::Find(
        TStringBuf key, TRowVersion version, TRowVersion base) const {
    const auto it = Rows.find(key);
    return it == Rows.end() ? nullptr : Find(it->second, version, base);
}

size_t THnswIndexChanges::CountAfter(TRowVersion base) const {
    if (auto it = ChangedRowCounts.find(base); it != ChangedRowCounts.end()) {
        return it->second;
    }
    size_t count = 0;
    for (const auto& [_, history] : Rows) {
        count += !history.empty() && history.back().Version > base;
    }
    ChangedRowCounts.emplace(base, count);
    return count;
}

void THnswIndexChanges::PruneThrough(TRowVersion base) {
    ChangedRowCounts.erase(ChangedRowCounts.begin(), ChangedRowCounts.lower_bound(base));
    for (auto it = Rows.begin(); it != Rows.end();) {
        auto& history = it->second;
        auto end = std::upper_bound(history.begin(), history.end(), base,
            [](TRowVersion value, const TVersion& item) { return value < item.Version; });
        if (end == history.begin()) {
            ++it;
            continue;
        }
        VersionCount -= end - history.begin();
        for (auto removed = history.begin(); removed != end; ++removed) {
            EstimatedBytes -= EstimateBytes(it->first, removed->Vector ? removed->Vector->size() : 0);
        }
        history.erase(history.begin(), end);
        if (history.empty()) {
            Rows.erase(it++);
        } else {
            // Release capacity as well as payload/reservations after pruning.
            THistory(history).swap(history);
            ++it;
        }
    }
}

size_t THnswIndexChanges::EstimateBytes(TStringBuf key, size_t vectorBytes) {
    return 2 * key.size() + vectorBytes + 2 * sizeof(TVersion) + 128;
}

class THnswIndex::TGraph {
public:
    TGraph(std::unique_ptr<similarity::Space<float>> space, size_t dimension)
        : Space(std::move(space))
        , Dimension(dimension)
    {}

    ~TGraph() {
        // Hnsw keeps pointers to Objects, so destroy it before their owners.
        Index.reset();
    }

    void AddVector(TString key, const void* data) {
        const auto id = static_cast<similarity::IdType>(Keys.size());
        auto obj = std::make_unique<similarity::Object>(
            id, /* label */ -1, Dimension * sizeof(float), data);
        KeyBytes += key.size();
        Keys.push_back(std::move(key));
        KeyToIndex.emplace(Keys.back(), static_cast<size_t>(id));
        Objects.push_back(obj.get());
        OwnedObjects.push_back(std::move(obj));
    }

    bool Build(const VectorIndexSettings& settings) {
        Settings = settings;
        Settings.set_vector_dimension(Dimension);
        Connectivity = settings.has_m() ? settings.m() : DefaultHnswM;
        if (Objects.empty()) {
            return true; // Empty rebuilt generations still have a searchable journal.
        }

        Index = std::make_unique<similarity::Hnsw<float>>(/* PrintProgress */ false, *Space, Objects);

        similarity::AnyParams buildParams(std::vector<std::string>{
            "M=" + std::to_string(Connectivity),
            "efConstruction=" + std::to_string(settings.has_ef_construction()
                ? settings.ef_construction() : DefaultHnswEfConstruction),
            "indexThreadQty=" + std::to_string(BuildThreadsPerIndex),
        });
        Index->CreateIndex(buildParams);
        for (size_t i = 0; i < Objects.size(); ++i) {
            if (Objects[i]->id() != static_cast<similarity::IdType>(i)) {
                Index.reset();
                return false;
            }
        }
        return true;
    }

    VectorIndexSettings Settings;
    std::unique_ptr<similarity::Space<float>> Space;
    size_t Dimension = 0;
    ui32 Connectivity = DefaultHnswM;
    size_t KeyBytes = 0;
    std::vector<const similarity::Object*> Objects;
    std::vector<std::unique_ptr<similarity::Object>> OwnedObjects;
    std::vector<TString> Keys; // Object::id() -> serialized primary key
    THashMap<TString, size_t> KeyToIndex;
    std::unique_ptr<similarity::Hnsw<float>> Index;
};

class THnswIndex::TImpl {
public:
    explicit TImpl(std::shared_ptr<const TGraph> graph)
        : Graph(std::move(graph))
    {}

    THnswSearchResult Search(TStringBuf targetVector, size_t k, TRowVersion readVersion, ui32 efSearch) const {
        THnswSearchResult result;
        if (!CanRead(readVersion)) {
            result.Covered = false;
            return result;
        }
        if (k == 0) {
            return result;
        }

        auto view = TFloatVectorView::FromSerialized(targetVector);
        if (!view.IsValid() || view.Dimension != Graph->Dimension) {
            return result;
        }

        std::unique_ptr<const similarity::Object> queryObj(
            new similarity::Object(-1, -1, Graph->Dimension * sizeof(float), view.Data));

        const size_t changes = ChangeCount();
        const size_t graphK = Min(k, Graph->Keys.size()) + Min(changes, Graph->Keys.size() - Min(k, Graph->Keys.size()));
        // Concurrent searches share graph storage, but each needs its own
        // visited bitmap, candidate queues and journal merge workspace.
        struct TSearchReservation {
            std::shared_ptr<THnswCacheMemoryTracker> Tracker;
            ui64 Bytes = 0;
            ~TSearchReservation() {
                if (Bytes)
                    Tracker->Release(Bytes);
            }
        } reservation{MemoryTracker};
        if (MemoryTracker) {
            const ui64 bytes = EstimatedSearchMemoryBytes(k, efSearch);
            if (!MemoryTracker->TryAcquire(bytes)) {
                result.Covered = false;
                return result;
            }
            reservation.Bytes = bytes;
        }
        similarity::KNNQuery<float> query(*Graph->Space, queryObj.get(), static_cast<unsigned>(Max<size_t>(graphK, 1)));
        if (Graph->Index) {
            Graph->Index->SearchWithEf(&query, efSearch);
        }

        const similarity::KNNQueue<float>* queue = query.Result();
        std::vector<std::pair<TString, float>> reversed;
        reversed.reserve(queue->Size());
        // KNNQueue is a max-heap over distance; popping yields farthest-first.
        // Clone it since Result() is owned by the query and Pop() is destructive.
        std::unique_ptr<similarity::KNNQueue<float>> mutableQueue(queue->Clone());
        while (!mutableQueue->Empty()) {
            const float distance = mutableQueue->TopDistance();
            const similarity::Object* obj = mutableQueue->Pop();
            const size_t idx = static_cast<size_t>(obj->id());
            if (idx < Graph->Keys.size()) {
                reversed.emplace_back(Graph->Keys[idx], distance);
            }
        }

        THashMap<TString, float> merged;
        for (auto it = reversed.rbegin(); it != reversed.rend(); ++it) {
            if (!Changes->Find(it->first, readVersion, BaseVersion)) {
                merged.emplace(it->first, it->second);
            }
        }

        for (const auto& [key, history] : Changes->Rows) {
            const auto* visible = THnswIndexChanges::Find(history, readVersion, BaseVersion);
            if (!visible || !visible->Vector) {
                continue;
            }
            auto deltaView = TFloatVectorView::FromSerialized(*visible->Vector);
            if (!deltaView.IsValid() || deltaView.Dimension != Graph->Dimension) {
                continue;
            }
            similarity::Object deltaObj(-1, -1, Graph->Dimension * sizeof(float), deltaView.Data);
            merged[key] = query.DistanceObjLeft(&deltaObj);
        }

        result.Results.assign(merged.begin(), merged.end());
        Sort(result.Results, [](const auto& lhs, const auto& rhs) {
            return std::tie(lhs.second, lhs.first) < std::tie(rhs.second, rhs.first);
        });
        if (result.Results.size() > k) {
            result.Results.resize(k);
        }
        return result;
    }

    ui64 EstimatedSearchMemoryBytes(size_t k, ui32 efSearch) const {
        const auto changes = ChangeCount();
        const auto graphK = Min(k, Graph->Keys.size())
            + Min(changes, Graph->Keys.size() - Min(k, Graph->Keys.size()));
        return Graph->Keys.size() + Graph->Dimension * 8 + 4096
            + (Min(Max<size_t>(efSearch, graphK), Graph->Keys.size()) + changes) * 256;
    }

    size_t Size() const {
        return Graph->Keys.size();
    }

    size_t Dim() const {
        return Graph->Dimension;
    }

    ui32 GetConnectivity() const {
        return Graph->Connectivity;
    }

    size_t GetKeyBytes() const {
        return Graph->KeyBytes;
    }

    bool GetVector(TStringBuf key, TString& result, TRowVersion readVersion) const {
        if (!CanRead(readVersion)) {
            return false;
        }
        if (const auto* visible = Changes->Find(key, readVersion, BaseVersion)) {
            if (!visible->Vector) {
                return false;
            }
            result = *visible->Vector;
            return true;
        }
        auto it = Graph->KeyToIndex.find(key);
        if (it == Graph->KeyToIndex.end()) {
            return false;
        }
        const auto* object = Graph->OwnedObjects[it->second].get();
        result.assign(object->data(), object->datalength());
        result.push_back(static_cast<char>(Format<float>));
        return true;
    }

    bool Upsert(TString key, TString vector, TRowVersion version) {
        auto view = TFloatVectorView::FromSerialized(vector);
        if (!view.IsValid() || view.Dimension != Graph->Dimension) {
            return false;
        }
        Changes->Set(std::move(key), version, std::move(vector));
        return true;
    }

    void Erase(TStringBuf key, TRowVersion version) {
        Changes->Set(TString(key), version, std::nullopt);
    }

    bool HasDelta(TStringBuf key) const {
        return Changes->Find(key, TRowVersion::Max(), BaseVersion) != nullptr;
    }

    size_t ChangeCount() const {
        return Changes->CountAfter(BaseVersion);
    }

    bool CanRead(TRowVersion version) const {
        return Changes->Valid && BaseVersion <= version && version <= UpperVersion;
    }

    TRowVersion BaseVersion = TRowVersion::Min();
    TRowVersion UpperVersion = TRowVersion::Max();
    std::shared_ptr<THnswIndexChanges> Changes = std::make_shared<THnswIndexChanges>();


    std::shared_ptr<void> GraphReservation;
    std::shared_ptr<THnswCacheMemoryTracker> MemoryTracker;
    std::shared_ptr<const TGraph> Graph;
};

THnswIndex::THnswIndex(std::unique_ptr<TImpl> impl)
    : Impl(std::move(impl))
{}

THnswIndex::~THnswIndex() = default;

std::unique_ptr<THnswIndex> THnswIndex::CreateView(TRowVersion base,
        std::shared_ptr<THnswIndexChanges> changes, TRowVersion upper) const {
    auto impl = std::make_unique<TImpl>(Impl->Graph);
    impl->GraphReservation = Impl->GraphReservation;
    impl->MemoryTracker = Impl->MemoryTracker;
    auto view = std::unique_ptr<THnswIndex>(new THnswIndex(std::move(impl)));
    view->SetSnapshot(base, std::move(changes), upper);
    return view;
}

bool THnswIndex::SharesGraphWith(const THnswIndex& other) const {
    return Impl->Graph == other.Impl->Graph;
}

bool THnswIndex::HasGraphReservation() const {
    return bool(Impl->GraphReservation);
}

size_t THnswIndex::GraphUseCount() const {
    return Impl->Graph.use_count();
}

void THnswIndex::SetGraphReservation(std::shared_ptr<void> reservation,
        std::shared_ptr<THnswCacheMemoryTracker> memoryTracker) {
    Y_ENSURE(!Impl->GraphReservation);
    Impl->GraphReservation = std::move(reservation);
    Impl->MemoryTracker = std::move(memoryTracker);
}

void THnswIndex::SaveGraph(std::ostream& output) const {
    TSnapshotStreamBuf buffer(output.rdbuf());
    std::ostream out(&buffer);
    out.exceptions(std::ios::badbit | std::ios::failbit);
    const auto& graph = *Impl->Graph;
    out.write("YHNSW001", 8);
    WriteSnapshotWord<ui32>(out, graph.Settings.metric());
    WriteSnapshotWord<ui32>(out, graph.Dimension);
    WriteSnapshotWord<ui32>(out, graph.Connectivity);
    WriteSnapshotWord<ui32>(out, graph.Settings.has_ef_construction()
        ? graph.Settings.ef_construction() : DefaultHnswEfConstruction);
    WriteSnapshotWord<ui64>(out, graph.Keys.size());
    for (size_t i = 0; i < graph.Keys.size(); ++i) {
        const auto& key = graph.Keys[i];
        Y_ENSURE(key.size() <= Max<ui32>());
        WriteSnapshotWord<ui32>(out, key.size());
        out.write(key.data(), key.size());
        const auto* data = graph.OwnedObjects[i]->data();
        for (size_t d = 0; d < graph.Dimension; ++d) {
            ui32 bits;
            std::memcpy(&bits, data + d * sizeof(float), sizeof(bits));
            WriteSnapshotWord<ui32>(out, bits);
        }
    }
    if (graph.Index)
        graph.Index->SaveTopology(out);
    WriteSnapshotWord<ui64>(output, buffer.Hash);
    if (!output)
        ythrow yexception() << "Cannot write HNSW snapshot";
}

std::unique_ptr<THnswIndex> THnswIndex::LoadGraph(std::istream& input,
        const VectorIndexSettings& settings, ui64 maxMemoryBytes, TString& error) {
    try {
        TString validationError;
        if (settings.vector_type() != VectorIndexSettings::VECTOR_TYPE_FLOAT
                || !NKMeans::ValidateSettingsPartial(settings, validationError)
                || !settings.vector_dimension()) {
            error = "Invalid HNSW snapshot settings: " + validationError;
            return nullptr;
        }
        TSnapshotStreamBuf buffer(input.rdbuf());
        std::istream in(&buffer);
        char magic[8];
        if (!in.read(magic, sizeof(magic)) || std::memcmp(magic, "YHNSW001", 8))
            ythrow yexception() << "Unsupported HNSW snapshot format";
        const auto metric = ReadSnapshotWord<ui32>(in);
        const auto dimension = ReadSnapshotWord<ui32>(in);
        const auto connectivity = ReadSnapshotWord<ui32>(in);
        const auto construction = ReadSnapshotWord<ui32>(in);
        const auto rows = ReadSnapshotWord<ui64>(in);
        if (metric != static_cast<ui32>(settings.metric()) || dimension != settings.vector_dimension()
                || connectivity != (settings.has_m() ? settings.m() : DefaultHnswM)
                || construction != (settings.has_ef_construction()
                    ? settings.ef_construction() : DefaultHnswEfConstruction))
            ythrow yexception() << "Incompatible HNSW snapshot settings";
        if (rows > Max<similarity::IdType>() || !maxMemoryBytes
                || EstimateMemoryBytes(rows, dimension, connectivity) > maxMemoryBytes)
            ythrow yexception() << "HNSW snapshot exceeds memory budget";
        auto space = CreateSpace(settings.metric(), error);
        if (!space)
            return nullptr;
        auto graph = std::make_shared<TGraph>(std::move(space), dimension);
        graph->Settings = settings;
        graph->Connectivity = connectivity;
        for (ui64 i = 0; i < rows; ++i) {
            const auto keyBytes = ReadSnapshotWord<ui32>(in);
            if (keyBytes > maxMemoryBytes || graph->KeyBytes > maxMemoryBytes - keyBytes
                    || EstimateMemoryBytes(rows, dimension, connectivity,
                        graph->KeyBytes + keyBytes) > maxMemoryBytes)
                ythrow yexception() << "HNSW keys exceed memory budget";
            TString key = TString::Uninitialized(keyBytes);
            if (!in.read(key.Detach(), keyBytes))
                ythrow yexception() << "Truncated HNSW key";
            if (graph->KeyToIndex.contains(key))
                ythrow yexception() << "Duplicate HNSW key";
            TString vector = TString::Uninitialized(dimension * sizeof(float));
            auto* data = vector.Detach();
            for (ui32 d = 0; d < dimension; ++d) {
                const ui32 bits = ReadSnapshotWord<ui32>(in);
                std::memcpy(data + d * sizeof(float), &bits, sizeof(bits));
            }
            graph->AddVector(std::move(key), data);
        }
        if (rows) {
            const ui64 inputs = rows * (ui64(dimension) * sizeof(float) + 128) + graph->KeyBytes;
            if (inputs >= maxMemoryBytes)
                ythrow yexception() << "HNSW snapshot exceeds memory budget";
            graph->Index = std::make_unique<similarity::Hnsw<float>>(false, *graph->Space, graph->Objects);
            graph->Index->LoadTopology(in, connectivity, maxMemoryBytes - inputs);
        }
        if (ReadSnapshotWord<ui64>(input) != buffer.Hash)
            ythrow yexception() << "HNSW snapshot checksum mismatch";
        return std::unique_ptr<THnswIndex>(new THnswIndex(std::make_unique<TImpl>(std::move(graph))));
    } catch (const std::exception& ex) {
        error = ex.what();
        return nullptr;
    }
}

size_t THnswIndex::EstimateMemoryBytes(size_t rowCount, size_t dimension, ui32 connectivity,
        size_t serializedKeyBytes) {
    // NMSLIB reserves up to 2*M friend ids on level zero. Higher levels and
    // container allocations are covered by the deliberately conservative
    // fixed overhead. Saturate on overflow so an attacker cannot wrap the
    // estimate and pass the cache budget check.
    const size_t friendBytes = static_cast<size_t>(connectivity) * 2
        * (sizeof(similarity::IdType) + sizeof(void*));
    // NMSLIB retains the source Object and copies its raw vector into the
    // contiguous optimized-search index. We no longer retain a third copy in
    // YDB wire format; GetVector() reconstructs its trailing format byte.
    const size_t bytesPerRow = 2 * dimension * sizeof(float)
        + EstimatedBytesPerNodeOverhead + friendBytes;
    if (rowCount != 0 && bytesPerRow > (Max<size_t>() - serializedKeyBytes) / rowCount) {
        return Max<size_t>();
    }
    return rowCount * bytesPerRow + serializedKeyBytes;
}

std::unique_ptr<THnswIndex> THnswIndex::Build(
    const Ydb::Table::VectorIndexSettings& settings,
    const std::vector<std::pair<TString, TString>>& keysAndVectors,
    ui64 maxMemoryBytes,
    TString& error, bool allowEmpty)
{
    if (settings.vector_type() != VectorIndexSettings::VECTOR_TYPE_FLOAT) {
        error = "HNSW index is only supported for float vectors";
        return nullptr;
    }

    TString settingsError;
    if (!NKMeans::ValidateSettingsPartial(settings, settingsError)) {
        error = TStringBuilder() << "Invalid HNSW settings: " << settingsError;
        return nullptr;
    }

    if (keysAndVectors.empty() && !allowEmpty) {
        error = "No vectors to build HNSW index from";
        return nullptr;
    }

    size_t dimension = settings.vector_dimension();
    if (dimension == 0) {
        // Auto-detect from the first well-formed vector.
        for (const auto& [key, vec] : keysAndVectors) {
            auto view = TFloatVectorView::FromSerialized(vec);
            if (view.IsValid()) {
                dimension = view.Dimension;
                break;
            }
        }
    }
    if (dimension == 0) {
        error = "Could not determine vector dimension";
        return nullptr;
    }

    if (maxMemoryBytes != 0) {
        const ui32 connectivity = settings.has_m()
            ? settings.m() : DefaultHnswM;
        size_t keyBytes = 0;
        for (const auto& [key, _] : keysAndVectors) {
            if (key.size() > Max<size_t>() - keyBytes) {
                keyBytes = Max<size_t>();
                break;
            }
            keyBytes += key.size();
        }
        const size_t estimated = EstimateMemoryBytes(
            keysAndVectors.size(), dimension, connectivity, keyBytes);
        if (estimated > maxMemoryBytes) {
            error = TStringBuilder() << "Estimated HNSW memory usage " << estimated
                << " exceeds budget " << maxMemoryBytes;
            return nullptr;
        }
    }

    auto space = CreateSpace(settings.metric(), error);
    if (!space) {
        return nullptr;
    }

    auto graph = std::make_shared<TGraph>(std::move(space), dimension);

    for (const auto& [key, vec] : keysAndVectors) {
        auto view = TFloatVectorView::FromSerialized(vec);
        if (!view.IsValid() || view.Dimension != dimension) {
            continue; // Skip malformed/mismatched rows; do not fail the whole build.
        }
        graph->AddVector(key, view.Data);
    }

    if (graph->Keys.empty() && !allowEmpty) {
        error = "No valid vectors of the expected dimension were found";
        return nullptr;
    }

    if (!graph->Build(settings)) {
        error = "Failed to build HNSW index";
        return nullptr;
    }

    return std::unique_ptr<THnswIndex>(new THnswIndex(std::make_unique<TImpl>(std::move(graph))));
}

THnswSearchResult THnswIndex::Search(TStringBuf targetVector, size_t k, TRowVersion version, ui32 efSearch) const {
    return Impl->Search(targetVector, k, version, efSearch);
}

ui64 THnswIndex::EstimatedSearchMemoryBytes(size_t k, ui32 efSearch) const {
    return Impl->EstimatedSearchMemoryBytes(k, efSearch);
}

bool THnswIndex::GetVector(TStringBuf key, TString& result, TRowVersion version) const {
    return Impl->GetVector(key, result, version);
}

bool THnswIndex::Upsert(TString key, TString vector, TRowVersion version) {
    return Impl->Upsert(std::move(key), std::move(vector), version);
}

void THnswIndex::Erase(TStringBuf key, TRowVersion version) {
    Impl->Erase(key, version);
}

bool THnswIndex::HasDelta(TStringBuf key) const {
    return Impl->HasDelta(key);
}

bool THnswIndex::HasChanges() const {
    return Impl->ChangeCount() != 0;
}

size_t THnswIndex::ChangeCount() const {
    return Impl->ChangeCount();
}

void THnswIndex::SetSnapshot(TRowVersion base, std::shared_ptr<THnswIndexChanges> changes,
        TRowVersion upper) {
    Impl->BaseVersion = base;
    Impl->UpperVersion = upper;
    Impl->Changes = std::move(changes);
}

TRowVersion THnswIndex::GetBaseVersion() const {
    return Impl->BaseVersion;
}

bool THnswIndex::CanRead(TRowVersion version) const {
    return Impl->CanRead(version);
}

bool THnswIndex::NeedsRebuild(ui64 deltaRows) const {
    return ChangeCount() > deltaRows;
}

bool THnswIndex::IsValidVector(TStringBuf vector, size_t dimension) {
    const auto view = TFloatVectorView::FromSerialized(vector);
    return view.IsValid() && view.Dimension == dimension;
}

size_t THnswIndex::Size() const {
    return Impl->Size();
}

size_t THnswIndex::Dimension() const {
    return Impl->Dim();
}

size_t THnswIndex::EstimatedMemoryBytes() const {
    return EstimateMemoryBytes(
        Impl->Size(), Impl->Dim(), Impl->GetConnectivity(), Impl->GetKeyBytes());
}

} // namespace NKikimr::NDataShard
