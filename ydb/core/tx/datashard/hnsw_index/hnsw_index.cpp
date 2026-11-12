#include "hnsw_index.h"

#include <ydb/core/base/kmeans_clusters.h>
#include <ydb/library/yql/udfs/common/knn/knn-defines.h>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wsign-compare"
#pragma clang diagnostic ignored "-Wunused-parameter"
#pragma clang diagnostic ignored "-Wunused-but-set-variable"
#include <ydb/library/nmslib/include/knnquery.h>
#include <ydb/library/nmslib/include/knnqueue.h>
#include <ydb/library/nmslib/include/logging.h>
#include <ydb/library/nmslib/include/method/hnsw.h>
#include <ydb/library/nmslib/include/space/space_lp.h>
#include <ydb/library/nmslib/include/space/space_scalar.h>
#pragma clang diagnostic pop

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/algorithm.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>

#include <cmath>
#include <tuple>
#include <queue>
#include <cstring>

namespace NKikimr::NDataShard {

    namespace {

        using Ydb::Table::VectorIndexSettings;

        // Rough fixed per-node overhead, excluding level-zero neighbor storage.
        constexpr size_t EstimatedBytesPerNodeOverhead = 256;
        // Construct synchronously with one NMSLIB worker; the caller controls scheduling.
        constexpr unsigned BuildThreadsPerIndex = 1;
        constexpr ui32 DefaultHnswEfConstruction = 200;
        constexpr ui64 DefaultHnswDeltaRows = 10000;
        constexpr ui64 DefaultHnswMinRows = 10000;

        size_t SaturatingAdd(size_t lhs, size_t rhs) {
            return rhs > Max<size_t>() - lhs ? Max<size_t>() : lhs + rhs;
        }

        size_t SaturatingMultiply(size_t lhs, size_t rhs) {
            return lhs != 0 && rhs > Max<size_t>() / lhs ? Max<size_t>() : lhs * rhs;
        }

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
            bool AllZero = true;

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

                for (size_t offset = 0; offset < dataSize; offset += sizeof(float)) {
                    float value;
                    std::memcpy(&value, serialized.data() + offset, sizeof(value));
                    if (!std::isfinite(value)) { return result; }
                    result.AllZero &= value == 0;
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

    } // namespace

    ui64 GetHnswDeltaRows(const VectorIndexSettings& settings) {
        return settings.has_delta_rows()
                   ? settings.delta_rows()
                   : DefaultHnswDeltaRows;
    }

    ui64 GetHnswMinRows(const VectorIndexSettings& settings) {
        return settings.has_min_rows() ? settings.min_rows() : DefaultHnswMinRows;
    }

    bool AreHnswIndexSettingsCompatible(
        const VectorIndexSettings& cached,
        const VectorIndexSettings& requested) {
        const auto connectivity = [](const auto& settings) {
            return settings.has_m()
                       ? settings.m()
                       : DefaultHnswM;
        };
        const auto constructionCandidates = [](const auto& settings) {
            return settings.has_ef_construction()
                       ? settings.ef_construction()
                       : DefaultHnswEfConstruction;
        };
        return cached.metric() == requested.metric() && cached.vector_type() == requested.vector_type() && cached.vector_dimension() == requested.vector_dimension() && connectivity(cached) == connectivity(requested) && constructionCandidates(cached) == constructionCandidates(requested);
    }

    void THnswIndexChanges::Set(TString key, TRowVersion version,
                                std::optional<TString> vector, std::shared_ptr<void> reservation) {
        auto& history = Rows[key];
        const auto before = history.empty() ? std::optional<TRowVersion>{} : history.back().Version;
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
        UpdateCounts(before, history.back().Version);
        // Compact just this key; its obsolete vector reservations must not wait
        // for the periodic full-journal prune.
        if (RetentionInitialized) {
            CompactHistory(key, history);
        }
    }

    void THnswIndexChanges::UpdateCounts(std::optional<TRowVersion> before, std::optional<TRowVersion> after) {
        std::lock_guard guard(CountsMutex);
        for (auto& [base, count] : CountsByBase) {
            if (before && *before > base) { --count; }
            if (after && *after > base) { ++count; }
        }
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
        std::lock_guard guard(CountsMutex);
        if (const auto* count = CountsByBase.FindPtr(base)) { return *count; }
        size_t count = 0;
        for (const auto& [_, history] : Rows) {
            count += !history.empty() && history.back().Version > base;
        }
        // Bound auxiliary accounting even for callers probing arbitrary bases.
        if (CountsByBase.size() == 8) { CountsByBase.erase(CountsByBase.begin()); }
        CountsByBase.emplace(base, count);
        return count;
    }

    void THnswIndexChanges::PruneThrough(TRowVersion base) {
        for (auto it = Rows.begin(); it != Rows.end();) {
            auto& history = it->second;
            auto end = std::upper_bound(history.begin(), history.end(), base,
                                        [](TRowVersion value, const TVersion& item) { return value < item.Version; });
            if (end == history.begin()) {
                ++it;
                continue;
            }
            UpdateCounts(history.back().Version, end == history.end()
                ? std::optional<TRowVersion>{} : history.back().Version);
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

    bool THnswIndexChanges::Covers(TRowVersion version) const {
        if (version >= LowerReadVersion || std::binary_search(PinnedVersions.begin(), PinnedVersions.end(), version)) {
            return true;
        }
        std::lock_guard guard(ReadPins->Mutex);
        return ReadPins->Versions.contains(version);
    }

    std::shared_ptr<void> THnswIndexChanges::PinReadVersion(TRowVersion version) {
        if (!Covers(version)) { return {}; }
        struct TPin {
            std::shared_ptr<TReadPins> Pins;
            TRowVersion Version;

            TPin(std::shared_ptr<TReadPins> pins, TRowVersion version)
                : Pins(std::move(pins)), Version(version)
            {
                std::lock_guard guard(Pins->Mutex);
                ++Pins->Versions[Version];
            }

            ~TPin() {
                std::lock_guard guard(Pins->Mutex);
                auto it = Pins->Versions.find(Version);
                if (!--it->second) { Pins->Versions.erase(it); }
            }
        };
        return std::make_shared<TPin>(ReadPins, version);
    }

    void THnswIndexChanges::AdvanceRetentionFloor(TRowVersion floor) {
        LowerReadVersion = Max(LowerReadVersion, floor);
        RetentionInitialized = true;
    }

    void THnswIndexChanges::AdvanceRetention(TRowVersion floor, const std::vector<TRowVersion>& pins) {
        std::vector<TRowVersion> retained;
        for (auto version : pins) {
            if (Covers(version)) { retained.push_back(version); }
        }
        Sort(retained);
        retained.erase(std::unique(retained.begin(), retained.end()), retained.end());
        PinnedVersions.swap(retained);
        AdvanceRetentionFloor(floor);
    }

    void THnswIndexChanges::CompactHistory(TStringBuf key, THistory& history) {
        std::vector<size_t> keep;
        // Keep the predecessor of the floor, every newer version, and each pin's predecessor.
        auto boundary = std::upper_bound(history.begin(), history.end(), LowerReadVersion,
            [](TRowVersion value, const TVersion& item) { return value < item.Version; });
        if (boundary != history.begin()) { --boundary; }
        {
            std::lock_guard guard(ReadPins->Mutex);
            for (size_t i = 0; i < size_t(boundary - history.begin()); ++i) {
                const auto retained = std::lower_bound(PinnedVersions.begin(), PinnedVersions.end(), history[i].Version);
                const auto pin = ReadPins->Versions.lower_bound(history[i].Version);
                if ((retained != PinnedVersions.end() && *retained < history[i + 1].Version)
                        || (pin != ReadPins->Versions.end() && pin->first < history[i + 1].Version)) {
                    keep.push_back(i);
                }
            }
        }
        for (size_t i = boundary - history.begin(); i < history.size(); ++i) { keep.push_back(i); }
        size_t next = 0;
        size_t output = 0;
        const size_t previousSize = history.size();
        for (size_t i = 0; i < previousSize; ++i) {
            if (next < keep.size() && keep[next] == i) {
                if (output != i) { history[output] = std::move(history[i]); }
                ++output;
                ++next;
            } else {
                EstimatedBytes -= EstimateBytes(key, history[i].Vector ? history[i].Vector->size() : 0);
            }
        }
        VersionCount -= previousSize - output;
        history.resize(output);
        if (history.capacity() > 2 * history.size()) { THistory(history).swap(history); }
    }

    void THnswIndexChanges::Compact(TRowVersion floor, const std::vector<TRowVersion>& pins) {
        AdvanceRetention(floor, pins);
        for (auto& [key, history] : Rows) { CompactHistory(key, history); }
    }

    void THnswIndexChanges::Clear() {
        Rows.clear();
        std::lock_guard guard(CountsMutex);
        CountsByBase.clear();
        VersionCount = 0;
        EstimatedBytes = 0;
    }

    size_t THnswIndexChanges::EstimateBytes(TStringBuf key, size_t vectorBytes) {
        return SaturatingAdd(SaturatingAdd(SaturatingMultiply(2, key.size()), vectorBytes),
                             2 * sizeof(TVersion) + 128);
    }

    class THnswIndex::TImpl {
    public:
        TImpl(std::unique_ptr<similarity::Space<float>> space, size_t dimension)
            : Space(std::move(space))
            , Dimension(dimension)
        {
        }

        ~TImpl() {
            // Hnsw keeps pointers to Objects, so destroy it before their owners.
            Index.reset();
        }

        void AddVector(TString key, const void* data, bool allZero) {
            HasZeroVector |= allZero;
            const auto id = static_cast<similarity::IdType>(Keys.size());
            auto obj = std::make_unique<similarity::Object>(
                id, /* label */ -1, Dimension * sizeof(float), data);
            KeyBytes += key.size();
            Keys.push_back(std::move(key));
            Objects.push_back(obj.get());
            OwnedObjects.push_back(std::move(obj));
        }

        bool Build(const VectorIndexSettings& settings, const std::atomic<bool>* cancelled) {
            ScopedLoggingDisabled quietBuild;
            SquaredGraphDistances = settings.metric() == VectorIndexSettings::DISTANCE_EUCLIDEAN;
            CosineDistances = settings.metric() == VectorIndexSettings::DISTANCE_COSINE
                || settings.metric() == VectorIndexSettings::SIMILARITY_COSINE;
            if (Objects.empty()) {
                return true; // Empty rebuilt generations still have a searchable journal.
            }

            Index = std::make_unique<similarity::Hnsw<float>>(/* PrintProgress */ false, *Space, Objects);

            Connectivity = settings.has_m()
                               ? settings.m()
                               : DefaultHnswM;
            similarity::AnyParams buildParams(std::vector<std::string>{
                "M=" + std::to_string(Connectivity),
                "efConstruction=" + std::to_string(settings.has_ef_construction()
                                                       ? settings.ef_construction()
                                                       : DefaultHnswEfConstruction),
                "indexThreadQty=" + std::to_string(BuildThreadsPerIndex),
            });
            Index->SetBuildCancellation(cancelled);
            Index->CreateIndex(buildParams);
            Index->SetBuildCancellation(nullptr);
            if (cancelled && cancelled->load(std::memory_order_relaxed)) { return false; }
            for (size_t i = 0; i < Objects.size(); ++i) {
                if (Objects[i]->id() != static_cast<similarity::IdType>(i)) {
                    Index.reset();
                    return false;
                }
            }
            return true;
        }

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
            if (!view.IsValid() || view.Dimension != Dimension) {
                return result;
            }

            if (CosineDistances && (view.AllZero || HasZeroVector)) {
                result.Covered = false;
                return result;
            }

            std::unique_ptr<const similarity::Object> queryObj(
                new similarity::Object(-1, -1, Dimension * sizeof(float), view.Data));

            // Dirty base nodes may hide live candidates, but a large journal
            // must never turn ANN into an ef ~= journal-size traversal.
            const size_t baseK = Min(k, Keys.size());
            const size_t graphK = baseK + Min(Min(ChangeCount(), Keys.size() - baseK), Max<size_t>(k, efSearch));
            similarity::KNNQuery<float> query(*Space, queryObj.get(), static_cast<unsigned>(Max<size_t>(graphK, 1)));
            if (Index) {
                Index->SearchWithEf(&query, efSearch);
            }

            const similarity::KNNQueue<float>* queue = query.Result();
            std::vector<std::pair<TString, float>> reversed;
            reversed.reserve(queue->Size());
            // KNNQueue is a max-heap over distance; popping yields farthest-first.
            // Clone it since Result() is owned by the query and Pop() is destructive.
            std::unique_ptr<similarity::KNNQueue<float>> mutableQueue(queue->Clone());
            while (!mutableQueue->Empty()) {
                const float graphDistance = mutableQueue->TopDistance();
                // CreateIndex uses optimized squared L2 for SpaceLp(2), while
                // journal scoring through Space returns ordinary L2.
                const float distance = SquaredGraphDistances ? std::sqrt(graphDistance) : graphDistance;
                const similarity::Object* obj = mutableQueue->Pop();
                const size_t idx = static_cast<size_t>(obj->id());
                if (idx < Keys.size()) {
                    reversed.emplace_back(Keys[idx], distance);
                }
            }

            using TCandidate = std::pair<float, TString>;
            std::priority_queue<TCandidate> nearest;
            auto add = [&](const TString& key, float distance) {
                if (std::isnan(distance)) { result.Covered = false; return; }
                TCandidate candidate(distance, key);
                if (nearest.size() < k) {
                    nearest.push(std::move(candidate));
                } else if (candidate < nearest.top()) {
                    nearest.pop();
                    nearest.push(std::move(candidate));
                }
            };
            size_t cleanGraphCandidates = 0;
            for (const auto& candidate : reversed) {
                if (!Changes->Find(candidate.first, readVersion, BaseVersion)) {
                    ++cleanGraphCandidates;
                    add(candidate.first, candidate.second);
                }
            }
            if (graphK < Keys.size() && cleanGraphCandidates < k) {
                result.Covered = false; // The bounded overfetch could have hidden live rows.
            }
            for (const auto& [key, history] : Changes->Rows) {
                const auto* visible = THnswIndexChanges::Find(history, readVersion, BaseVersion);
                if (!visible || !visible->Vector) { continue; }
                auto deltaView = TFloatVectorView::FromSerialized(*visible->Vector);
                if (!deltaView.IsValid() || deltaView.Dimension != Dimension) { continue; }
                if (CosineDistances && deltaView.AllZero) { result.Covered = false; continue; }
                similarity::Object deltaObj(-1, -1, Dimension * sizeof(float), deltaView.Data);
                add(key, query.DistanceObjLeft(&deltaObj));
            }
            result.Results.reserve(nearest.size());
            while (!nearest.empty()) {
                result.Results.emplace_back(nearest.top().second, nearest.top().first);
                nearest.pop();
            }
            std::reverse(result.Results.begin(), result.Results.end());
            return result;
        }

        size_t Size() const {
            return Keys.size();
        }

        size_t Dim() const {
            return Dimension;
        }

        ui32 GetConnectivity() const {
            return Connectivity;
        }

        size_t GetKeyBytes() const {
            return KeyBytes;
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
            // Inspection helper; serving queries reads projected cells from LocalDB.
            const auto it = std::find(Keys.begin(), Keys.end(), key);
            if (it == Keys.end()) { return false; }
            const auto* object = OwnedObjects[it - Keys.begin()].get();
            result.assign(object->data(), object->datalength());
            result.push_back(static_cast<char>(Format<float>));
            return true;
        }

        bool Upsert(TString key, TString vector, TRowVersion version) {
            auto view = TFloatVectorView::FromSerialized(vector);
            if (!view.IsValid() || view.Dimension != Dimension) {
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
            return Changes->Valid && Changes->Covers(version) && BaseVersion <= version && version <= UpperVersion;
        }

        TRowVersion BaseVersion = TRowVersion::Min();
        TRowVersion UpperVersion = TRowVersion::Max();
        std::shared_ptr<THnswIndexChanges> Changes = std::make_shared<THnswIndexChanges>();

    private:
        std::unique_ptr<similarity::Space<float>> Space;
        size_t Dimension = 0;
        ui32 Connectivity = DefaultHnswM;
        bool SquaredGraphDistances = false;
        bool CosineDistances = false;
        bool HasZeroVector = false;
        size_t KeyBytes = 0;
        std::vector<const similarity::Object*> Objects;
        std::vector<std::unique_ptr<similarity::Object>> OwnedObjects;
        std::vector<TString> Keys; // Object::id() -> serialized primary key
        std::unique_ptr<similarity::Hnsw<float>> Index;
    };

    THnswIndex::THnswIndex(std::unique_ptr<TImpl> impl)
        : Impl(std::move(impl))
    {
    }

    THnswIndex::~THnswIndex() = default;

    size_t THnswIndex::EstimateMemoryBytes(size_t rowCount, size_t dimension, ui32 connectivity,
                                           size_t serializedKeyBytes) {
        // NMSLIB retains the pointer-based graph alongside the optimized graph.
        // Level zero reserves 2*M+1 neighbor pointers in each HnswNode and
        // 2*M+1 ids in the optimized representation. Higher levels and container
        // allocations use the fixed overhead. Saturate all estimate arithmetic.
        const size_t friendSlots = SaturatingAdd(SaturatingMultiply(connectivity, 2), 1);
        const size_t friendBytes = SaturatingMultiply(friendSlots,
                                                      sizeof(similarity::HnswNode*) + sizeof(similarity::IdType));
        // NMSLIB retains the source Object and copies its raw vector into the
        // contiguous optimized-search index. We no longer retain a third copy in
        // YDB wire format; GetVector() reconstructs its trailing format byte.
        const size_t bytesPerRow = SaturatingAdd(
            SaturatingAdd(SaturatingMultiply(dimension, 2 * sizeof(float)), EstimatedBytesPerNodeOverhead),
            friendBytes);
        return SaturatingAdd(SaturatingMultiply(rowCount, bytesPerRow), serializedKeyBytes);
    }

    std::unique_ptr<THnswIndex> THnswIndex::Build(
        const Ydb::Table::VectorIndexSettings& settings,
        const std::vector<std::pair<TString, TString>>& keysAndVectors,
        ui64 maxMemoryBytes,
        TString& error, bool allowEmpty, const std::atomic<bool>* cancelled)
    {
        if (settings.vector_type() != VectorIndexSettings::VECTOR_TYPE_FLOAT) {
            error = "HNSW index is only supported for float vectors";
            return nullptr;
        }

        Ydb::Table::KMeansTreeSettings hnswSettings;
        *hnswSettings.mutable_settings() = settings;
        TString settingsError;
        if (!NKMeans::ValidateHnswSettingsPartial(hnswSettings, settingsError)) {
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
                                          ? settings.m()
                                          : DefaultHnswM;
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

        auto impl = std::make_unique<TImpl>(std::move(space), dimension);

        for (const auto& [key, vec] : keysAndVectors) {
            if (cancelled && cancelled->load(std::memory_order_relaxed)) { error = "HNSW build cancelled"; return nullptr; }
            auto view = TFloatVectorView::FromSerialized(vec);
            if (!view.IsValid() || view.Dimension != dimension) {
                continue; // Skip malformed/mismatched rows; do not fail the whole build.
            }
            impl->AddVector(key, view.Data, view.AllZero);
        }

        if (impl->Size() == 0 && !allowEmpty) {
            error = "No valid vectors of the expected dimension were found";
            return nullptr;
        }

        if (!impl->Build(settings, cancelled)) {
            error = "Failed to build HNSW index";
            return nullptr;
        }

        return std::unique_ptr<THnswIndex>(new THnswIndex(std::move(impl)));
    }

    THnswSearchResult THnswIndex::Search(TStringBuf targetVector, size_t k, TRowVersion version, ui32 efSearch) const {
        return Impl->Search(targetVector, k, version, efSearch);
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

    std::shared_ptr<void> THnswIndex::PinReadVersion(TRowVersion version) {
        return CanRead(version) ? Impl->Changes->PinReadVersion(version) : nullptr;
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
