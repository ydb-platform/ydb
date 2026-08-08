#include "hnsw_index.h"

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
#include <util/generic/yexception.h>
#include <util/string/builder.h>

#include <thread>

namespace NKikimr::NDataShard {

namespace {

using Ydb::Table::VectorIndexSettings;

// Rough per-node overhead of the HNSW graph itself (friend lists at every
// level, on top of the raw vector data), used only to decide up front
// whether a build should be attempted at all. Actual usage additionally
// depends on connectivity (M) and level distribution.
constexpr size_t EstimatedBytesPerNodeOverhead = 256;

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
    const float* Data = nullptr;
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

        result.Data = reinterpret_cast<const float*>(serialized.data());
        result.Dimension = dataSize / sizeof(float);
        return result;
    }

    bool IsValid() const {
        return Data != nullptr && Dimension > 0;
    }
};

} // namespace

class THnswIndex::TImpl {
public:
    TImpl(std::unique_ptr<similarity::Space<float>> space, size_t dimension)
        : Space(std::move(space))
        , Dimension(dimension)
    {}

    ~TImpl() {
        Index.reset();
        for (const similarity::Object* obj : Objects) {
            delete obj;
        }
    }

    void AddVector(TString key, const float* data) {
        const auto id = static_cast<similarity::IdType>(Keys.size());
        auto* obj = new similarity::Object(id, /* label */ -1, Dimension * sizeof(float), data);
        Keys.push_back(std::move(key));
        Objects.push_back(obj);
    }

    bool Build() {
        if (Objects.empty()) {
            return false;
        }

        Index = std::make_unique<similarity::Hnsw<float>>(/* PrintProgress */ false, *Space, Objects);

        unsigned indexThreads = std::thread::hardware_concurrency();
        if (indexThreads == 0) {
            indexThreads = 1;
        }

        const std::string indexThreadQtyParam = "indexThreadQty=" + std::to_string(indexThreads);
        similarity::AnyParams buildParams(std::vector<std::string>{
            "M=16",
            "efConstruction=200",
            indexThreadQtyParam,
        });
        Index->CreateIndex(buildParams);
        Index->SetQueryTimeParams(similarity::AnyParams({"efSearch=100"}));
        return true;
    }

    THnswSearchResult Search(TStringBuf targetVector, size_t k) const {
        THnswSearchResult result;
        if (!Index || k == 0) {
            return result;
        }

        auto view = TFloatVectorView::FromSerialized(targetVector);
        if (!view.IsValid() || view.Dimension != Dimension) {
            return result;
        }

        std::unique_ptr<const similarity::Object> queryObj(
            new similarity::Object(-1, -1, Dimension * sizeof(float), view.Data));

        similarity::KNNQuery<float> query(*Space, queryObj.get(), static_cast<unsigned>(k));
        Index->Search(&query, -1);

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
            if (idx < Keys.size()) {
                reversed.emplace_back(Keys[idx], distance);
            }
        }

        result.Results.assign(reversed.rbegin(), reversed.rend());
        return result;
    }

    size_t Size() const {
        return Keys.size();
    }

    size_t Dim() const {
        return Dimension;
    }

private:
    std::unique_ptr<similarity::Space<float>> Space;
    size_t Dimension = 0;
    std::vector<const similarity::Object*> Objects;
    std::vector<TString> Keys; // Object::id() -> serialized primary key
    std::unique_ptr<similarity::Hnsw<float>> Index;
};

THnswIndex::THnswIndex(std::unique_ptr<TImpl> impl)
    : Impl(std::move(impl))
{}

THnswIndex::~THnswIndex() = default;

size_t THnswIndex::EstimateMemoryBytes(size_t rowCount, size_t dimension) {
    return rowCount * (dimension * sizeof(float) + EstimatedBytesPerNodeOverhead);
}

std::unique_ptr<THnswIndex> THnswIndex::Build(
    const Ydb::Table::VectorIndexSettings& settings,
    const std::vector<std::pair<TString, TString>>& keysAndVectors,
    ui64 maxMemoryBytes,
    TString& error)
{
    if (settings.vector_type() != VectorIndexSettings::VECTOR_TYPE_FLOAT) {
        error = "HNSW index is only supported for float vectors";
        return nullptr;
    }

    if (keysAndVectors.empty()) {
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
        const size_t estimated = EstimateMemoryBytes(keysAndVectors.size(), dimension);
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
        auto view = TFloatVectorView::FromSerialized(vec);
        if (!view.IsValid() || view.Dimension != dimension) {
            continue; // Skip malformed/mismatched rows; do not fail the whole build.
        }
        impl->AddVector(key, view.Data);
    }

    if (impl->Size() == 0) {
        error = "No valid vectors of the expected dimension were found";
        return nullptr;
    }

    if (!impl->Build()) {
        error = "Failed to build HNSW index";
        return nullptr;
    }

    return std::unique_ptr<THnswIndex>(new THnswIndex(std::move(impl)));
}

THnswSearchResult THnswIndex::Search(TStringBuf targetVector, size_t k) const {
    return Impl->Search(targetVector, k);
}

size_t THnswIndex::Size() const {
    return Impl->Size();
}

size_t THnswIndex::Dimension() const {
    return Impl->Dim();
}

size_t THnswIndex::EstimatedMemoryBytes() const {
    return EstimateMemoryBytes(Impl->Size(), Impl->Dim());
}

} // namespace NKikimr::NDataShard
