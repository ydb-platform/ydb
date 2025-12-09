#pragma once
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>
#include <ydb/core/tx/columnshard/engines/reader/common/comparable.h>
#include <ydb/core/tx/columnshard/engines/reader/common/description.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/constructor/read_metadata.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/context.h>

namespace NKikimr::NOlap::NReader::NCommon {

class TDataSourceConstructor: public ICursorEntity, public TMoveOnly {
private:
    ui32 SourceId = 0;
    TReplaceKeyAdapter Start;
    TReplaceKeyAdapter Finish;
    ui32 SourceIdx = 0;
    bool SourceIdxInitialized = false;

    virtual ui64 DoGetEntityId() const override {
        return GetSourceIdx();
    }

public:
    ui32 GetSourceId() const {
        return SourceId;
    }

    void SetIndex(const ui32 index) {
        AFL_VERIFY(!SourceIdxInitialized);
        SourceIdxInitialized = true;
        SourceIdx = index;
    }

    ui32 GetSourceIdx() const {
        AFL_VERIFY(SourceIdxInitialized);
        return SourceIdx;
    }

    TReplaceKeyAdapter ExtractStart() {
        return std::move(Start);
    }

    TReplaceKeyAdapter ExtractFinish() {
        return std::move(Finish);
    }

    TDataSourceConstructor(const ui32 sourceId, TReplaceKeyAdapter&& start, TReplaceKeyAdapter&& finish)
        : SourceId(sourceId)
        , Start(std::move(start))
        , Finish(std::move(finish))
    {
        AFL_VERIFY(SourceId);
    }

    const TReplaceKeyAdapter& GetStart() const {
        return Start;
    }
    const TReplaceKeyAdapter& GetFinish() const {
        return Finish;
    }

    class TComparator {
    private:
        const ERequestSorting Sorting;

    public:
        TComparator(const ERequestSorting sorting)
            : Sorting(sorting)
        {
            AFL_VERIFY(Sorting != ERequestSorting::NONE);
        }

        bool operator()(const TDataSourceConstructor& l, const TDataSourceConstructor& r) const {
            return std::make_pair(r.Start, r.SourceId) < std::make_pair(l.Start, l.SourceId);
        }
    };
};

template <std::derived_from<TDataSourceConstructor> TObject>
class TOrderedObjects {
private:
    const ERequestSorting Sorting;
    std::deque<TObject> HeapObjects;
    YDB_READONLY_DEF(std::deque<TObject>, AlreadySorted);
    bool Initialized = false;
    ui32 NextObjectIdx = 0;

public:
    TOrderedObjects(const ERequestSorting sorting)
        : Sorting(sorting) {
    }

    ERequestSorting GetSorting() const {
        return Sorting;
    }

    const std::deque<TObject>& GetObjects() const {
        if (AlreadySorted.size()) {
            AFL_VERIFY(!HeapObjects.size());
            return AlreadySorted;
        }
        return HeapObjects;
    }

    TObject& MutableNextObject() {
        AFL_VERIFY(GetSize());
        if (AlreadySorted.empty()) {
            PrepareOrdered(1);
        }
        return AlreadySorted.front();
    }

    void Initialize(std::deque<TObject>&& objects) {
        AFL_VERIFY(!Initialized);
        Initialized = true;
        if (Sorting != ERequestSorting::NONE) {
            HeapObjects = std::move(objects);
            std::make_heap(HeapObjects.begin(), HeapObjects.end(), typename TObject::TComparator(Sorting));
        } else {
            AlreadySorted = std::move(objects);
            for (auto& source : AlreadySorted) {
                source.SetIndex(NextObjectIdx++);
            }
        }
    }

    void PrepareOrdered(const ui32 count) {
        if (Sorting != ERequestSorting::NONE) {
            while (AlreadySorted.size() < count && HeapObjects.size()) {
                std::pop_heap(HeapObjects.begin(), HeapObjects.end(), typename TObject::TComparator(Sorting));
                HeapObjects.back().SetIndex(NextObjectIdx++);
                AlreadySorted.emplace_back(std::move(HeapObjects.back()));
                HeapObjects.pop_back();
            }
        } else {
            AFL_VERIFY(HeapObjects.empty());
        }
    }

    TObject PopFront() {
        if (AlreadySorted.empty()) {
            PrepareOrdered(1);
        }
        AFL_VERIFY(AlreadySorted.size());
        auto result = std::move(AlreadySorted.front());
        AlreadySorted.pop_front();
        return result;
    }

    bool IsEmpty() const {
        return AlreadySorted.empty() && HeapObjects.empty();
    }

    ui32 GetSize() const {
        return AlreadySorted.size() + HeapObjects.size();
    }

    void Clear() {
        AlreadySorted.clear();
        HeapObjects.clear();
    }
};

class TAccessorsFetcherImpl {
private:
    THashMap<ui64, std::shared_ptr<TPortionDataAccessor>> Accessors;
    int InFlightRequests = 0;
    bool Finished = false;

public:
    void Stop() {
        Finished = true;
        Accessors.clear();
    }

    ui32 GetSize() const {
        return Accessors.size();
    }

    bool HasRequest() const {
        return InFlightRequests;
    }

    std::shared_ptr<TPortionDataAccessor> ExtractAccessorVerified(const ui64 portionId) {
        auto it = Accessors.find(portionId);
        AFL_VERIFY(it != Accessors.end());
        auto result = std::move(it->second);
        Accessors.erase(it);
        return std::move(result);
    }

    void StartRequest(std::shared_ptr<TDataAccessorsRequest>&& request, const std::shared_ptr<NReader::NCommon::TSpecialReadContext>& context);

    void AddRequestedAccessors(TDataAccessorsResult&& accessors) {
        if (Finished) {
            return;
        }
        AFL_VERIFY(InFlightRequests);
        if (Accessors.empty()) {
            Accessors = std::move(accessors.ExtractPortions());
        } else {
            for (auto&& i : accessors.ExtractPortions()) {
                AFL_VERIFY(Accessors.emplace(i.first, std::move(i.second)).second);
            }
        }
        AFL_VERIFY(InFlightRequests);
        --InFlightRequests;
    }
};

class TSourcesConstructorWithAccessorsImpl: public ISourcesConstructor {
protected:
    TAccessorsFetcherImpl Accessors;

public:
    void AddAccessors(TDataAccessorsResult&& accessors) {
        Accessors.AddRequestedAccessors(std::move(accessors));
    }
};

template <std::derived_from<TDataSourceConstructor> TConstructor>
class TSourcesConstructorWithAccessors: public TSourcesConstructorWithAccessorsImpl {
private:
    TOrderedObjects<TConstructor> Constructors;

    virtual TString DoDebugString() const override {
        return "{CC:" + ::ToString(Constructors.GetSize()) + "}";
    }

    virtual TString GetClassName() const override {
        return "GENERAL_ORDERING::" + ::ToString(Constructors.GetSorting());
    }

    virtual void DoClear() override {
        Constructors.Clear();
        Accessors.Stop();
    }
    virtual void DoAbort() override {
        Constructors.Clear();
        Accessors.Stop();
    }
    virtual bool DoIsFinished() const override {
        return Constructors.IsEmpty();
    }

    virtual std::shared_ptr<IDataSource> DoExtractNextImpl(const std::shared_ptr<TSpecialReadContext>& context) = 0;

    virtual std::shared_ptr<IDataSource> DoTryExtractNext(
        const std::shared_ptr<TSpecialReadContext>& context, const ui32 inFlightCurrentLimit) override final {
        if (!Accessors.GetSize() && Accessors.HasRequest()) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "SKIP_NO_ACCESSORS")("has_request", Accessors.HasRequest())(
                "in_flight", inFlightCurrentLimit);
            return nullptr;
        }
        if (!Accessors.HasRequest() && (Accessors.GetSize() < Constructors.GetSize() && Accessors.GetSize() < inFlightCurrentLimit)) {
            Constructors.PrepareOrdered(inFlightCurrentLimit * 2);
            std::shared_ptr<TDataAccessorsRequest> request =
                std::make_shared<TDataAccessorsRequest>(NGeneralCache::TPortionsMetadataCachePolicy::EConsumer::SCAN);
            for (ui32 idx = Accessors.GetSize(); idx < Constructors.GetAlreadySorted().size(); ++idx) {
                request->AddPortion(Constructors.GetAlreadySorted()[idx].GetPortion());
                if (request->GetSize() == 2 * inFlightCurrentLimit) {
                    break;
                }
            }
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "START_FETCH_ACCESSORS")("acc_count", Accessors.GetSize())(
                "add", request->GetSize())("in_flight", inFlightCurrentLimit);
            request->SetColumnIds(context->GetAllUsageColumns()->GetColumnIds());
            Accessors.StartRequest(std::move(request), context);
        }
        if (!Accessors.GetSize()) {
            AFL_VERIFY(Accessors.HasRequest());
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "SKIP_NO_ACCESSORS")("has_request", Accessors.HasRequest())(
                "in_flight", inFlightCurrentLimit);
            return nullptr;
        }
        return DoExtractNextImpl(context);
    }

public:
    const std::deque<TConstructor>& GetConstructors() const {
        return Constructors.GetObjects();
    }

    ui32 GetConstructorsCount() const {
        return Constructors.GetSize();
    }

    void DropNextConstructor() {
        Constructors.PopFront();
    }

    TConstructor& MutableNextConstructor() {
        return Constructors.MutableNextObject();
    }

    class TObjectWithAccessor {
    private:
        TConstructor Object;
        YDB_ACCESSOR_DEF(std::shared_ptr<TPortionDataAccessor>, Accessor);

    public:
        TObjectWithAccessor(TConstructor&& obj, std::shared_ptr<TPortionDataAccessor>&& acc)
            : Object(std::move(obj))
            , Accessor(std::move(acc)) {
        }

        TConstructor& MutableObject() {
            return Object;
        }
    };

    TObjectWithAccessor PopObjectWithAccessor() {
        auto object = Constructors.PopFront();
        auto acc = Accessors.ExtractAccessorVerified(object.GetPortion()->GetPortionId());
        TObjectWithAccessor result(std::move(object), std::move(acc));
        return result;
    }

    TSourcesConstructorWithAccessors(const ERequestSorting sorting)
        : Constructors(sorting) {
    }

    void InitializeConstructors(std::deque<TConstructor>&& objects) {
        Constructors.Initialize(std::move(objects));
    }
};
}   // namespace NKikimr::NOlap::NReader::NCommon
