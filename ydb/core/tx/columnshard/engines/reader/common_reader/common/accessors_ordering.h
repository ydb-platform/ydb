#pragma once
#include <ydb/core/tx/columnshard/data_accessor/request.h>
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>
#include <ydb/core/tx/columnshard/engines/reader/common/comparable.h>
#include <ydb/core/tx/columnshard/engines/reader/common/description.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/constructor/read_metadata.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/context.h>

#include <ydb/library/conclusion/status.h>

namespace NKikimr::NOlap::NReader::NCommon {

class TDataSourceConstructor: public ICursorEntity, public TMoveOnly {
private:
    TReplaceKeyAdapter Start;
    TReplaceKeyAdapter Finish;
    bool Conflicting;
    ui32 SourceIdx = 0;
    bool SourceIdxInitialized = false;

    virtual ui64 DoGetEntityId() const override {
        return GetSourceIdx();
    }

public:
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

    TDataSourceConstructor(TReplaceKeyAdapter&& start, TReplaceKeyAdapter&& finish, const bool conflicting)
        : Start(std::move(start))
        , Finish(std::move(finish))
        , Conflicting(conflicting)
    {
    }

    const TReplaceKeyAdapter& GetStart() const {
        return Start;
    }

    const TReplaceKeyAdapter& GetFinish() const {
        return Finish;
    }

    bool IsConflicting() const {
        return Conflicting;
    }

    virtual bool QueryAgnosticLess(const TDataSourceConstructor& rhs) const = 0;
    virtual ~TDataSourceConstructor() = default;

    TDataSourceConstructor(TDataSourceConstructor&& other)
        : Start(std::move(other.Start))
        , Finish(std::move(other.Finish))
        , Conflicting(other.Conflicting)
        , SourceIdx(other.SourceIdx)
        , SourceIdxInitialized(other.SourceIdxInitialized)
    {
    }

    TDataSourceConstructor& operator=(TDataSourceConstructor&& other) {
        Start = std::move(other.Start);
        Finish = std::move(other.Finish);
        Conflicting = other.Conflicting;
        SourceIdx = other.SourceIdx;
        SourceIdxInitialized = other.SourceIdxInitialized;
        return *this;
    }

    class TLessByStart {
    public:
        bool operator()(const TDataSourceConstructor& l, const TDataSourceConstructor& r) const {
            auto cmp = l.Start.Compare(r.Start);
            if (cmp == std::partial_ordering::less) {
                return true;
            } else if (cmp == std::partial_ordering::greater) {
                return false;
            } else {
                return l.QueryAgnosticLess(r);
            }
        }
    };

    class TLessByFinish {
    public:
        bool operator()(const TDataSourceConstructor& l, const TDataSourceConstructor& r) const {
            auto cmp = l.Finish.Compare(r.Finish);
            if (cmp == std::partial_ordering::less) {
                return true;
            } else if (cmp == std::partial_ordering::greater) {
                return false;
            } else {
                return l.QueryAgnosticLess(r);
            }
        }
    };

    class TSimpleLess {
    public:
        bool operator()(const TDataSourceConstructor& l, const TDataSourceConstructor& r) const {
            return l.QueryAgnosticLess(r);
        }
    };

    // Comparator for std::make_heap/pop_heap, which is a max heap. We need a min heap, so we swap arguments.
    class TReversedComparator {
    private:
        ESourcesSorting SourcesSorting;

        bool Less(const TDataSourceConstructor& l, const TDataSourceConstructor& r) const {
            switch (SourcesSorting) {
                case ESourcesSorting::SourceIdAsc:
                    return TSimpleLess()(l, r);
                case ESourcesSorting::FirstPkAsc:
                case ESourcesSorting::LastPkDesc:
                    // the same comparator for them because we know
                    // that TReplaceKeyAdapter swaps first/last already,
                    // so we should not do that here.
                    // Not a very smart and obvious code contract, I know,
                    // some day, maybe, we will fix it
                    return TLessByStart()(l, r);
                case ESourcesSorting::LastPkAsc:
                    return TLessByFinish()(l, r);
            }
            AFL_VERIFY(false)("sources_sorting", (ui64)SourcesSorting);
            return false;
        }

    public:
        TReversedComparator(const ESourcesSorting sourcesSorting)
            : SourcesSorting(sourcesSorting)
        {
        }

        bool operator()(const TDataSourceConstructor& l, const TDataSourceConstructor& r) const {
            // comparator is reversed, so we swap the arguments to achieve that
            return Less(r, l);
        }
    };
};

template <std::derived_from<TDataSourceConstructor> TObject>
class TOrderedObjects {
private:
    const ESourcesSorting SourcesSorting;
    std::deque<TObject> HeapObjects;
    YDB_READONLY_DEF(std::deque<TObject>, AlreadySorted);
    bool Initialized = false;
    ui32 NextObjectIdx = 0;

public:
    TOrderedObjects(const ESourcesSorting sourcesSorting)
        : SourcesSorting(sourcesSorting)
    {
    }

    ESourcesSorting GetSourcesSorting() const {
        return SourcesSorting;
    }

    template <typename F>
    void ForEachObject(F&& f) const {
        for (const auto& obj : AlreadySorted) {
            f(obj);
        }
        for (const auto& obj : HeapObjects) {
            f(obj);
        }
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
        HeapObjects = std::move(objects);
        // we need a min heap, so we use a reversed comparator to achieve that
        std::make_heap(HeapObjects.begin(), HeapObjects.end(), typename TObject::TReversedComparator(SourcesSorting));
    }

    void PrepareOrdered(const ui32 count) {
        while (AlreadySorted.size() < count && HeapObjects.size()) {
            // we need a min heap, so we use a reversed comparator to achieve that
            std::pop_heap(HeapObjects.begin(), HeapObjects.end(), typename TObject::TReversedComparator(SourcesSorting));
            HeapObjects.back().SetIndex(NextObjectIdx++);
            AlreadySorted.emplace_back(std::move(HeapObjects.back()));
            HeapObjects.pop_back();
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

    TConclusionStatus AddRequestedAccessors(TDataAccessorsResult&& accessors) {
        if (Finished) {
            return TConclusionStatus::Success();
        }

        AFL_VERIFY(InFlightRequests);
        --InFlightRequests;

        if (accessors.HasErrors()) {
            const TString errorMessage = TStringBuilder{} << "prefetch accessors fetch failed: " << accessors.GetErrorMessage();
            YDB_LOG_ERROR_COMP(NKikimrServices::TX_COLUMNSHARD, "", {"error", errorMessage});
            return TConclusionStatus::Fail(errorMessage);
        }

        if (accessors.HasRemovedData()) {
            const TString errorMessage = TStringBuilder{}
                                         << "prefetch accessors fetch has removed data, count=" << accessors.GetRemovedData().size()
                                         << ". The reading data snapshot is stale. Please reduce the database load and try again.";
            YDB_LOG_ERROR_COMP(NKikimrServices::TX_COLUMNSHARD, "", {"error", errorMessage});
            return TConclusionStatus::Fail(errorMessage);
        }

        if (Accessors.empty()) {
            Accessors = std::move(accessors.ExtractPortions());
        } else {
            for (auto&& i : accessors.ExtractPortions()) {
                AFL_VERIFY(Accessors.emplace(i.first, std::move(i.second)).second);
            }
        }
        return TConclusionStatus::Success();
    }
};

class TSourcesConstructorWithAccessorsImpl: public ISourcesConstructor {
protected:
    TAccessorsFetcherImpl Accessors;

public:
    TConclusionStatus AddAccessors(TDataAccessorsResult&& accessors) {
        return Accessors.AddRequestedAccessors(std::move(accessors));
    }
};

template <std::derived_from<TDataSourceConstructor> TConstructor>
class TSourcesConstructorWithAccessors: public TSourcesConstructorWithAccessorsImpl {
private:
    TOrderedObjects<TConstructor> Constructors;
    // Conflicting portions are not sorted, they produce no rows and no cursor can name them.
    // They are scanned only so TConflictDetector can break the lock.
    // So we need to process them first before a limit can stop the scan.
    // Their index only has to be unique, since ISyncPoint::AddSource leaves them out of the ordered stream.
    std::deque<TConstructor> ConflictingConstructors;
    ui32 NextConflictingIdx = Max<ui32>();

    virtual TString DoDebugString() const override {
        return "{CC:" + ::ToString(Constructors.GetSize()) + "}";
    }

    virtual TString GetClassName() const override {
        return "GENERAL_ORDERING::" + ::ToString(Constructors.GetSourcesSorting());
    }

    virtual void DoClear() override {
        ConflictingConstructors.clear();
        Constructors.Clear();
        Accessors.Stop();
    }

    virtual void DoAbort() override {
        ConflictingConstructors.clear();
        Constructors.Clear();
        Accessors.Stop();
    }

    virtual bool DoIsFinished() const override {
        return ConflictingConstructors.empty() && Constructors.IsEmpty();
    }

    virtual std::shared_ptr<IDataSource> DoExtractNextImpl(const std::shared_ptr<TSpecialReadContext>& context) = 0;

    virtual std::shared_ptr<IDataSource> DoTryExtractNext(
        const std::shared_ptr<TSpecialReadContext>& context, const ui32 inFlightCurrentLimit) override final {
        if (!context->GetCommonContext()->IsActive()) {
            return nullptr;
        }
        if (!Accessors.GetSize() && Accessors.HasRequest()) {
            YDB_LOG_DEBUG_COMP(NKikimrServices::TX_COLUMNSHARD_SCAN, "",
                {"event", "SKIP_NO_ACCESSORS"},
                {"hasRequest", Accessors.HasRequest()},
                {"inFlight", inFlightCurrentLimit});
            return nullptr;
        }
        const ui32 constructorsCount = ConflictingConstructors.size() + Constructors.GetSize();
        const ui32 twiceInFlightCurrentLimit = 2 * inFlightCurrentLimit;
        if (!Accessors.HasRequest() && (Accessors.GetSize() < constructorsCount && Accessors.GetSize() < inFlightCurrentLimit)) {
            std::shared_ptr<TDataAccessorsRequest> request =
                std::make_shared<TDataAccessorsRequest>(NGeneralCache::TPortionsMetadataCachePolicy::EConsumer::SCAN);
            // Accessors are fetched in hand-out order and popped from the front, so the ones already held are
            // the first Accessors.GetSize() sources still to hand out. Conflicting sources are handed out
            // first, so the ordered queue starts at whatever is left of that count.
            const ui32 alreadyFetched = Accessors.GetSize();
            for (ui32 idx = alreadyFetched; idx < ConflictingConstructors.size() && request->GetSize() < twiceInFlightCurrentLimit; ++idx) {
                request->AddPortion(ConflictingConstructors[idx].GetPortion());
            }
            if (request->GetSize() < twiceInFlightCurrentLimit) {
                Constructors.PrepareOrdered(twiceInFlightCurrentLimit);
                const auto& ordered = Constructors.GetAlreadySorted();
                for (ui32 idx = alreadyFetched - Min<ui32>(alreadyFetched, ConflictingConstructors.size());
                     idx < ordered.size() && request->GetSize() < twiceInFlightCurrentLimit; ++idx) {
                    request->AddPortion(ordered[idx].GetPortion());
                }
            }
            YDB_LOG_DEBUG_COMP(NKikimrServices::TX_COLUMNSHARD_SCAN, "",
                {"event", "START_FETCH_ACCESSORS"},
                {"accCount", Accessors.GetSize()},
                {"add", request->GetSize()},
                {"inFlight", inFlightCurrentLimit});
            request->SetColumnIds(context->GetAllUsageColumns()->GetColumnIds());
            Accessors.StartRequest(std::move(request), context);
        }
        if (!Accessors.GetSize()) {
            AFL_VERIFY(Accessors.HasRequest());
            YDB_LOG_DEBUG_COMP(NKikimrServices::TX_COLUMNSHARD_SCAN, "",
                {"event", "SKIP_NO_ACCESSORS"},
                {"hasRequest", Accessors.HasRequest()},
                {"inFlight", inFlightCurrentLimit});
            return nullptr;
        }
        return DoExtractNextImpl(context);
    }

public:
    template <typename F>
    void ForEachConstructor(F&& f) const {
        Constructors.ForEachObject(std::forward<F>(f));
    }

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
            , Accessor(std::move(acc))
        {
        }

        TConstructor& MutableObject() {
            return Object;
        }
    };

    TObjectWithAccessor PopObjectWithAccessor() {
        auto object = [&]() {
            if (ConflictingConstructors.empty()) {
                return Constructors.PopFront();
            }
            auto conflicting = std::move(ConflictingConstructors.front());
            ConflictingConstructors.pop_front();
            return conflicting;
        }();
        auto acc = Accessors.ExtractAccessorVerified(object.GetPortion()->GetPortionId());
        TObjectWithAccessor result(std::move(object), std::move(acc));
        return result;
    }

    TSourcesConstructorWithAccessors(const ESourcesSorting sourcesSorting)
        : Constructors(sourcesSorting)
    {
    }

    void InitializeConstructors(std::deque<TConstructor>&& objects) {
        std::deque<TConstructor> ordered;
        for (auto&& object : objects) {
            if (object.IsConflicting()) {
                object.SetIndex(NextConflictingIdx--);
                ConflictingConstructors.emplace_back(std::move(object));
            } else {
                ordered.emplace_back(std::move(object));
            }
        }
        Constructors.Initialize(std::move(ordered));
    }

    const std::deque<TConstructor>& GetConflictingConstructors() const {
        return ConflictingConstructors;
    }
};
}   // namespace NKikimr::NOlap::NReader::NCommon
