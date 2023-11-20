#pragma once

#include "defs.h"

#include <ydb/core/blobstorage/base/ptr.h>

namespace NKikimr {

    /////////////////////////////////////////////////////////////////////////////////////////
    // TSVec - snapshotable vector
    // We can easily take snapshot of this vector to work with asynchronously
    /////////////////////////////////////////////////////////////////////////////////////////
    template <class T>
    class TSVec;

    template <class T>
    class TSVecSnap {
        friend class TSVec<T>;
    public:
        class TIterator {
        public:
            TIterator(const TSVecSnap *snap)
                : Snap(snap)
            {}

            void SeekToFirst() {
                Pos = 0;
            }

            bool Valid() const {
                return Snap && Pos >= 0 && Pos < Snap->NElements;
            }

            void Next() {
                Y_DEBUG_ABORT_UNLESS(Valid());
                ++Pos;
            }

            const std::shared_ptr<T> &Get() const {
                Y_DEBUG_ABORT_UNLESS(Valid());
                return Snap->VecPtr->at(Pos);
            }

        private:
            const TSVecSnap *Snap = nullptr;
            size_t Pos = 0;
        };

        size_t GetSize() const {
            return NElements;
        }

        template <class TPrinter>
        void Output(IOutputStream &str, TPrinter &printer) const {
            TIterator it(this);
            it.SeekToFirst();
            while (it.Valid()) {
                printer(str, it.Get());
                it.Next();
            }
        }

        TSVecSnap() = default;

        void Destroy() {
            VecPtr.reset();
        }

    private:
        TSVecSnap(const std::shared_ptr<TVector<std::shared_ptr<T>>> &vecPtr) {
            VecPtr = vecPtr;
            NElements = vecPtr->size();
        }

        std::shared_ptr<TVector<std::shared_ptr<T>>> VecPtr;
        size_t NElements = 0;
    };

    template <class T>
    class TSVec {
    public:
        TSVec(size_t capacity)
            : VecPtr(std::make_shared<TVector<std::shared_ptr<T>>>())
        {
            VecPtr->reserve(capacity);
        }

        void Add(const std::shared_ptr<T> &t) {
            ResizeIfRequired();
            SizeApprox += t->SizeApproximation();
            VecPtr->push_back(t);
        }

        void Add(std::shared_ptr<T> &&t) {
            ResizeIfRequired();
            SizeApprox += t->SizeApproximation();
            VecPtr->push_back(std::move(t));
        }

        void RemoveFirstElements(size_t n) {
            auto rebuildVecPtr = std::make_shared<TVector<std::shared_ptr<T>>>();
            rebuildVecPtr->reserve(VecPtr->capacity());
            ui64 sizeApprox = 0;
            for (size_t i = n, size = VecPtr->size(); i < size; ++i) {
                sizeApprox += VecPtr->at(i)->SizeApproximation();
                rebuildVecPtr->push_back(VecPtr->at(i));
            }
            VecPtr.swap(rebuildVecPtr);
            SizeApprox = sizeApprox;
        }

        TSVecSnap<T> GetSnapshot() const {
            return TSVecSnap(VecPtr);
        }

        size_t GetVecSize() const {
            return VecPtr->size();
        }

        ui64 SizeApproximation() const {
            return SizeApprox;
        }

    private:
        std::shared_ptr<TVector<std::shared_ptr<T>>> VecPtr;
        ui64 SizeApprox = 0;

        void ResizeIfRequired() {
            if (VecPtr->size() == VecPtr->capacity()) {
                // rebuild vec with extended capacity
                auto rebuildVecPtr = std::make_shared<TVector<std::shared_ptr<T>>>();
                rebuildVecPtr->reserve(VecPtr->capacity() * 2);
                for (const auto &x : *VecPtr) {
                    rebuildVecPtr->push_back(x);
                }
                VecPtr.swap(rebuildVecPtr);
            }
        }
    };


    /////////////////////////////////////////////////////////////////////////////////////////
    // TSTree - snapshotable tree
    // We can easily take snapshot of this leveled tree to work with asynchronously
    /////////////////////////////////////////////////////////////////////////////////////////
    template <class T, class TCtx>
    class TSTree;

    template <class T, class TCtx>
    class TSTreeSnap {
    private:
        using TOneLevel = TVector<std::shared_ptr<T>>;
        using TOneLevelPtr = std::shared_ptr<TOneLevel>;
        using TLevels = TVector<TOneLevelPtr>;
        using TLevelsPtr = std::shared_ptr<TLevels>;
        friend class TSTree<T, TCtx>;

        TSTreeSnap(const TCtx &ctx, const TSVecSnap<T> &staging, const TLevelsPtr &levelsPtr)
            : Ctx(ctx)
            , Staging(staging)
            , LevelsPtr(levelsPtr)
        {}

        // TLevelsIterator -- iterates over levels (not staging), returns std::shared_ptr<T> on each iteration
        class TLevelsIterator {
        public:
            TLevelsIterator(TLevels *levels)
                : Levels(levels)
            {
                if (Levels) {
                    ViaLevelsIt = Levels->end();
                }
            }

            void SeekToFirst() {
                if (Levels) {
                    ViaLevelsIt = Levels->begin();
                    Position();
                }
            }

            bool Valid() const {
                return Levels && ViaLevelsIt != Levels->end();
            }

            void Next() {
                Y_DEBUG_ABORT_UNLESS(Valid());
                ++IntraLevelIt;
                if (IntraLevelIt == (*ViaLevelsIt)->end()) {
                    ++ViaLevelsIt;
                    Position();
                }
            }

            const std::shared_ptr<T> &Get() const {
                Y_DEBUG_ABORT_UNLESS(Valid());
                return *IntraLevelIt;
            }

            size_t GetLevel() const {
                return ViaLevelsIt - Levels->begin();
            }

        private:
            TLevels *Levels;
            typename TLevels::iterator ViaLevelsIt;
            typename TOneLevel::iterator IntraLevelIt;

            void Position() {
                while (ViaLevelsIt != Levels->end()) {
                    if (ViaLevelsIt->get() && !(*ViaLevelsIt)->empty()) {
                        IntraLevelIt = (*ViaLevelsIt)->begin();
                        break;
                    } else {
                        ++ViaLevelsIt;
                    }
                }
            }
        };

    public:
        TSTreeSnap() = default;

        // TIterator -- iterates over levels _and_ staging, returns std::shared_ptr<T> on each iteration
        class TIterator {
        public:
            TIterator(const TSTreeSnap *snap)
                : Snap(snap)
                , StagingIt(Snap ? &Snap->Staging : nullptr)
                , LevelsIt(Snap ? Snap->LevelsPtr.get() : nullptr)
            {}

            void SeekToFirst() {
                StagingIt.SeekToFirst();
                LevelsIt.SeekToFirst();
            }

            bool Valid() const {
                return Snap && (StagingIt.Valid() || LevelsIt.Valid());
            }

            void Next() {
                Y_DEBUG_ABORT_UNLESS(Valid());
                if (StagingIt.Valid()) {
                    StagingIt.Next();
                } else {
                    LevelsIt.Next();
                }
            }

            const std::shared_ptr<T> &Get() const {
                Y_DEBUG_ABORT_UNLESS(Valid());
                if (StagingIt.Valid()) {
                    return StagingIt.Get();
                } else {
                    return LevelsIt.Get();
                }
            }

        private:
            const TSTreeSnap *Snap;
            typename TSVecSnap<T>::TIterator StagingIt;
            TLevelsIterator LevelsIt;
        };

        template <class TPrinter>
        void Output(IOutputStream &str, TPrinter &printer) const {
            str << "Staging: ";
            Staging.Output(str, printer);
            str << "\n";
            TLevelsIterator it(LevelsPtr.get());
            it.SeekToFirst();
            while (it.Valid()) {
                str << "Level# " << it.GetLevel() << " Value# ";
                printer(str, it.Get());
                str << "\n";
                it.Next();
            }
        }

        void Destroy() {
            Staging.Destroy();
            LevelsPtr.reset();
        }

    private:
        TCtx Ctx;
        TSVecSnap<T> Staging;
        TLevelsPtr LevelsPtr;
    };

    /////////////////////////////////////////////////////////////////////////////////////////
    // TSTree - snapshotable tree
    // We can easily take snapshot of this leveled tree to work with asynchronously
    /////////////////////////////////////////////////////////////////////////////////////////
    class ISTreeCompaction {
    public:
        virtual void Work() = 0;
        virtual ~ISTreeCompaction() {}
    };

    struct TSTreeCompactionStat {
        ui64 Runs = 0;
        ui64 RecordsMerged = 0;
        ui64 BytesMerged = 0;
        ui64 MergeCalls = 0;

        TSTreeCompactionStat &operator+=(const TSTreeCompactionStat &s) {
            Runs += s.Runs;
            RecordsMerged += s.RecordsMerged;
            BytesMerged += s.BytesMerged;
            MergeCalls += s.MergeCalls;
            return *this;
        }

        void Output(IOutputStream &str) const {
            str << "[Runs# " << Runs << " MergeCalls# " << MergeCalls
                <<" RecordsMerged# " << RecordsMerged << " BytesMerged# " << BytesMerged << "]";
        }
    };

    template <class T, class TCtx>
    class TSTree {
    public:
        // FIXME: setup default threshold
        TSTree(
                const TCtx &ctx,
                size_t stagingCapacity,
                size_t stagingCompThreshold = 8,
                size_t levelCompThreshold = 8)
            : Ctx(ctx)
            , StagingCompactionThreshold(stagingCompThreshold)
            , LevelCompactionThreshold(levelCompThreshold)
            , Staging(stagingCapacity)
            , LevelsPtr(std::make_shared<TLevels>())
        {}

        void Add(const std::shared_ptr<T> &t) {
            Staging.Add(t);
        }

        ui64 SizeApproximation() const {
            return Staging.SizeApproximation() + LevelsSizeApprox;
        }

        TSTreeSnap<T, TCtx> GetSnapshot() const {
            return TSTreeSnap<T, TCtx>(Ctx, Staging.GetSnapshot(), LevelsPtr);
        }

        std::shared_ptr<ISTreeCompaction> Compact() {
            if (Staging.GetVecSize() > StagingCompactionThreshold) {
                return std::make_shared<TCompactionJob>(Ctx, LevelCompactionThreshold,
                    Staging.GetSnapshot(), LevelsPtr);
            } else {
                return nullptr;
            }
        }

        std::shared_ptr<ISTreeCompaction> ApplyCompactionResult(std::shared_ptr<ISTreeCompaction> cjob) {
            auto *job = dynamic_cast<TCompactionJob*>(cjob.get());
            auto result = job->GetCompactionResult();
            Staging.RemoveFirstElements(result.ClearFirstRecs);
            LevelsPtr = result.LevelsPtrDst;
            LevelsSizeApprox = result.BuiltLevelsSizeApprox;
            CompactionStat += result.CompactionStat;

            return Compact();
        }

        const TSTreeCompactionStat &GetCompactionStat() const {
            return CompactionStat;
        }

    private:
        using TOneLevel = TVector<std::shared_ptr<T>>;
        using TOneLevelPtr = std::shared_ptr<TOneLevel>;
        using TLevels = TVector<TOneLevelPtr>;
        using TLevelsPtr = std::shared_ptr<TLevels>;

        TCtx Ctx;
        const size_t StagingCompactionThreshold;
        const size_t LevelCompactionThreshold;
        TSVec<T> Staging;
        TLevelsPtr LevelsPtr;
        // size approximation for levels only
        ui64 LevelsSizeApprox = 0;
        TSTreeCompactionStat CompactionStat;

        static ui64 CalculateSizeApproximation(const TLevelsPtr &levelsPtr) {
            ui64 sizeApprox = 0;
            for (size_t lev = 0, levSz = levelsPtr->size(); lev < levSz; ++lev) {
                const TOneLevelPtr &oneLevel = levelsPtr->at(lev);
                if (oneLevel) {
                    for (size_t item = 0, itemSz = oneLevel->size(); item < itemSz; ++item) {
                        sizeApprox += oneLevel->at(item)->SizeApproximation();
                    }
                }
            }
            return sizeApprox;
        }

    public:
        class TCompactionJob : public ISTreeCompaction {
        public:
            TCompactionJob(
                    const TCtx &ctx,
                    size_t levelCompThreshold,
                    const TSVecSnap<T> &stagingSource,
                    const TLevelsPtr &levelsPtrSource)
                : Ctx(ctx)
                , LevelCompactionThreshold(levelCompThreshold)
                , StagingSource(stagingSource)
                , LevelsPtrSource(levelsPtrSource)
            {
                ++CompactionResult.CompactionStat.Runs;
            }

            virtual void Work() override {
                std::shared_ptr<T> s = CompactStaging();
                ui64 sizeApprox = 0;
                bool toNextLine = true;

                CompactionResult.LevelsPtrDst = std::make_shared<TLevels>();
                CompactionResult.LevelsPtrDst->reserve(LevelsPtrSource->size() + 1);

                size_t level = 0;
                while (toNextLine) {
                    auto newLevelPtr = std::make_shared<TOneLevel>();
                    if (level < LevelsPtrSource->size() && // handle no vec at level=level
                            LevelsPtrSource->at(level)->size() + 1 > LevelCompactionThreshold) {
                        // compact
                        TVector<std::shared_ptr<T>> input;
                        input = *LevelsPtrSource->at(level);
                        input.push_back(s);
                        // gather stat
                        for (const auto &x : input) {
                            CompactionResult.CompactionStat.RecordsMerged += x->GetSize();
                            CompactionResult.CompactionStat.BytesMerged += x->SizeApproximation();
                        }
                        ++CompactionResult.CompactionStat.MergeCalls;
                        s = T::Merge(Ctx, input);
                    } else {
                        if (level < LevelsPtrSource->size()) {
                            // copy prev state if any
                            newLevelPtr->reserve(LevelsPtrSource->at(level)->size() + 1);
                            *newLevelPtr = *LevelsPtrSource->at(level);
                        }
                        sizeApprox += s->SizeApproximation();
                        newLevelPtr->push_back(s);
                        toNextLine = false;
                    }
                    CompactionResult.LevelsPtrDst->push_back(newLevelPtr);
                    ++level;
                }

                // copy other levels
                for (size_t i = level; i < LevelsPtrSource->size(); ++i) {
                    CompactionResult.LevelsPtrDst->push_back(LevelsPtrSource->at(i));
                }
                // calculate size approximation for built levels
                CompactionResult.BuiltLevelsSizeApprox = CalculateSizeApproximation(CompactionResult.LevelsPtrDst);
            }

            struct TCompactionResult {
                // clear command for snap vec (i.e. staging)
                size_t ClearFirstRecs = 0;
                // size approximation for newly built levels
                ui64 BuiltLevelsSizeApprox = 0;
                // newly built levels
                TLevelsPtr LevelsPtrDst;
                // compaction statistics
                TSTreeCompactionStat CompactionStat;
            };

            const TCompactionResult &GetCompactionResult() const {
                return CompactionResult;
            }

        private:
            // Context
            TCtx Ctx;
            const size_t LevelCompactionThreshold;
            // source for compaction
            TSVecSnap<T> StagingSource;
            const TLevelsPtr LevelsPtrSource;
            TCompactionResult CompactionResult;

            std::shared_ptr<T> CompactStaging() {
                TVector<std::shared_ptr<T>> input;
                CompactionResult.ClearFirstRecs = StagingSource.GetSize();
                input.reserve(CompactionResult.ClearFirstRecs);
                typename TSVecSnap<T>::TIterator it(&StagingSource);
                it.SeekToFirst();
                while (it.Valid()) {
                    CompactionResult.CompactionStat.RecordsMerged += it.Get()->GetSize();
                    CompactionResult.CompactionStat.BytesMerged += it.Get()->SizeApproximation();
                    input.push_back(it.Get());
                    it.Next();
                }

                ++CompactionResult.CompactionStat.MergeCalls;
                return T::Merge(Ctx, input);
            }
        };
    };

} // NKikimr

