#pragma once

#include <util/generic/buffer.h>

namespace NKiwiAggr {
    class IMerger {
    private:
        bool IsMerged;
        ui32 AutoMergeInterval; // Call Merge() after each AutoMergeInterval calls of Add(); zero means no autoMerge
        ui32 NotMergedCount;

    public:
        IMerger(ui32 autoMergeInterval = 0)
            : IsMerged(true)
            , AutoMergeInterval(autoMergeInterval)
            , NotMergedCount(0)
        {
        }

        virtual ~IMerger() {
        }

        // returns true if something is added
        virtual bool Add(const void* data, size_t size) {
            if (AddImpl(data, size)) {
                AutoMerge();
                return true;
            }
            return false;
        }

        virtual void Merge() {
            if (!IsMerged) {
                MergeImpl();
                IsMerged = true;
            }
        }

        virtual void Reset() {
            ResetImpl();
            IsMerged = true;
        }

        // You can add some more result-getters if you want.
        // Do not forget to call Merge() in the beginning of each merger.
        virtual void GetResult(TBuffer& buffer) = 0;

    protected:
        // AutoMerge() is called in Add() after each AddImpl()
        void AutoMerge() {
            IsMerged = false;
            if (AutoMergeInterval) {
                ++NotMergedCount;
                if (NotMergedCount >= AutoMergeInterval) {
                    MergeImpl();
                    IsMerged = true;
                    NotMergedCount = 0;
                }
            }
        }

        // Implementation of merger: define it in derivatives
        virtual bool AddImpl(const void* data, size_t size) = 0; // returns true if something is added
        virtual void MergeImpl() = 0;
        virtual void ResetImpl() = 0;
    };

}
