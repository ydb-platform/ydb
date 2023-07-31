#pragma once

#include "memory.h"

#include <util/generic/yexception.h>

namespace NReverseGeocoder {
    class TBlockAllocator: public TNonCopyable {
    public:
        TBlockAllocator()
            : Data_(nullptr)
            , BytesAllocated_(0)
            , BytesLimit_(0)
        {
        }

        TBlockAllocator(void* data, size_t bytesLimit)
            : Data_(data)
            , BytesAllocated_(0)
            , BytesLimit_(bytesLimit)
        {
        }

        TBlockAllocator(TBlockAllocator&& a)
            : TBlockAllocator()
        {
            DoSwap(Data_, a.Data_);
            DoSwap(BytesAllocated_, a.BytesAllocated_);
            DoSwap(BytesLimit_, a.BytesLimit_);
        }

        TBlockAllocator& operator=(TBlockAllocator&& a) {
            DoSwap(Data_, a.Data_);
            DoSwap(BytesAllocated_, a.BytesAllocated_);
            DoSwap(BytesLimit_, a.BytesLimit_);
            return *this;
        }

        virtual ~TBlockAllocator() {
        }

        virtual void* Allocate(size_t number);

        static size_t AllocateSize(size_t number);

        virtual void Deallocate(void* ptr, size_t number);

        size_t TotalAllocatedSize() const {
            return BytesAllocated_;
        }

        void Setup(void* data, size_t bytesLimit) {
            Data_ = data;
            BytesLimit_ = bytesLimit;
            BytesAllocated_ = 0;
        }

    private:
        void* Data_;
        size_t BytesAllocated_;
        size_t BytesLimit_;
    };

}
