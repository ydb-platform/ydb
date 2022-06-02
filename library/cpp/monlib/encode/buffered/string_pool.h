#pragma once

#include <util/generic/hash.h>
#include <util/generic/vector.h>

namespace NMonitoring {
    ////////////////////////////////////////////////////////////////////////////////
    // TStringPoolBuilder
    ////////////////////////////////////////////////////////////////////////////////
    class TStringPoolBuilder {
    public:
        struct TValue: TNonCopyable {
            TValue(ui32 idx, ui32 freq)
                : Index{idx}
                , Frequency{freq}
            {
            }

            ui32 Index;
            ui32 Frequency;
        };

    public:
        const TValue* PutIfAbsent(TStringBuf str);
        const TValue* GetByIndex(ui32 index) const;

        /// Determines whether pool must be sorted by value frequencies
        TStringPoolBuilder& SetSorted(bool sorted) {
            RequiresSorting_ = sorted;
            return *this;
        }

        TStringPoolBuilder& Build();

        TStringBuf Get(ui32 index) const {
            if (RequiresSorting_) {
                Y_ENSURE(IsBuilt_, "Pool must be sorted first");
            }

            return StrVector_.at(index).first;
        }

        TStringBuf Get(const TValue* value) const {
            return StrVector_.at(value->Index).first;
        }

        template <typename TConsumer>
        void ForEach(TConsumer&& c) {
            Y_ENSURE(IsBuilt_, "Pool must be built first");

            for (const auto& value : StrVector_) {
                c(value.first, value.second->Index, value.second->Frequency);
            }
        }

        size_t BytesSize() const noexcept {
            return BytesSize_;
        }

        size_t Count() const noexcept {
            return StrMap_.size();
        }

    private:
        THashMap<TString, TValue> StrMap_;
        TVector<std::pair<TStringBuf, TValue*>> StrVector_;
        bool RequiresSorting_ = false;
        bool IsBuilt_ = false;
        size_t BytesSize_ = 0;
    };

    ////////////////////////////////////////////////////////////////////////////////
    // TStringPool
    ////////////////////////////////////////////////////////////////////////////////
    class TStringPool {
    public:
        TStringPool(const char* data, ui32 size) {
            InitIndex(data, size);
        }

        TStringBuf Get(ui32 i) const {
            return Index_.at(i);
        }

        size_t Size() const {
            return Index_.size();
        }

        size_t SizeBytes() const {
            return Index_.capacity() * sizeof(TStringBuf);
        }

    private:
        void InitIndex(const char* data, ui32 size);

    private:
        TVector<TStringBuf> Index_;
    };

}
