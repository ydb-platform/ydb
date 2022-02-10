#include "string_pool.h"

namespace NMonitoring {
    ////////////////////////////////////////////////////////////////////////////////
    // TStringPoolBuilder
    ////////////////////////////////////////////////////////////////////////////////
    const TStringPoolBuilder::TValue* TStringPoolBuilder::PutIfAbsent(TStringBuf str) {
        Y_ENSURE(!IsBuilt_, "Cannot add more values after string has been built");

        auto [it, isInserted] = StrMap_.try_emplace(str, Max<ui32>(), 0);
        if (isInserted) {
            BytesSize_ += str.size();
            it->second.Index = StrVector_.size();
            StrVector_.emplace_back(it->first, &it->second);
        }

        TValue* value = &it->second;
        ++value->Frequency;
        return value;
    }

    const TStringPoolBuilder::TValue* TStringPoolBuilder::GetByIndex(ui32 index) const {
        return StrVector_.at(index).second;
    }

    TStringPoolBuilder& TStringPoolBuilder::Build() {
        if (RequiresSorting_) {
            // sort in reversed order
            std::sort(StrVector_.begin(), StrVector_.end(), [](auto& a, auto& b) {
                return a.second->Frequency > b.second->Frequency;
            });

            ui32 i = 0;
            for (auto& value : StrVector_) {
                value.second->Index = i++;
            }
        }

        IsBuilt_ = true;

        return *this;
    }

    ////////////////////////////////////////////////////////////////////////////////
    // TStringPool
    ////////////////////////////////////////////////////////////////////////////////
    void TStringPool::InitIndex(const char* data, ui32 size) {
        const char* begin = data;
        const char* end = begin + size;
        for (const char* p = begin; p != end; ++p) {
            if (*p == '\0') {
                Index_.push_back(TStringBuf(begin, p));
                begin = p + 1;
            }
        }
    }

}
