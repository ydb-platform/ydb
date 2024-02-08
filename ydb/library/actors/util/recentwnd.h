#pragma once

#include <util/generic/deque.h>

template <typename TElem,
          template <typename, typename...> class TContainer = TDeque>
class TRecentWnd {
public:
    TRecentWnd(ui32 wndSize)
        : MaxWndSize_(wndSize)
    {
    }

    void Push(const TElem& elem) {
        if (Window_.size() == MaxWndSize_)
            Window_.erase(Window_.begin());
        Window_.emplace_back(elem);
    }

    void Push(TElem&& elem) {
        if (Window_.size() == MaxWndSize_)
            Window_.erase(Window_.begin());
        Window_.emplace_back(std::move(elem));
    }

    TElem& Last() {
        return Window_.back();
    }
    const TElem& Last() const {
        return Window_.back();
    }
    bool Full() const {
        return Window_.size() == MaxWndSize_;
    }
    ui64 Size() const {
        return Window_.size();
    }

    using const_iterator = typename TContainer<TElem>::const_iterator;

    const_iterator begin() {
        return Window_.begin();
    }
    const_iterator end() {
        return Window_.end();
    }

    void Reset(ui32 wndSize = 0) {
        Window_.clear();
        if (wndSize != 0) {
            MaxWndSize_ = wndSize;
        }
    }

    void ResetWnd(ui32 wndSize) {
        Y_ABORT_UNLESS(wndSize != 0);
        MaxWndSize_ = wndSize;
        if (Window_.size() > MaxWndSize_) {
            Window_.erase(Window_.begin(),
                          Window_.begin() + Window_.size() - MaxWndSize_);
        }
    }

private:
    TContainer<TElem> Window_;
    ui32 MaxWndSize_;
};
