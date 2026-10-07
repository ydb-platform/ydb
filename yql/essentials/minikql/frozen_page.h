#pragma once

#include <util/generic/yexception.h>
#include <util/system/yassert.h>

#include <yql/essentials/utils/strong_alias.h>

#include <cstddef>
#include <expected>
#include <utility>

namespace NKikimr {

template <typename TProvider>
class TFrozenPage {
public:
    using TUnlockedPage = NYql::TStrongAlias<class TUnlockedPageTag, void*>;
    using TUnlockFailure = NYql::TStrongAlias<class TUnlockFailureTag, std::pair<TFrozenPage, TSystemError>>;
    using TUnlockResult = std::expected<TUnlockedPage, TUnlockFailure>;

    TFrozenPage() = default;

    [[nodiscard]] static std::expected<TFrozenPage, TSystemError> Freeze(TProvider& provider, void* page, size_t size) {
        Y_ENSURE(page, "Cannot freeze a null page");
        auto result = provider.Freeze(page, size);
        if (!result) {
            return std::unexpected(std::move(result).error());
        }
        return TFrozenPage(provider, page, size);
    }

    TFrozenPage(const TFrozenPage&) = delete;
    TFrozenPage& operator=(const TFrozenPage&) = delete;

    TFrozenPage(TFrozenPage&& other) noexcept
        : Provider_(std::exchange(other.Provider_, nullptr))
        , Page_(std::exchange(other.Page_, nullptr))
        , Size_(std::exchange(other.Size_, 0))
    {
    }

    TFrozenPage& operator=(TFrozenPage&& other) noexcept {
        if (this != &other) {
            Reset();
            Provider_ = std::exchange(other.Provider_, nullptr);
            Page_ = std::exchange(other.Page_, nullptr);
            Size_ = std::exchange(other.Size_, 0);
        }
        return *this;
    }

    ~TFrozenPage() {
        Reset();
    }

    [[nodiscard]] TUnlockResult Unlock() && {
        Y_ENSURE(Page_, "Cannot unlock an empty frozen page");
        auto result = Provider_->Unfreeze(Page_, Size_);
        if (!result) {
            return std::unexpected(TUnlockFailure(std::pair{std::move(*this), std::move(result).error()}));
        }
        Provider_ = nullptr;
        Size_ = 0;
        return TUnlockedPage(std::exchange(Page_, nullptr));
    }

private:
    TFrozenPage(TProvider& provider, void* page, size_t size) noexcept
        : Provider_(&provider)
        , Page_(page)
        , Size_(size)
    {
    }

    void Reset() noexcept {
        if (Page_) {
            auto result = Provider_->Munmap(Page_, Size_, /*frozen=*/true);
            Y_DEBUG_ABORT_UNLESS(result, "%s", result.error().what());
        }
        Provider_ = nullptr;
        Page_ = nullptr;
        Size_ = 0;
    }

    TProvider* Provider_ = nullptr;
    void* Page_ = nullptr;
    size_t Size_ = 0;
};

} // namespace NKikimr
