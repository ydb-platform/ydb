#pragma once

#include <library/cpp/containers/stack_vector/stack_vec.h>

#include <util/generic/yexception.h>
#include <util/system/compiler.h>
#include <util/system/sys_alloc.h>
#include <util/system/types.h>
#include <util/system/yassert.h>

#include <algorithm>
#include <bit>
#include <cstddef>
#include <initializer_list>
#include <iterator>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <span>
#include <type_traits>
#include <utility>

namespace NKikimr::NKqp::NPrivate {

namespace NIndexedWords {

struct TWord {
    ui32 Index = 0;
    ui64 Bits = 0;
};

static_assert(sizeof(TWord) == 2 * sizeof(ui64));
static_assert(offsetof(TWord, Index) == 0);
static_assert(offsetof(TWord, Bits) == sizeof(ui64));
static_assert(std::is_trivially_copyable_v<TWord>);
static_assert(std::is_trivially_destructible_v<TWord>);

template <ui32 Delta>
struct TConstantDeltaIndices {
    static_assert(Delta > 0);

    ui32 Base = 0;

    Y_FORCE_INLINE ui32 Get(size_t position) const {
        return Base + Delta * static_cast<ui32>(position);
    }

    Y_FORCE_INLINE std::optional<size_t> Find(ui32 index, size_t size) const {
        if (index < Base) {
            return std::nullopt;
        }
        const ui32 offset = index - Base;
        if (offset % Delta) {
            return std::nullopt;
        }
        const size_t position = static_cast<size_t>(offset / Delta);
        return position < size ? std::optional<size_t>(position) : std::nullopt;
    }
};

template <ui32 BaseBits, ui32 DeltaBits, size_t DeltaCount>
struct TPackedBaseDeltas {
    static_assert(BaseBits > 0 && DeltaBits > 0);
    static_assert(BaseBits <= 32 && DeltaBits <= 32);
    static_assert(BaseBits + DeltaCount * DeltaBits <= 64);

    static constexpr ui64 MakeMask(ui32 bits) {
        return bits == 64 ? std::numeric_limits<ui64>::max() : (ui64{1} << bits) - 1;
    }

    static constexpr ui64 BaseMask = MakeMask(BaseBits);
    static constexpr ui64 DeltaMask = MakeMask(DeltaBits);

    static_assert(DeltaCount == 0
        || BaseMask + DeltaMask <= std::numeric_limits<ui32>::max());

    ui64 Meta = 0;

    static constexpr ui32 DeltaShift(size_t position) {
        return BaseBits + static_cast<ui32>((position - 1) * DeltaBits);
    }

    static ui64 Encode(std::span<const TWord> words) {
        Y_ASSERT(!words.empty() && words.size() <= DeltaCount + 1);
        const ui32 base = words.front().Index;
        Y_ASSERT(base <= BaseMask);
        ui64 meta = base;
        for (size_t position = 1; position < words.size(); ++position) {
            const ui32 index = words[position].Index;
            Y_ASSERT(index > base && static_cast<ui64>(index) - base <= DeltaMask);
            meta |= static_cast<ui64>(index - base) << DeltaShift(position);
        }
        return meta;
    }

    Y_FORCE_INLINE ui32 Get(size_t position) const {
        Y_ASSERT(position <= DeltaCount);
        const ui32 base = static_cast<ui32>(Meta & BaseMask);
        if (!position) {
            return base;
        }
        return base + static_cast<ui32>((Meta >> DeltaShift(position)) & DeltaMask);
    }

    Y_FORCE_INLINE std::optional<size_t> Find(ui32 index, size_t size) const {
        Y_ASSERT(size <= DeltaCount + 1);
        for (size_t position = 0; position < size; ++position) {
            const ui32 current = Get(position);
            if (current >= index) {
                return current == index ? std::optional<size_t>(position) : std::nullopt;
            }
        }
        return std::nullopt;
    }
};

template <typename TMask, typename TIndices>
struct TView {
    static_assert(std::is_same_v<std::remove_const_t<TMask>, ui64>);

    // TIndices must produce strictly increasing, in-domain indices for every
    // position exposed by Masks and Find() must describe the same sequence.
    std::span<TMask> Masks;
    TIndices Indices;

    Y_FORCE_INLINE size_t PhysicalSize() const {
        return Masks.size();
    }

    Y_FORCE_INLINE TWord Word(size_t position) const {
        Y_ASSERT(position < Masks.size());
        return {Indices.Get(position), Masks[position]};
    }

    Y_FORCE_INLINE TMask* FindMask(ui32 index) const {
        const auto position = Indices.Find(index, Masks.size());
        return position ? std::addressof(Masks[*position]) : nullptr;
    }
};

template <typename TStoredWord>
struct TStoredWordView {
    static_assert(std::is_same_v<std::remove_const_t<TStoredWord>, TWord>);
    using TMask = std::conditional_t<std::is_const_v<TStoredWord>, const ui64, ui64>;

    // Words must be nonzero, strictly ordered by Index, and in-domain.
    std::span<TStoredWord> Words;

    Y_FORCE_INLINE size_t PhysicalSize() const {
        return Words.size();
    }

    Y_FORCE_INLINE TWord Word(size_t position) const {
        Y_ASSERT(position < Words.size());
        return Words[position];
    }

    // Insertion can reuse the search, with an O(1) path for increasing indices.
    Y_FORCE_INLINE TMask* FindMask(ui32 index, size_t* insertionPosition = nullptr) const {
        const auto it = insertionPosition && (Words.empty() || Words.back().Index < index)
            ? Words.end()
            : std::lower_bound(Words.begin(), Words.end(), index,
                [](const TWord& word, ui32 index) { return word.Index < index; });
        if (insertionPosition) {
            *insertionPosition = it - Words.begin();
        }
        return it != Words.end() && it->Index == index ? std::addressof(it->Bits) : nullptr;
    }
};

template <typename TView>
Y_FORCE_INLINE bool NextNonzeroWord(const TView& view, size_t& position, TWord& result) {
    while (position < view.PhysicalSize()) {
        const auto word = view.Word(position++);
        if (word.Bits) {
            result = word;
            return true;
        }
    }
    return false;
}

template <typename TView>
class TCursor {
public:
    explicit TCursor(TView view)
        : View_(view)
    {
        Next();
    }

    Y_FORCE_INLINE bool Valid() const {
        return Valid_;
    }

    Y_FORCE_INLINE const TWord& Get() const {
        return Current_;
    }

    Y_FORCE_INLINE void Next() {
        Valid_ = NextNonzeroWord(View_, Position_, Current_);
    }

private:
    TView View_;
    size_t Position_ = 0;
    TWord Current_;
    bool Valid_ = false;
};

enum class EMerge : ui8 {
    Union,
    Subtract,
    Intersect,
};

enum class EMergeMode : ui8 {
    Preview,
    Apply,
};

struct TDenseMergeResult {
    size_t NonzeroWords;
    bool Changed;
};

// Contiguous topologies share a word-wise kernel. Union requires a left window
// containing the right one. A read-only preview lets shrinking operations check
// their destination topology before making any changes that could need allocation.
template <EMerge Op, EMergeMode Mode>
Y_FORCE_INLINE TDenseMergeResult MergeDense(
    TView<ui64, TConstantDeltaIndices<1>> left,
    TView<const ui64, TConstantDeltaIndices<1>> right,
    size_t nonzeroWords)
{
    const size_t leftBase = left.Indices.Base;
    const size_t leftEnd = leftBase + left.Masks.size();
    const size_t rightBase = right.Indices.Base;
    const size_t rightEnd = rightBase + right.Masks.size();
    if constexpr (Op == EMerge::Union) {
        Y_ASSERT(leftBase <= rightBase && rightEnd <= leftEnd);
    }
    const size_t begin = std::clamp(rightBase, leftBase, leftEnd) - leftBase;
    const size_t end = std::clamp(rightEnd, leftBase, leftEnd) - leftBase;
    size_t wordChanges = 0;
    ui64 changedBits = 0;
    if (begin < end) {
        const auto* rhs = right.Masks.data() + (leftBase + begin - rightBase);
        for (size_t i = begin; i < end; ++i) {
            const ui64 oldBits = left.Masks[i];
            ui64 bits;
            if constexpr (Op == EMerge::Union) {
                bits = oldBits | rhs[i - begin];
                wordChanges += !oldBits && bits;
            } else {
                if constexpr (Op == EMerge::Subtract) {
                    bits = oldBits & ~rhs[i - begin];
                } else {
                    bits = oldBits & rhs[i - begin];
                }
                wordChanges += oldBits && !bits;
            }
            changedBits |= oldBits ^ bits;
            if constexpr (Mode == EMergeMode::Apply) {
                left.Masks[i] = bits;
            }
        }
    }
    if constexpr (Op == EMerge::Intersect) {
        const auto clear = [&](size_t first, size_t last) {
            for (size_t i = first; i < last; ++i) {
                wordChanges += left.Masks[i] != 0;
                changedBits |= left.Masks[i];
                if constexpr (Mode == EMergeMode::Apply) {
                    left.Masks[i] = 0;
                }
            }
        };
        clear(0, begin);
        clear(end, left.Masks.size());
    }
    return {
        Op == EMerge::Union ? nonzeroWords + wordChanges : nonzeroWords - wordChanges,
        changedBits != 0,
    };
}

template <EMerge Op, typename TLeftView, typename TRightView, typename TSink>
bool Merge(TLeftView leftView, TRightView rightView, TSink&& sink) {
    TCursor left(leftView);
    TCursor right(rightView);
    bool changed = false;

    while (left.Valid() && right.Valid()) {
        if (left.Get().Index < right.Get().Index) {
            if constexpr (Op != EMerge::Intersect) {
                sink(left.Get());
            } else {
                changed = true;
            }
            left.Next();
        } else if (right.Get().Index < left.Get().Index) {
            if constexpr (Op == EMerge::Union) {
                sink(right.Get());
                changed = true;
            }
            right.Next();
        } else {
            ui64 bits;
            if constexpr (Op == EMerge::Union) {
                bits = left.Get().Bits | right.Get().Bits;
            } else if constexpr (Op == EMerge::Subtract) {
                bits = left.Get().Bits & ~right.Get().Bits;
            } else {
                bits = left.Get().Bits & right.Get().Bits;
            }
            changed |= bits != left.Get().Bits;
            if (bits) {
                sink(TWord{left.Get().Index, bits});
            }
            left.Next();
            right.Next();
        }
    }

    if constexpr (Op != EMerge::Intersect) {
        while (left.Valid()) {
            sink(left.Get());
            left.Next();
        }
    } else {
        changed |= left.Valid();
    }

    if constexpr (Op == EMerge::Union) {
        while (right.Valid()) {
            sink(right.Get());
            changed = true;
            right.Next();
        }
    }
    return changed;
}

template <typename TLeftView, typename TRightView>
bool HasAny(TLeftView leftView, TRightView rightView) {
    TCursor left(leftView);
    TCursor right(rightView);
    while (left.Valid() && right.Valid()) {
        if (left.Get().Index < right.Get().Index) {
            left.Next();
        } else if (right.Get().Index < left.Get().Index) {
            right.Next();
        } else {
            if (left.Get().Bits & right.Get().Bits) {
                return true;
            }
            left.Next();
            right.Next();
        }
    }
    return false;
}

template <typename TLeftView, typename TRightView>
bool IsSubset(TLeftView leftView, TRightView rightView) {
    TCursor left(leftView);
    TCursor right(rightView);
    while (left.Valid()) {
        while (right.Valid() && right.Get().Index < left.Get().Index) {
            right.Next();
        }
        if (!right.Valid() || right.Get().Index != left.Get().Index
            || (left.Get().Bits & ~right.Get().Bits))
        {
            return false;
        }
        left.Next();
    }
    return true;
}

template <typename TLeftView, typename TRightView>
bool Equal(TLeftView leftView, TRightView rightView) {
    TCursor left(leftView);
    TCursor right(rightView);
    while (left.Valid()) {
        if (!right.Valid() || left.Get().Index != right.Get().Index
            || left.Get().Bits != right.Get().Bits)
        {
            return false;
        }
        left.Next();
        right.Next();
    }
    return !right.Valid();
}

} // namespace NIndexedWords

/**
 * A 32-byte adaptive set over the full valid ui32-ID domain.
 *
 * Three payload words are either a movable dense window or three sparse word
 * masks whose indices are encoded as a base and two 18-bit deltas. Larger
 * sets use a common pointer/size/capacity heap handle, interpreted either as
 * a dense word span or as sorted sparse {index, mask} entries. Dense heap is
 * built at a 3:1 span-to-nonzero-word ratio and retained at 4:1.
 *
 * Meta[63:62] is the kind: 00 dense-inline, 01 sparse-inline,
 * 10 dense-heap, 11 sparse-heap. The lower bits are respectively
 * {base:26, span:2}, {base:26, delta1:18, delta2:18},
 * {base:26, nonzero-word-count:27}, and zero.
 *
 * Invariants at public operation boundaries:
 * - Meta_ == 0 is the unique empty state: dense-inline with zero payload.
 * - The kind matches the active union member. A heap buffer has one owner and
 *   0 < Size <= Capacity; Size counts dense masks or sparse {index, mask} entries.
 * - Word indices strictly increase; all represented IDs are below InvalidBit.
 *   Dense windows may contain holes, but their first/last masks are nonzero;
 *   sparse entries are all nonzero. Unused inline masks and metadata bits are zero.
 * - Dense-heap metadata counts nonzero masks, not set bits or the physical span.
 *
 * Mutation rules:
 * - Complete throwing preparation before overwriting live storage. Installation
 *   must preserve old ownership until its replacement is ready.
 * - A declined fast path leaves the original set unchanged for the fallback.
 *   Successful mutations restore counts and window boundaries before returning.
 * - Views borrow storage; do not reuse them after replacement or rebasing.
 *
 * Contiguous masks are exposed as NIndexedWords::TView<TMask, TIndices>;
 * sparse heap entries use NIndexedWords::TStoredWordView. One templated
 * cursor, lookup, merge, overlap, subset, and equality layer serves all four
 * layouts. Dense inline and heap also share an in-place word-wise merge kernel;
 * representation transitions continue through the common installation path.
 */
class TAdaptiveBitSetStorage {
public:
    enum class EStorageKind : ui8 {
        DenseInline = 0,
        SparseInline = 1,
        DenseHeap = 2,
        SparseHeap = 3,
    };

private:
    static constexpr size_t InlinePayloadWords = 3;
    using TWord = NIndexedWords::TWord;

    struct TInlineStorage {
        ui64 Words[InlinePayloadWords] = {};
    };

    static_assert(sizeof(TInlineStorage) == 3 * sizeof(ui64));

    struct THeapStorage {
        void* Data = nullptr;
        ui32 Size = 0;
        ui32 Capacity = 0;
    };

    static_assert(sizeof(THeapStorage) == 2 * sizeof(ui64));
    static_assert(offsetof(THeapStorage, Data) == 0);
    static_assert(offsetof(THeapStorage, Size) == sizeof(void*));
    static_assert(offsetof(THeapStorage, Capacity) == sizeof(void*) + sizeof(ui32));
    static_assert(std::is_trivially_copyable_v<THeapStorage>);
    static_assert(std::is_trivially_destructible_v<THeapStorage>);

    union TStorage {
        TInlineStorage Inline;
        THeapStorage Heap;

        TStorage()
            : Inline()
        {
        }
    };

    static_assert(sizeof(TStorage) == 3 * sizeof(ui64));
    static_assert(std::is_trivially_copyable_v<TStorage>);
    static_assert(std::is_trivially_destructible_v<TStorage>);

    static constexpr size_t MergeInlineWordCapacity = 8;

    using TWordVector = TStackVec<TWord, MergeInlineWordCapacity>;
    using TWordSpan = std::span<const TWord>;

public:
    static constexpr ui32 InvalidBit = std::numeric_limits<ui32>::max();

    class const_iterator {
    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = ui32;
        using difference_type = std::ptrdiff_t;
        using pointer = void;
        using reference = ui32;

        const_iterator() = default;

        ui32 operator*() const {
            return Position_;
        }

        const_iterator& operator++() {
            if (RemainingBits_) {
                SelectNextBit();
            } else {
                LoadNextWord();
            }
            return *this;
        }

        const_iterator operator++(int) {
            auto copy = *this;
            ++*this;
            return copy;
        }

        bool operator==(const const_iterator& other) const {
            return Set_ == other.Set_ && Position_ == other.Position_;
        }

    private:
        friend class TAdaptiveBitSetStorage;

        const_iterator(const TAdaptiveBitSetStorage* set, bool atEnd)
            : Set_(set)
        {
            if (!atEnd) {
                LoadNextWord();
            }
        }

        void LoadNextWord() {
            TWord word;
            if (Set_->NextNonzeroWord(WordPosition_, word)) {
                WordIndex_ = word.Index;
                RemainingBits_ = word.Bits;
                SelectNextBit();
            } else {
                Position_ = InvalidBit;
            }
        }

        void SelectNextBit() {
            Position_ = (WordIndex_ << WordShift) + std::countr_zero(RemainingBits_);
            RemainingBits_ &= RemainingBits_ - 1;
        }

        const TAdaptiveBitSetStorage* Set_ = nullptr;
        size_t WordPosition_ = 0;
        ui64 RemainingBits_ = 0;
        ui32 WordIndex_ = 0;
        ui32 Position_ = InvalidBit;
    };

    TAdaptiveBitSetStorage() = default;

    TAdaptiveBitSetStorage(std::initializer_list<ui32> ids) {
        Assign(ids);
    }

    TAdaptiveBitSetStorage(const TAdaptiveBitSetStorage& other) {
        CopyFrom(other);
    }

    TAdaptiveBitSetStorage& operator=(const TAdaptiveBitSetStorage& other) {
        if (this != &other) {
            CopyFrom(other);
        }
        return *this;
    }

    TAdaptiveBitSetStorage(TAdaptiveBitSetStorage&& other) noexcept {
        MoveFrom(other);
    }

    TAdaptiveBitSetStorage& operator=(TAdaptiveBitSetStorage&& other) noexcept {
        if (this != &other) {
            MoveFrom(other);
        }
        return *this;
    }

    ~TAdaptiveBitSetStorage() {
        DestroyHeap();
    }

    Y_FORCE_INLINE bool Add(ui32 bit) {
        Y_ENSURE(bit != InvalidBit);
        const ui32 wordIndex = bit >> WordShift;
        const ui64 mask = ui64{1} << (bit & WordMask);
        if (Y_UNLIKELY(Empty())) {
            const TWord word{wordIndex, mask};
            AssignWords(TWordSpan(&word, 1));
            return true;
        }

        size_t sparsePosition = 0;
        if (auto* word = FindMask(*this, wordIndex, &sparsePosition)) {
            const ui64 oldBits = *word;
            if (oldBits & mask) {
                return false;
            }
            *word = oldBits | mask;
            if (!oldBits && StorageKind() == EStorageKind::DenseHeap) {
                SetDenseHeapNonzeroWords(DenseHeapNonzeroWords() + 1);
            }
            return true;
        }

        if (TryInsertHeapWord({wordIndex, mask}, sparsePosition)) {
            return true;
        }

        TWordVector words;
        CollectWords(words);
        const auto position = std::lower_bound(words.begin(), words.end(), wordIndex,
            [](const TWord& word, ui32 index) { return word.Index < index; });
        words.insert(position, {wordIndex, mask});
        AssignWords(words, StorageKind() == EStorageKind::DenseHeap);
        return true;
    }

    // Coalesce adjacent masks while reading; ordered word indices need no sort.
    // Input may be unordered, contain duplicates, or refer to this set.
    template <typename TRange>
    void Assign(const TRange& ids) {
        TWordVector words;
        bool ordered = true;
        for (const ui32 bit : ids) {
            Y_ENSURE(bit != InvalidBit);
            const TWord word{bit >> WordShift, ui64{1} << (bit & WordMask)};
            if (!words.empty()) {
                if (words.back().Index == word.Index) {
                    words.back().Bits |= word.Bits;
                    continue;
                }
                ordered &= words.back().Index < word.Index;
            }
            words.push_back(word);
        }
        if (!ordered) {
            std::sort(words.begin(), words.end(),
                [](const TWord& lhs, const TWord& rhs) { return lhs.Index < rhs.Index; });
            size_t count = 0;
            for (const auto word : words) {
                if (count && words[count - 1].Index == word.Index) {
                    words[count - 1].Bits |= word.Bits;
                } else {
                    words[count++] = word;
                }
            }
            words.resize(count);
        }
        AssignWords(words);
    }

    bool Remove(ui32 bit) {
        if (bit == InvalidBit) {
            return false;
        }
        const ui32 wordIndex = bit >> WordShift;
        const ui64 mask = ui64{1} << (bit & WordMask);

        size_t sparsePosition = 0;
        auto* word = FindMask(*this, wordIndex, &sparsePosition);
        if (!word || !(*word & mask)) {
            return false;
        }
        if (const ui64 remainingBits = *word & ~mask) {
            *word = remainingBits;
            return true;
        }

        if (TryRemoveHeapWord(word, sparsePosition)) {
            return true;
        }

        // Keep the last bit intact until the replacement is ready.
        TWordVector words;
        const size_t remainingWords = NonzeroWordCount() - 1;
        if (remainingWords > MergeInlineWordCapacity) {
            words.reserve(remainingWords);
        }
        ForEachNonzeroWord([&](ui32 index, ui64 bits) {
            if (index != wordIndex) {
                AppendWord(words, {index, bits});
            }
        });
        AssignWords(words, StorageKind() == EStorageKind::DenseHeap);
        return true;
    }

    Y_FORCE_INLINE bool Contains(ui32 bit) const {
        if (bit == InvalidBit) {
            return false;
        }
        const auto* word = FindMask(*this, bit >> WordShift);
        return word && (*word & (ui64{1} << (bit & WordMask)));
    }

    bool Empty() const {
        return Meta_ == 0;
    }

    size_t Size() const {
        size_t result = 0;
        ForEachNonzeroWord([&](ui32, ui64 bits) {
            result += std::popcount(bits);
        });
        return result;
    }

    void Clear() {
        PrepareInline();
    }

    bool UnionWith(const TAdaptiveBitSetStorage& other) {
        if (this == &other || other.Empty()) {
            return false;
        }
        if (Empty()) {
            *this = other;
            return true;
        }

        return ApplyMerge<NIndexedWords::EMerge::Union>(other);
    }

    bool Subtract(const TAdaptiveBitSetStorage& other) {
        if (this == &other) {
            const bool changed = !Empty();
            Clear();
            return changed;
        }
        if (Empty() || other.Empty()) {
            return false;
        }

        return ApplyMerge<NIndexedWords::EMerge::Subtract>(other);
    }

    bool IntersectWith(const TAdaptiveBitSetStorage& other) {
        if (this == &other || Empty()) {
            return false;
        }
        if (other.Empty()) {
            Clear();
            return true;
        }

        return ApplyMerge<NIndexedWords::EMerge::Intersect>(other);
    }

    bool HasAny(const TAdaptiveBitSetStorage& other) const {
        return VisitWordPair(*this, other, [](auto left, auto right) {
            return NIndexedWords::HasAny(left, right);
        });
    }

    bool IsSubsetOf(const TAdaptiveBitSetStorage& other) const {
        if (NonzeroWordCount() > other.NonzeroWordCount()) {
            return false;
        }
        return VisitWordPair(*this, other, [](auto left, auto right) {
            return NIndexedWords::IsSubset(left, right);
        });
    }

    const_iterator begin() const {
        return const_iterator(this, false);
    }

    const_iterator end() const {
        return const_iterator(this, true);
    }

    bool operator==(const TAdaptiveBitSetStorage& other) const {
        if (this == &other) {
            return true;
        }
        if (NonzeroWordCount() != other.NonzeroWordCount()) {
            return false;
        }
        return VisitWordPair(*this, other, [](auto left, auto right) {
            return NIndexedWords::Equal(left, right);
        });
    }

    TAdaptiveBitSetStorage& operator&=(const TAdaptiveBitSetStorage& other) {
        IntersectWith(other);
        return *this;
    }

    Y_FORCE_INLINE EStorageKind StorageKind() const {
        return static_cast<EStorageKind>(Meta_ >> KindShift);
    }

private:
    static constexpr ui32 WordShift = 6;
    static constexpr ui32 WordMask = 63;
    static constexpr ui32 MaxWordIndex = (InvalidBit - 1) >> WordShift;
    static constexpr size_t MaxWordCount = static_cast<size_t>(MaxWordIndex) + 1;
    static constexpr ui32 KindShift = 62;
    static constexpr ui64 SparseInlineTag = ui64{1} << KindShift;
    static constexpr ui64 DenseHeapTag = ui64{2} << KindShift;
    static constexpr ui64 SparseHeapTag = ui64{3} << KindShift;
    static constexpr ui32 BaseBits = 26;
    static constexpr ui64 BaseMask = (ui64{1} << BaseBits) - 1;
    static constexpr ui32 DeltaBits = 18;
    static constexpr ui64 DenseBuildRatio = 3;
    static constexpr ui64 DenseKeepRatio = 4;
    static constexpr size_t InitialHeapCapacity = 4;

    static_assert(BaseMask == MaxWordIndex);

    // Both dense layouts encode a base followed by a count; only the count's
    // width and meaning differ (inline span versus heap nonzero-word count).
    template <EStorageKind Kind, ui32 CountBits>
    struct TDenseMetadata {
        static_assert(BaseBits + CountBits <= KindShift);
        static constexpr ui64 CountMask = (ui64{1} << CountBits) - 1;

        static ui64 Encode(ui32 base, size_t count) {
            Y_ASSERT(base <= BaseMask && count <= CountMask);
            return (static_cast<ui64>(Kind) << KindShift) | base
                | (static_cast<ui64>(count) << BaseBits);
        }

        static Y_FORCE_INLINE size_t Count(ui64 meta) {
            return static_cast<size_t>((meta >> BaseBits) & CountMask);
        }
    };

    using TDenseInlineMetadata = TDenseMetadata<EStorageKind::DenseInline, 2>;
    using TDenseHeapMetadata = TDenseMetadata<EStorageKind::DenseHeap, 27>;

    template <typename TMask>
    using TDenseWordView = NIndexedWords::TView<
        TMask,
        NIndexedWords::TConstantDeltaIndices<1>>;

    using TPackedInlineIndices =
        NIndexedWords::TPackedBaseDeltas<BaseBits, DeltaBits, InlinePayloadWords - 1>;
    static_assert(BaseBits + DeltaBits * (InlinePayloadWords - 1) == KindShift);

    template <typename TMask>
    using TSparseInlineView = NIndexedWords::TView<
        TMask,
        TPackedInlineIndices>;

    template <typename TStoredWord>
    using TSparseHeapView = NIndexedWords::TStoredWordView<TStoredWord>;

    enum class EFastPathResult : ui8 {
        Fallback,
        Unchanged,
        Changed,
    };

    static Y_FORCE_INLINE void AppendWord(TWordVector& output, TWord word) {
        Y_ASSERT(word.Bits);
        Y_ASSERT(!output.size() || output.back().Index < word.Index);
        output.push_back(word);
    }

    template <typename TCallback>
    static bool VisitWordPair(
        const TAdaptiveBitSetStorage& left,
        const TAdaptiveBitSetStorage& right,
        TCallback&& callback)
    {
        return left.VisitWords([&](auto leftView) -> decltype(auto) {
            return right.VisitWords([&](auto rightView) -> decltype(auto) {
                return callback(leftView, rightView);
            });
        });
    }

    template <NIndexedWords::EMerge Op>
    bool ApplyMerge(const TAdaptiveBitSetStorage& other) {
        const auto outcome = TryMergeDense<Op>(other);
        if (outcome != EFastPathResult::Fallback) {
            return outcome == EFastPathResult::Changed;
        }
        TWordVector result;
        if constexpr (Op == NIndexedWords::EMerge::Union) {
            const size_t lhsWords = NonzeroWordCount();
            const size_t rhsWords = other.NonzeroWordCount();
            if (std::max(lhsWords, rhsWords) > MergeInlineWordCapacity) {
                result.reserve(std::min(MaxWordCount, lhsWords + rhsWords));
            }
        }
        const bool changed = VisitWordPair(*this, other, [&](auto leftView, auto rightView) {
            return NIndexedWords::Merge<Op>(leftView, rightView, [&](TWord word) {
                AppendWord(result, word);
            });
        });
        if (changed) {
            AssignWords(result, StorageKind() == EStorageKind::DenseHeap);
        }
        return changed;
    }

    template <NIndexedWords::EMerge Op>
    EFastPathResult TryMergeDense(const TAdaptiveBitSetStorage& other) {
        using NIndexedWords::EMergeMode;

        if (!UsesDenseStorage() || !other.UsesDenseStorage()) {
            return EFastPathResult::Fallback;
        }
        const auto left = DenseWords(*this);
        const auto right = DenseWords(other);
        const size_t count = NonzeroWordCount();
        if constexpr (Op == NIndexedWords::EMerge::Union) {
            if (right.Indices.Base < left.Indices.Base
                || right.Indices.Base + right.Masks.size() > left.Indices.Base + left.Masks.size())
            {
                return EFastPathResult::Fallback;
            }
            const auto result = NIndexedWords::MergeDense<Op, EMergeMode::Apply>(left, right, count);
            if (UsesHeapStorage()) {
                SetDenseHeapNonzeroWords(result.NonzeroWords);
            }
            return result.Changed ? EFastPathResult::Changed : EFastPathResult::Unchanged;
        } else {
            const auto result = NIndexedWords::MergeDense<Op, EMergeMode::Preview>(left, right, count);
            if (!result.Changed) {
                return EFastPathResult::Unchanged;
            }
            if (!result.NonzeroWords) {
                Clear();
                return EFastPathResult::Changed;
            }
            // A conservative check: if the untrimmed window still fits, trimming
            // cannot require a sparse heap. Small results use normal installation.
            if (UsesHeapStorage() && (result.NonzeroWords <= InlinePayloadWords
                || !ShouldUseDenseHeap(left.Masks.size(), result.NonzeroWords, true)))
            {
                return EFastPathResult::Fallback;
            }
            NIndexedWords::MergeDense<Op, EMergeMode::Apply>(left, right, count);
            TrimDense(result.NonzeroWords);
            return EFastPathResult::Changed;
        }
    }

    Y_FORCE_INLINE ui32 BaseWordIndex() const {
        return static_cast<ui32>(Meta_ & BaseMask);
    }

    Y_FORCE_INLINE size_t DenseWindowSpan() const {
        return TDenseInlineMetadata::Count(Meta_);
    }

    Y_FORCE_INLINE size_t SparseInlineWordCount() const {
        Y_ASSERT(StorageKind() == EStorageKind::SparseInline);
        return 1 + (Storage_.Inline.Words[1] != 0) + (Storage_.Inline.Words[2] != 0);
    }

    Y_FORCE_INLINE size_t DenseHeapNonzeroWords() const {
        return TDenseHeapMetadata::Count(Meta_);
    }

    Y_FORCE_INLINE void SetDenseHeapNonzeroWords(size_t count) {
        Meta_ = TDenseHeapMetadata::Encode(BaseWordIndex(), count);
    }

    template <typename TSelf>
    static Y_FORCE_INLINE TDenseWordView<std::conditional_t<std::is_const_v<TSelf>, const ui64, ui64>>
    DenseWords(TSelf& self) {
        Y_ASSERT(self.UsesDenseStorage());
        using TMask = std::conditional_t<std::is_const_v<TSelf>, const ui64, ui64>;
        const auto masks = self.UsesHeapStorage()
            ? std::span<TMask>(static_cast<TMask*>(self.Storage_.Heap.Data), self.Storage_.Heap.Size)
            : std::span<TMask>(self.Storage_.Inline.Words, self.DenseWindowSpan());
        return {masks, NIndexedWords::TConstantDeltaIndices<1>{self.BaseWordIndex()}};
    }

    // Restore dense boundaries and metadata after changing masks in place.
    // The caller has already established that the nonempty result stays dense.
    void TrimDense(size_t nonzeroWords) {
        Y_ASSERT(nonzeroWords);
        const auto view = DenseWords(*this);
        const auto masks = view.Masks;
        size_t first = 0;
        size_t end = masks.size();
        while (!masks[first]) {
            ++first;
        }
        while (!masks[end - 1]) {
            --end;
        }
        const size_t span = end - first;
        const ui32 base = view.Indices.Base + static_cast<ui32>(first);
        if (first) {
            std::move(masks.begin() + first, masks.begin() + end, masks.begin());
        }
        if (UsesHeapStorage()) {
            Storage_.Heap.Size = static_cast<ui32>(span);
            Meta_ = TDenseHeapMetadata::Encode(base, nonzeroWords);
        } else {
            std::fill(Storage_.Inline.Words + span, Storage_.Inline.Words + InlinePayloadWords, ui64{0});
            Meta_ = TDenseInlineMetadata::Encode(base, span);
        }
    }

    template <typename TSelf, typename TCallback>
    static Y_FORCE_INLINE decltype(auto) VisitWordsImpl(TSelf& self, TCallback&& callback) {
        using TMask = std::conditional_t<std::is_const_v<TSelf>, const ui64, ui64>;
        using TStoredWord = std::conditional_t<std::is_const_v<TSelf>, const TWord, TWord>;

        switch (self.StorageKind()) {
            case EStorageKind::DenseInline:
                return std::forward<TCallback>(callback)(TDenseWordView<TMask>{
                    std::span<TMask>(self.Storage_.Inline.Words, self.DenseWindowSpan()),
                    NIndexedWords::TConstantDeltaIndices<1>{self.BaseWordIndex()}});
            case EStorageKind::SparseInline:
                return std::forward<TCallback>(callback)(TSparseInlineView<TMask>{
                    std::span<TMask>(self.Storage_.Inline.Words, self.SparseInlineWordCount()),
                    TPackedInlineIndices{self.Meta_}});
            case EStorageKind::DenseHeap:
                return std::forward<TCallback>(callback)(TDenseWordView<TMask>{
                    std::span<TMask>(
                        static_cast<TMask*>(self.Storage_.Heap.Data), self.Storage_.Heap.Size),
                    NIndexedWords::TConstantDeltaIndices<1>{self.BaseWordIndex()}});
            case EStorageKind::SparseHeap: {
                auto* entries = static_cast<TStoredWord*>(self.Storage_.Heap.Data);
                return std::forward<TCallback>(callback)(TSparseHeapView<TStoredWord>{
                    std::span<TStoredWord>(entries, self.Storage_.Heap.Size)});
            }
        }
        Y_UNREACHABLE();
    }

    template <typename TCallback>
    Y_FORCE_INLINE decltype(auto) VisitWords(TCallback&& callback) const {
        return VisitWordsImpl(*this, std::forward<TCallback>(callback));
    }

    bool NextNonzeroWord(size_t& position, TWord& result) const {
        return VisitWords([&](auto view) {
            return NIndexedWords::NextNonzeroWord(view, position, result);
        });
    }

    template <typename TCallback>
    void ForEachNonzeroWord(TCallback&& callback) const {
        VisitWords([&](auto view) {
            NIndexedWords::TCursor cursor(view);
            while (cursor.Valid()) {
                callback(cursor.Get().Index, cursor.Get().Bits);
                cursor.Next();
            }
        });
    }

    template <typename TSelf>
    static Y_FORCE_INLINE std::conditional_t<std::is_const_v<TSelf>, const ui64, ui64>*
    FindMask(TSelf& self, ui32 index, size_t* sparsePosition = nullptr) {
        using TStoredWord = std::conditional_t<std::is_const_v<TSelf>, const TWord, TWord>;
        return VisitWordsImpl(self, [index, sparsePosition](auto view) {
            if constexpr (std::is_same_v<decltype(view), TSparseHeapView<TStoredWord>>) {
                return view.FindMask(index, sparsePosition);
            } else {
                return view.FindMask(index);
            }
        });
    }

    void CollectWords(TWordVector& result) const {
        result.clear();
        const size_t count = NonzeroWordCount();
        if (count > MergeInlineWordCapacity) {
            result.reserve(count);
        }
        ForEachNonzeroWord([&](ui32 index, ui64 bits) {
            AppendWord(result, {index, bits});
        });
    }

    size_t NonzeroWordCount() const {
        switch (StorageKind()) {
            case EStorageKind::DenseInline: {
                size_t count = 0;
                for (size_t i = 0; i < DenseWindowSpan(); ++i) {
                    count += Storage_.Inline.Words[i] != 0;
                }
                return count;
            }
            case EStorageKind::SparseInline:
                return SparseInlineWordCount();
            case EStorageKind::DenseHeap:
                return DenseHeapNonzeroWords();
            case EStorageKind::SparseHeap:
                return Storage_.Heap.Size;
        }
        Y_UNREACHABLE();
    }

    static bool CanUseDenseInline(TWordSpan words) {
        const ui64 span = static_cast<ui64>(words.back().Index) - words.front().Index + 1;
        return span <= InlinePayloadWords;
    }

    static bool CanUseSparseInline(TWordSpan words) {
        return words.size() <= InlinePayloadWords
            && (words.size() < 2
                || static_cast<ui64>(words.back().Index) - words.front().Index <= TPackedInlineIndices::DeltaMask);
    }

    static bool ShouldUseDenseHeap(ui64 span, size_t nonzeroWords, bool retainDense) {
        const ui64 ratio = retainDense ? DenseKeepRatio : DenseBuildRatio;
        return span <= ratio * static_cast<ui64>(nonzeroWords);
    }

    static bool ShouldUseDenseHeap(TWordSpan words, bool retainDense) {
        const ui64 span = static_cast<ui64>(words.back().Index) - words.front().Index + 1;
        return ShouldUseDenseHeap(span, words.size(), retainDense);
    }

    // No allocation: decline unchanged if removal may require another topology.
    bool TryRemoveHeapWord(ui64* mask, size_t sparsePosition) {
        if (!UsesHeapStorage()) {
            return false;
        }
        const size_t count = NonzeroWordCount() - 1;
        if (count <= InlinePayloadWords) {
            return false;
        }
        auto& heap = Storage_.Heap;
        if (StorageKind() == EStorageKind::SparseHeap) {
            auto* data = static_cast<TWord*>(heap.Data);
            // Exclude a removed endpoint before checking the remaining span.
            ui32 first = 0;
            ui32 last = heap.Size - 1;
            if (sparsePosition == first) {
                ++first;
            }
            if (sparsePosition == last) {
                --last;
            }
            const ui64 span = static_cast<ui64>(data[last].Index) - data[first].Index + 1;
            if (ShouldUseDenseHeap(span, count, false)) {
                return false;
            }
            std::move(data + sparsePosition + 1, data + heap.Size, data + sparsePosition);
            --heap.Size;
        } else {
            if (!ShouldUseDenseHeap(heap.Size, count, true)) {
                return false;
            }
            *mask = 0;
            TrimDense(count);
        }
        return true;
    }

    // A new physical word can usually stay in its existing heap topology.
    // Sorted sparse insertion still shifts successors; increasing IDs need
    // amortized O(1) append. Dense windows grow geometrically.
    bool TryInsertHeapWord(TWord word, size_t sparsePosition) {
        if (!UsesHeapStorage()) {
            return false;
        }
        const auto heap = Storage_.Heap;
        if (StorageKind() == EStorageKind::SparseHeap) {
            auto* data = static_cast<TWord*>(heap.Data);
            const ui64 span = static_cast<ui64>(std::max(word.Index, data[heap.Size - 1].Index))
                - std::min(word.Index, data[0].Index) + 1;
            if (ShouldUseDenseHeap(span, heap.Size + 1, false)) {
                return false;
            }
            const size_t position = sparsePosition;
            Y_ASSERT(position <= heap.Size);
            Y_ASSERT(!position || data[position - 1].Index < word.Index);
            Y_ASSERT(position == heap.Size || word.Index < data[position].Index);
            if (heap.Size < heap.Capacity) {
                std::move_backward(data + position, data + heap.Size, data + heap.Size + 1);
                data[position] = word;
                ++Storage_.Heap.Size;
            } else {
                auto replacement = AllocateHeap<TWord>(GrowCapacity(heap.Capacity, heap.Size + 1));
                auto* target = static_cast<TWord*>(replacement.Data);
                std::copy_n(data, position, target);
                target[position] = word;
                std::copy(data + position, data + heap.Size, target + position + 1);
                replacement.Size = heap.Size + 1;
                ReplaceHeap(replacement, Meta_);
            }
            return true;
        }

        const ui32 oldBase = BaseWordIndex();
        const ui32 base = std::min(oldBase, word.Index);
        const size_t span = static_cast<size_t>(std::max(oldBase + heap.Size - 1, word.Index)) - base + 1;
        const size_t count = DenseHeapNonzeroWords() + 1;
        if (!ShouldUseDenseHeap(span, count, true)) {
            return false;
        }
        const size_t offset = oldBase - base;
        const auto meta = TDenseHeapMetadata::Encode(base, count);
        auto* data = static_cast<ui64*>(heap.Data);
        if (span <= heap.Capacity) {
            if (offset) {
                std::move_backward(data, data + heap.Size, data + offset + heap.Size);
            }
            std::fill_n(data, offset, ui64{0});
            std::fill(data + offset + heap.Size, data + span, ui64{0});
            data[word.Index - base] = word.Bits;
            Storage_.Heap.Size = static_cast<ui32>(span);
            Meta_ = meta;
        } else {
            auto replacement = AllocateHeap<ui64>(GrowCapacity(heap.Capacity, span));
            auto* target = static_cast<ui64*>(replacement.Data);
            std::fill_n(target, span, ui64{0});
            std::copy_n(data, heap.Size, target + offset);
            target[word.Index - base] = word.Bits;
            replacement.Size = static_cast<ui32>(span);
            ReplaceHeap(replacement, meta);
        }
        return true;
    }

    void AssignWords(TWordSpan words, bool retainDense = false) {
        Y_ASSERT(words.size() <= MaxWordCount);
        if (!words.empty()) {
            Y_ASSERT(words.front().Bits && words.back().Bits);
            Y_ASSERT(words.front().Index <= words.back().Index);
            Y_ASSERT(words.back().Index <= MaxWordIndex);
            Y_ASSERT(words.back().Index != MaxWordIndex
                || !(words.back().Bits & (ui64{1} << 63)));
        }

        if (words.empty()) {
            PrepareInline();
        } else if (CanUseDenseInline(words)) {
            SetDenseInline(words);
        } else if (CanUseSparseInline(words)) {
            SetSparseInline(words);
        } else if (ShouldUseDenseHeap(words, retainDense)) {
            SetDenseHeap(words);
        } else {
            SetSparseHeap(words);
        }
    }

    void AssignWords(const TWordVector& words, bool retainDense = false) {
        AssignWords(TWordSpan(words.data(), words.size()), retainDense);
    }

    void SetDenseInline(TWordSpan words) {
        Y_ASSERT(!words.empty() && CanUseDenseInline(words));
        TInlineStorage inlineStorage;
        const ui32 base = words.front().Index;
        for (const auto& word : words) {
            inlineStorage.Words[word.Index - base] = word.Bits;
        }
        const size_t span = words.back().Index - base + 1;
        ReplaceInline(inlineStorage, TDenseInlineMetadata::Encode(base, span));
    }

    void SetSparseInline(TWordSpan words) {
        Y_ASSERT(!words.empty() && CanUseSparseInline(words));
        TInlineStorage inlineStorage;
        for (size_t i = 0; i < words.size(); ++i) {
            inlineStorage.Words[i] = words[i].Bits;
        }
        ReplaceInline(inlineStorage, SparseInlineTag | TPackedInlineIndices::Encode(words));
    }

    template <EStorageKind Kind, typename TFill>
    Y_FORCE_INLINE void InstallHeap(size_t size, ui64 meta, TFill&& fill) {
        static_assert(Kind == EStorageKind::DenseHeap || Kind == EStorageKind::SparseHeap);
        using T = std::conditional_t<Kind == EStorageKind::DenseHeap, ui64, TWord>;
        // Filling may overwrite an existing buffer or an uncommitted allocation.
        static_assert(std::is_nothrow_invocable_v<TFill&, T*>);
        const size_t capacity = StorageKind() == Kind ? Storage_.Heap.Capacity : 0;
        if (size <= capacity) {
            fill(static_cast<T*>(Storage_.Heap.Data));
            Storage_.Heap.Size = static_cast<ui32>(size);
            Meta_ = meta;
            return;
        }

        auto replacement = AllocateHeap<T>(GrowCapacity(capacity, size));
        fill(static_cast<T*>(replacement.Data));
        replacement.Size = static_cast<ui32>(size);
        ReplaceHeap(replacement, meta);
    }

    void SetDenseHeap(TWordSpan words) {
        Y_ASSERT(!words.empty() && ShouldUseDenseHeap(words, true));
        const ui32 base = words.front().Index;
        const size_t span = static_cast<size_t>(words.back().Index - base) + 1;
        InstallHeap<EStorageKind::DenseHeap>(span, TDenseHeapMetadata::Encode(base, words.size()),
            [words, base, span](ui64* data) noexcept {
                std::fill_n(data, span, ui64{0});
                for (const auto& word : words) {
                    data[word.Index - base] = word.Bits;
                }
            });
    }

    void SetSparseHeap(TWordSpan words) {
        Y_ASSERT(!words.empty());
        InstallHeap<EStorageKind::SparseHeap>(words.size(), SparseHeapTag,
            [words](TWord* data) noexcept {
                if (words.data() != data) {
                    std::copy(words.begin(), words.end(), data);
                }
            });
    }

    static size_t GrowCapacity(size_t current, size_t required) {
        Y_ENSURE(required && required <= MaxWordCount);
        size_t capacity = std::max(current, InitialHeapCapacity);
        while (capacity < required) {
            capacity = capacity <= MaxWordCount / 2 ? 2 * capacity : MaxWordCount;
        }
        return capacity;
    }

    template <typename T>
    static THeapStorage AllocateHeap(size_t capacity) {
        static_assert(std::is_trivially_copyable_v<T> && std::is_trivially_destructible_v<T>);
        constexpr size_t MaxAllocationCount = std::min<size_t>(
            std::numeric_limits<ui32>::max(),
            std::numeric_limits<size_t>::max() / sizeof(T));
        Y_ENSURE(capacity && capacity <= MaxWordCount && capacity <= MaxAllocationCount);
        return {
            .Data = y_allocate(capacity * sizeof(T)),
            .Size = 0,
            .Capacity = static_cast<ui32>(capacity),
        };
    }

    template <typename T>
    static THeapStorage CopyHeap(const THeapStorage& source) {
        static_assert(std::is_trivially_copyable_v<T> && std::is_trivially_destructible_v<T>);
        Y_ASSERT(source.Data && source.Size && source.Size <= source.Capacity);
        THeapStorage copy = AllocateHeap<T>(source.Size);
        std::copy_n(static_cast<const T*>(source.Data), source.Size, static_cast<T*>(copy.Data));
        copy.Size = source.Size;
        return copy;
    }

    static void ReleaseHeap(const THeapStorage& heap) noexcept {
        if (heap.Data) {
            y_deallocate(heap.Data);
        }
    }

    void ReplaceInline(TInlineStorage replacement, ui64 meta) noexcept {
        if (UsesHeapStorage()) {
            const THeapStorage old = Storage_.Heap;
            std::construct_at(std::addressof(Storage_.Inline), replacement);
            Meta_ = meta;
            ReleaseHeap(old);
        } else {
            Storage_.Inline = replacement;
            Meta_ = meta;
        }
    }

    void ReplaceHeap(THeapStorage replacement, ui64 meta) noexcept {
        Y_ASSERT(replacement.Data && replacement.Size && replacement.Size <= replacement.Capacity);
        if (UsesHeapStorage()) {
            const THeapStorage old = Storage_.Heap;
            Storage_.Heap = replacement;
            Meta_ = meta;
            ReleaseHeap(old);
        } else {
            std::construct_at(std::addressof(Storage_.Heap), replacement);
            Meta_ = meta;
        }
    }

    void PrepareInline() noexcept {
        ReplaceInline(TInlineStorage{}, 0);
    }

    void DestroyHeap() noexcept {
        if (UsesHeapStorage()) {
            ReleaseHeap(Storage_.Heap);
        }
    }

    void CopyFrom(const TAdaptiveBitSetStorage& other) {
        if (other.UsesHeapStorage()) {
            const THeapStorage copy = other.StorageKind() == EStorageKind::DenseHeap
                ? CopyHeap<ui64>(other.Storage_.Heap)
                : CopyHeap<TWord>(other.Storage_.Heap);
            ReplaceHeap(copy, other.Meta_);
        } else {
            ReplaceInline(other.Storage_.Inline, other.Meta_);
        }
    }

    void MoveFrom(TAdaptiveBitSetStorage& other) noexcept {
        if (other.UsesHeapStorage()) {
            ReplaceHeap(other.Storage_.Heap, other.Meta_);
        } else {
            ReplaceInline(other.Storage_.Inline, other.Meta_);
        }
        // Reset the source without freeing the heap ownership just transferred.
        std::construct_at(std::addressof(other.Storage_.Inline));
        other.Meta_ = 0;
    }

    Y_FORCE_INLINE bool UsesHeapStorage() const {
        return Meta_ & DenseHeapTag;
    }

    Y_FORCE_INLINE bool UsesDenseStorage() const {
        return !(Meta_ & SparseInlineTag);
    }

    TStorage Storage_;
    ui64 Meta_ = 0;
};

static_assert(sizeof(TAdaptiveBitSetStorage) == 4 * sizeof(ui64));
static_assert(alignof(TAdaptiveBitSetStorage) == alignof(ui64));
static_assert(std::is_nothrow_move_constructible_v<TAdaptiveBitSetStorage>);
static_assert(std::is_nothrow_move_assignable_v<TAdaptiveBitSetStorage>);

} // namespace NKikimr::NKqp::NPrivate
