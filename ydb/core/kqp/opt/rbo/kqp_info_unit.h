#pragma once

#include "kqp_adaptive_bitset.h"

#include <contrib/restricted/abseil-cpp/absl/container/inlined_vector.h>

#include <contrib/restricted/abseil-cpp/absl/container/btree_map.h>
#include <library/cpp/containers/absl/flat_hash_map.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/maybe.h>
#include <util/generic/vector.h>
#include <util/generic/string.h>
#include <util/generic/yexception.h>
#include <util/system/compiler.h>

#include <algorithm>
#include <concepts>
#include <initializer_list>
#include <iterator>
#include <limits>
#include <optional>
#include <ranges>
#include <string>
#include <type_traits>
#include <utility>

namespace NKikimr {
namespace NKqp {

inline std::pair<TString, TString> SplitAliasedMemberName(const TString& name) {
    if (name.StartsWith("_alias_")) {
        TString alias;
        size_t i = 7;
        for (; i < name.size(); ++i) {
            if (name[i] == '\\' && i + 1 < name.size()) {
                alias += name[++i];
                continue;
            }
            if (name[i] == '.') {
                break;
            }
            alias += name[i];
        }
        Y_ENSURE(i < name.size(), "Invalid _alias_ prefix: no separator dot");
        return {std::move(alias), name.substr(i + 1)};
    }
    if (auto idx = name.rfind('.'); idx != TString::npos) {
        return {name.substr(0, idx), name.substr(idx + 1)};
    }
    return {TString(), name};
}

/** A readable column spelling; logical identity is its plan-local ID. */
struct TInfoUnit {
    TInfoUnit(const TString& alias, const TString& column)
        : Alias(alias)
        , ColumnName(column) {
    }

    TInfoUnit(const TString& name) {
        std::tie(Alias, ColumnName) = SplitAliasedMemberName(name);
    }
    TInfoUnit() = default;

    TString GetFullName() const {
        return (Alias != "" ? Alias + "." : "") + ColumnName;
    }

    TString GetAlias() const { return Alias; }
    TString GetColumnName() const { return ColumnName; }

    bool operator==(const TInfoUnit& other) const {
        return Alias == other.Alias && ColumnName == other.ColumnName;
    }

private:
    TString Alias;
    TString ColumnName;
};

using TInfoUnitId = ui32;
using TUnorderedIUs = NPrivate::TAdaptiveBitSetStorage;

// IDs are array indices in one plan. Add creates a new logical definition even
// when its name already exists. Names are not binding-lookup keys or final
// output labels. For Read outputs, ColumnName is the physical source column.
// Subplan membership/dependencies live in TPlanProps::Subplans.
class TInfoUnitRegistry {
public:
    TInfoUnitId Add(TInfoUnit infoUnit) {
        return Add(std::move(infoUnit), std::nullopt);
    }

    // Generated labels have a per-prefix sequence; logical identity remains the ID.
    TInfoUnitId AddGenerated(TStringBuf annotation = {}) {
        const TString prefix = annotation.empty() ? TString("tmp") : TString(annotation);
        auto& counter = GeneratedCounters_[prefix];
        Y_ENSURE(counter < std::numeric_limits<ui32>::max(), "Generated column counter overflow");
        TString label = prefix + std::to_string(++counter);
        return Add(TInfoUnit("", std::move(label)), prefix);
    }

    // A fresh binding for the same value, e.g. at a Replicate port. A temporary
    // keeps its annotation under the new ID; other labels are copied.
    TInfoUnitId AddCopy(TInfoUnitId source) {
        Y_ENSURE(source < Entries_.size(), "Unknown information-unit ID " << source);
        // Both paths copy the source entry's text before the registry grows.
        if (const auto& annotation = Entries_[source].Annotation) {
            return AddGenerated(*annotation);
        }
        return Add(Get(source));
    }

    // Borrowed until registry growth, assignment or destruction. Retain IDs
    // across mutations and look the name up again.
    const TInfoUnit& Get(TInfoUnitId id) const Y_LIFETIME_BOUND {
        Y_ENSURE(id < Entries_.size(), "Unknown information-unit ID " << id);
        return Entries_[id].InfoUnit;
    }

    size_t Size() const {
        return Entries_.size();
    }

    // Freeze the shared explain/lowering spelling after logical optimization.
    // Dead definitions must not force live columns to acquire suffixes.
    void FinalizeDisplayNames(const TUnorderedIUs& live) {
        Y_ENSURE(!DisplayNames_, "Information-unit display names are already finalized");
        absl::flat_hash_map<TString, size_t> counts;
        THashSet<TString> used;
        for (const auto id : live) {
            const auto name = Get(id).GetFullName();
            ++counts[name];
            used.insert(name);
        }

        TVector<TString> names(Size());
        auto disambiguate = [&](TInfoUnitId id) {
            auto name = Get(id).GetFullName() + "_" + std::to_string(id);
            // Reserve literal names first, including spellings such as x_7.
            while (!used.insert(name).second) {
                name += "_";
            }
            return name;
        };
        for (const auto id : live) {
            auto name = Get(id).GetFullName();
            names[id] = counts.at(name) == 1 ? std::move(name) : disambiguate(id);
        }
        // Some builders also describe unused fields. Keep their spellings safe
        // without letting them influence the names of the live plan.
        for (TInfoUnitId id = 0; id < Size(); ++id) {
            if (!live.Contains(id)) {
                names[id] = disambiguate(id);
            }
        }
        DisplayNames_ = std::move(names);
    }

    // Before finalization, diagnostics may still request an unambiguous name.
    // Optimizer AST bindings and row-schema fields always use the decimal ID.
    TString GetDisplayName(TInfoUnitId id) const {
        Y_ENSURE(id < Entries_.size(), "Unknown information-unit ID " << id);
        if (DisplayNames_) {
            return (*DisplayNames_)[id];
        }
        return Get(id).GetFullName() + "_" + std::to_string(id);
    }

    TString GetDebugName(TInfoUnitId id) const {
        return "%" + std::to_string(id) + (IsGenerated(id) ? "{" : "[")
            + Get(id).GetFullName() + (IsGenerated(id) ? "}" : "]");
    }

    bool IsGenerated(TInfoUnitId id) const {
        Y_ENSURE(id < Entries_.size(), "Unknown information-unit ID " << id);
        return Entries_[id].Annotation.has_value();
    }

private:
    struct TEntry {
        TInfoUnit InfoUnit;
        // The generation prefix, engaged only for temporaries.
        std::optional<TString> Annotation;
    };

    TInfoUnitId Add(TInfoUnit infoUnit, std::optional<TString> annotation) {
        Y_ENSURE(!DisplayNames_, "Cannot add information units after display names are finalized");
        Y_ENSURE(Entries_.size() < std::numeric_limits<TInfoUnitId>::max(),
            "Too many information units in one plan");
        const auto id = static_cast<TInfoUnitId>(Entries_.size());
        Entries_.push_back({std::move(infoUnit), std::move(annotation)});
        return id;
    }

private:
    TVector<TEntry> Entries_;
    absl::flat_hash_map<TString, ui32> GeneratedCounters_;
    std::optional<TVector<TString>> DisplayNames_;
};

// Freeze once at physical conversion entry, after logical rewrites. Every
// internal physical field (including connection keys) uses this spelling.
// Storage columns and external result/effect labels have separate contracts.
// Never decode these names: IDs remain authoritative in the optimizer.
class TPhysicalNames {
public:
    explicit TPhysicalNames(const TInfoUnitRegistry& registry) {
        Names_.reserve(registry.Size());
        for (TInfoUnitId id = 0; id < registry.Size(); ++id) {
            Names_.push_back(registry.GetDisplayName(id));
            const auto& name = Names_.back();
            size_t underscores = 0;
            while (underscores < name.size() && name[name.size() - underscores - 1] == '_') {
                ++underscores;
            }
            if (underscores > TemporarySuffix_.size()) {
                TemporarySuffix_ = TString(underscores, '_');
            }
        }
    }

    const TString& Get(TInfoUnitId id) const Y_LIFETIME_BOUND {
        Y_ENSURE(id < Names_.size(), "Information-unit ID was not frozen for lowering: " << id);
        return Names_[id];
    }

    // All temporary bases end in '_'. Append the same suffix to each so they
    // stay distinct and end in more underscores than any logical field name.
    TString GetTemporaryName(TString name) const {
        Y_ENSURE(name.EndsWith('_'));
        return name + TemporarySuffix_;
    }

private:
    TVector<TString> Names_;
    TString TemporarySuffix_;
};

// A positional sequence, not a unique set: repeated IDs retain separate entries.
// Without metadata an entry is just an ID; otherwise it is {ID, value}.
// Entries are immutable through this API. Their moves, swaps and destruction must
// not throw, so positional edits and insertion rollback cannot corrupt the cache.
template <typename TValue = void>
class TOrderedIUs {
public:
    using TEntry = std::conditional_t<std::is_void_v<TValue>, TInfoUnitId, std::pair<TInfoUnitId, TValue>>;
    // Abseil fills otherwise unused heap-handle space with extra inline entries:
    // four bare IDs (or two ID/bool pairs) fit without allocation on x86-64.
    using TEntries = absl::InlinedVector<TEntry, 1>;

    static_assert(std::is_nothrow_move_constructible_v<TEntry>);
    static_assert(std::is_nothrow_move_assignable_v<TEntry>);
    static_assert(std::is_nothrow_swappable_v<TEntry>);
    static_assert(std::is_nothrow_destructible_v<TEntry>);

    TOrderedIUs() = default;

    explicit TOrderedIUs(TEntries entries)
        : Entries_(std::move(entries))
    {
        for (const auto& entry : Entries_) {
            Y_ENSURE(GetId(entry) != TUnorderedIUs::InvalidBit);
        }
    }

    template <std::input_iterator TIterator>
    TOrderedIUs(TIterator first, TIterator last)
        : TOrderedIUs(TEntries(first, last))
    {}

    TOrderedIUs(std::initializer_list<TEntry> entries)
        requires std::is_copy_constructible_v<TEntry>
        : TOrderedIUs(entries.begin(), entries.end())
    {}

    TOrderedIUs(const TOrderedIUs&)
        requires std::is_copy_constructible_v<TEntry> = default;

    TOrderedIUs(TOrderedIUs&& other) noexcept {
        Entries_.swap(other.Entries_);
        Unordered_.swap(other.Unordered_);
    }

    // Prepare both sequence and cache before committing; self-move is safe too.
    TOrderedIUs& operator=(TOrderedIUs other) noexcept {
        Entries_.swap(other.Entries_);
        Unordered_.swap(other.Unordered_);
        return *this;
    }

    // Borrowed entries may be invalidated by edits, reserve, move or assignment,
    // including an insertion that reallocates before its membership update fails.
    // A temporary hands its views out by value, so they outlive it.
    const TEntries& Items() const& Y_LIFETIME_BOUND {
        return Entries_;
    }

    TEntries Items() && {
        TEntries entries;
        entries.swap(Entries_);
        Unordered_.reset();
        return entries;
    }

    void Reserve(size_t capacity) {
        Entries_.reserve(capacity);
    }

    template <typename... TArgs>
    const TEntry& Append(TInfoUnitId id, TArgs&&... args) {
        return InsertAt(Entries_.size(), id, std::forward<TArgs>(args)...);
    }

    // Appends the ID unless it is already present.
    bool AppendMissing(TInfoUnitId id)
        requires std::is_void_v<TValue>
    {
        if (Unordered().Contains(id)) {
            return false;
        }
        Append(id);
        return true;
    }

    // Appends, in order, each ID that is not present yet.
    template <std::ranges::input_range TRange>
    void AppendMissing(const TRange& ids)
        requires std::is_void_v<TValue>
    {
        for (const auto id : ids) {
            AppendMissing(id);
        }
    }

    template <typename... TArgs>
    const TEntry& InsertAt(size_t position, TInfoUnitId id, TArgs&&... args) {
        Y_ENSURE(position <= Entries_.size());
        // Materialize metadata before the vector can move its source entries.
        auto entry = MakeEntry(id, std::forward<TArgs>(args)...);
        if (position == Entries_.size()) {
            Entries_.push_back(std::move(entry));
        } else {
            Entries_.insert(Entries_.begin() + position, std::move(entry));
        }
        if (Unordered_) {
            try {
                Unordered_->Add(id);
            } catch (...) {
                Entries_.erase(Entries_.begin() + position);
                throw;
            }
        }
        return Entries_[position];
    }

    template <typename... TArgs>
    void ReplaceAt(size_t position, TInfoUnitId id, TArgs&&... args) {
        Y_ENSURE(position < Entries_.size());
        auto entry = MakeEntry(id, std::forward<TArgs>(args)...);
        if (GetId(Entries_[position]) != id) {
            Unordered_.reset();
        }
        Entries_[position] = std::move(entry);
    }

    void EraseAt(size_t position) {
        Y_ENSURE(position < Entries_.size());
        Entries_.erase(Entries_.begin() + position);
        // Another occurrence may still need this ID; rebuild once on demand.
        Unordered_.reset();
    }

    void Clear() {
        Entries_.clear();
        Unordered_.reset();
    }

    // Borrowed until mutation, move, assignment or destruction. Reads reuse the
    // cache; insertions update it, metadata-only replacements preserve it, and
    // erases/ID replacements invalidate it. No per-ID reference counts are kept.
    const TUnorderedIUs& Unordered() const& Y_LIFETIME_BOUND {
        if (!Unordered_) {
            TUnorderedIUs result;
            result.Assign(Entries_ | std::views::transform([](const TEntry& entry) { return GetId(entry); }));
            Unordered_.emplace(std::move(result));
        }
        return *Unordered_;
    }

    TUnorderedIUs Unordered() && {
        std::as_const(*this).Unordered();
        auto unordered = std::move(*Unordered_);
        Unordered_.reset();
        return unordered;
    }

    bool operator==(const TOrderedIUs& other) const
        requires (std::is_void_v<TValue> || std::equality_comparable<TValue>)
    {
        return Entries_ == other.Entries_;
    }

private:
    static TInfoUnitId GetId(const TEntry& entry) {
        if constexpr (std::is_void_v<TValue>) {
            return entry;
        } else {
            return entry.first;
        }
    }

    template <typename... TArgs>
    static TEntry MakeEntry(TInfoUnitId id, TArgs&&... args) {
        Y_ENSURE(id != TUnorderedIUs::InvalidBit);
        if constexpr (std::is_void_v<TValue>) {
            static_assert(sizeof...(TArgs) == 0, "Plain ordered IUs have no metadata");
            return id;
        } else {
            return {id, TValue(std::forward<TArgs>(args)...)};
        }
    }

    TEntries Entries_;
    mutable std::optional<TUnorderedIUs> Unordered_;
};

// A set of oriented pairs: either side may repeat, but identical pairs cannot.
// Lexicographic order is internal, not a positional contract. Derive aligned
// physical key sequences from Items(), never from the independent side sets.
template <typename TPairType>
class TPairedIUCollection {
public:
    using TPair = TPairType;
    using TPairs = absl::InlinedVector<TPair, 1>; // Two pairs inline on x86-64.

    TPairedIUCollection() = default;

    // Normalize once for bulk construction, avoiding repeated insertion shifts.
    explicit TPairedIUCollection(TPairs pairs)
        : Pairs_(std::move(pairs))
    {
        Normalize();
    }

    template <std::input_iterator TIterator>
    TPairedIUCollection(TIterator first, TIterator last) {
        if constexpr (std::forward_iterator<TIterator>) {
            Pairs_.reserve(std::distance(first, last));
        }
        for (; first != last; ++first) {
            Pairs_.push_back(*first);
        }
        Normalize();
    }

    TPairedIUCollection(std::initializer_list<TPair> pairs)
        : TPairedIUCollection(pairs.begin(), pairs.end())
    {}

    template <typename TOtherPair>
        requires std::is_constructible_v<TPair, const TOtherPair&>
    TPairedIUCollection(const TPairedIUCollection<TOtherPair>& other) {
        for (const auto& pair : other.Items()) {
            Pairs_.emplace_back(pair);
        }
        Normalize();
    }

    TPairedIUCollection(const TPairedIUCollection&) = default;

    TPairedIUCollection(TPairedIUCollection&& other) noexcept {
        Pairs_.swap(other.Pairs_);
        Sides_.swap(other.Sides_);
    }

    // Commit pairs and cache together; also preserves self-move.
    TPairedIUCollection& operator=(TPairedIUCollection other) noexcept {
        Pairs_.swap(other.Pairs_);
        Sides_.swap(other.Sides_);
        return *this;
    }

    // Borrowed entries may be invalidated by edits, reserve, move or assignment.
    // A temporary hands its views out by value, so they outlive it.
    const TPairs& Items() const& Y_LIFETIME_BOUND {
        return Pairs_;
    }

    TPairs Items() && {
        TPairs pairs;
        pairs.swap(Pairs_);
        Sides_.reset();
        return pairs;
    }

    void Reserve(size_t capacity) {
        Pairs_.reserve(capacity);
    }

    bool Contains(TInfoUnitId left, TInfoUnitId right) const {
        return Contains(TPair{left, right});
    }

    bool Contains(const TPair& pair) const {
        return std::binary_search(Pairs_.begin(), Pairs_.end(), pair);
    }

    bool Add(TInfoUnitId left, TInfoUnitId right) {
        return Add(TPair{left, right});
    }

    bool Add(TPair pair) {
        Validate(pair.first, pair.second);
        const auto it = std::lower_bound(Pairs_.begin(), Pairs_.end(), pair);
        if (it == Pairs_.end()) {
            Pairs_.push_back(pair);
        } else if (*it == pair) {
            return false;
        } else {
            Pairs_.insert(it, pair);
        }
        // Publish invalidation only after insertion succeeds.
        Sides_.reset();
        return true;
    }

    bool Remove(TInfoUnitId left, TInfoUnitId right) {
        return Remove(TPair{left, right});
    }

    bool Remove(const TPair& pair) {
        const auto it = std::lower_bound(Pairs_.begin(), Pairs_.end(), pair);
        if (it == Pairs_.end() || *it != pair) {
            return false;
        }
        Pairs_.erase(it);
        Sides_.reset();
        return true;
    }

    void SwapSides() {
        for (auto& pair : Pairs_) {
            std::swap(pair.first, pair.second);
        }
        std::sort(Pairs_.begin(), Pairs_.end());
        Sides_.reset();
    }

    void Clear() {
        Pairs_.clear();
        Sides_.reset();
    }

    // Both sets are built together on first demand and reused until a pair edit,
    // move, assignment or destruction. Duplicate Add / absent Remove keep them.
    const TUnorderedIUs& Left() const& Y_LIFETIME_BOUND {
        return GetSides().Left;
    }

    TUnorderedIUs Left() && {
        GetSides();
        auto left = std::move(Sides_->Left);
        Sides_.reset();
        return left;
    }

    const TUnorderedIUs& Right() const& Y_LIFETIME_BOUND {
        return GetSides().Right;
    }

    TUnorderedIUs Right() && {
        GetSides();
        auto right = std::move(Sides_->Right);
        Sides_.reset();
        return right;
    }

    bool operator==(const TPairedIUCollection& other) const {
        return Pairs_ == other.Pairs_;
    }

private:
    static void Validate(TInfoUnitId left, TInfoUnitId right) {
        Y_ENSURE(left != TUnorderedIUs::InvalidBit && right != TUnorderedIUs::InvalidBit);
    }

    void Normalize() {
        for (const auto& pair : Pairs_) {
            Validate(pair.first, pair.second);
        }
        std::sort(Pairs_.begin(), Pairs_.end());
        Pairs_.erase(std::unique(Pairs_.begin(), Pairs_.end()), Pairs_.end());
    }

    struct TSides {
        TUnorderedIUs Left;
        TUnorderedIUs Right;
    };

    const TSides& GetSides() const Y_LIFETIME_BOUND {
        if (!Sides_) {
            TSides result;
            result.Left.Assign(Pairs_ | std::views::transform([](const auto& pair) { return pair.first; }));
            result.Right.Assign(Pairs_ | std::views::transform([](const auto& pair) { return pair.second; }));
            Sides_.emplace(std::move(result));
        }
        return *Sides_;
    }

    TPairs Pairs_;
    mutable std::optional<TSides> Sides_;
};

using TPairedIUs = TPairedIUCollection<std::pair<TInfoUnitId, TInfoUnitId>>;

// Values are immutable through this API so a cached dependency union cannot go
// stale. TValuePolicy may validate values with Validate(value) and/or return a
// range of referenced IDs with operator()(value). Validation returns void and
// throws on invalid values. Neither hook may depend on mutable external state.
// Construction/copy of a value happens before entering Abseil; moves/swaps must
// not throw. Abseil still does not support recovery from allocator failure.
// Entries iterate in ascending ID order, so rewrites that allocate IDs while
// iterating are deterministic.
template <typename TValue, typename TValuePolicy = std::nullptr_t>
class TMappedIUs {
public:
    using TMap = absl::btree_map<TInfoUnitId, TValue>;

    static_assert(std::is_nothrow_move_constructible_v<TValue>);
    static_assert(std::is_nothrow_swappable_v<TValue>);
    static_assert(std::is_nothrow_move_constructible_v<TValuePolicy>);
    static_assert(std::is_nothrow_swappable_v<TValuePolicy>);

    // Without a policy, values change in place. A policy validates values and
    // may cache what they reference, so those change only through Replace.
    static constexpr bool MutableValues = std::is_same_v<TValuePolicy, std::nullptr_t>;
    using TValueRef = std::conditional_t<MutableValues, TValue&, const TValue&>;

    TMappedIUs()
        : TMappedIUs(TValuePolicy{})
    {}

    explicit TMappedIUs(TValuePolicy policy)
        : Policy_(std::move(policy))
    {}

    // Bulk construction builds the bitset once, including for scattered IDs.
    template <typename TIterator>
    TMappedIUs(TIterator first, TIterator last, TValuePolicy policy = {})
        : TMappedIUs(std::move(policy))
    {
        TVector<TInfoUnitId> ids;
        if constexpr (std::forward_iterator<TIterator>) {
            ids.reserve(std::distance(first, last));
        }
        for (; first != last; ++first) {
            auto&& item = *first;
            const auto id = item.first;
            InsertValue(id, TValue(std::forward<decltype(item)>(item).second));
            ids.push_back(id);
        }
        Keys_.Assign(ids);
    }

    TMappedIUs(std::initializer_list<std::pair<TInfoUnitId, TValue>> items, TValuePolicy policy = {})
        : TMappedIUs(items.begin(), items.end(), std::move(policy))
    {}

    TMappedIUs(const TMappedIUs& other)
        requires (std::is_copy_constructible_v<TValue> && std::is_copy_constructible_v<TValuePolicy>)
        : Keys_(other.Keys_)
        , Policy_(other.Policy_)
    {
        // Reuse the key index; value copies still happen before entering Abseil.
        for (const auto& [id, value] : other.Values_) {
            InsertValue(id, TValue(value));
        }
    }

    TMappedIUs(TMappedIUs&& other) noexcept
        : Policy_(std::move(other.Policy_))
    {
        Values_.swap(other.Values_);
        std::swap(Keys_, other.Keys_);
        Mapped_.swap(other.Mapped_);
    }

    // Construct both indices before committing; also preserves self-move.
    TMappedIUs& operator=(TMappedIUs other) noexcept {
        using std::swap;
        Values_.swap(other.Values_);
        swap(Keys_, other.Keys_);
        swap(Policy_, other.Policy_);
        Mapped_.swap(other.Mapped_);
        return *this;
    }

    // A temporary hands its views out by value, so they outlive it. Taking the
    // keys or entries leaves it empty.
    const TUnorderedIUs& Keys() const& Y_LIFETIME_BOUND {
        return Keys_;
    }

    TUnorderedIUs Keys() && {
        auto keys = std::exchange(Keys_, TUnorderedIUs{});
        Clear();
        return keys;
    }

    // Configuration cannot change independently of the values it governs.
    const TValuePolicy& Policy() const Y_LIFETIME_BOUND {
        return Policy_;
    }

    // Entries and Find/Add results are borrowed until a structural mutation
    // (an insertion or removal), move, assignment or destruction.
    const TMap& Items() const& Y_LIFETIME_BOUND {
        return Values_;
    }

    TMap Items() && {
        auto values = std::exchange(Values_, TMap{});
        Clear();
        return values;
    }

    const TValue* Find(TInfoUnitId id) const Y_LIFETIME_BOUND {
        const auto it = Values_.find(id);
        return it == Values_.end() ? nullptr : &it->second;
    }

    TValue* Find(TInfoUnitId id) Y_LIFETIME_BOUND
        requires MutableValues
    {
        const auto it = Values_.find(id);
        return it == Values_.end() ? nullptr : &it->second;
    }

    const TValue& At(TInfoUnitId id) const Y_LIFETIME_BOUND {
        const auto* value = Find(id);
        Y_ENSURE(value, "Unknown mapped information unit " << id);
        return *value;
    }

    TValue& At(TInfoUnitId id) Y_LIFETIME_BOUND
        requires MutableValues
    {
        auto* value = Find(id);
        Y_ENSURE(value, "Unknown mapped information unit " << id);
        return *value;
    }

    template <typename... TArgs>
    TValueRef Add(TInfoUnitId id, TArgs&&... args) {
        const auto it = InsertValue(id, TValue(std::forward<TArgs>(args)...));
        try {
            Y_ENSURE(Keys_.Add(id), "Mapped information-unit index is inconsistent");
        } catch (...) {
            Values_.erase(it);
            throw;
        }
        Mapped_.reset();
        return it->second;
    }

    template <typename... TArgs>
    void Replace(TInfoUnitId id, TArgs&&... args) {
        const auto it = Values_.find(id);
        Y_ENSURE(it != Values_.end(), "Unknown mapped information unit " << id);
        TValue replacement(std::forward<TArgs>(args)...);
        ValidateValue(replacement);
        using std::swap;
        swap(it->second, replacement);
        Mapped_.reset();
    }

    bool Remove(TInfoUnitId id) {
        const auto it = Values_.find(id);
        if (it == Values_.end()) {
            return false;
        }
        Y_ENSURE(Keys_.Remove(id), "Mapped information-unit index is inconsistent");
        Values_.erase(it);
        Mapped_.reset();
        return true;
    }

    // Bulk pruning preserves values and policy without rebuilding the hash map.
    bool RetainKeys(const TUnorderedIUs& keep) {
        if (Keys_.IsSubsetOf(keep)) {
            return false;
        }
        auto removed = Keys_;
        removed.Subtract(keep);
        auto retained = Keys_;
        retained.Subtract(removed); // Finish allocating before mutating values.
        for (const auto id : removed) {
            Y_ENSURE(Values_.erase(id), "Mapped information-unit index is inconsistent");
        }
        Keys_ = std::move(retained);
        Mapped_.reset();
        return true;
    }

    void Clear() {
        Values_.clear();
        Keys_.Clear();
        Mapped_.reset();
    }

    // Borrowed until the next successful mutation. Rebuild on first demand;
    // overlapping dependencies survive removal of any one referring value.
    const TUnorderedIUs& MappedIUs() const& Y_LIFETIME_BOUND
        requires std::is_invocable_v<const TValuePolicy&, const TValue&>
    {
        if (!Mapped_) {
            TVector<TInfoUnitId> ids;
            for (const auto& item : Values_) {
                for (const auto id : Policy_(item.second)) {
                    ids.push_back(id);
                }
            }
            TUnorderedIUs result;
            result.Assign(ids);
            Mapped_.emplace(std::move(result));
        }
        return *Mapped_;
    }

    TUnorderedIUs MappedIUs() &&
        requires std::is_invocable_v<const TValuePolicy&, const TValue&>
    {
        std::as_const(*this).MappedIUs();
        auto mapped = std::move(*Mapped_);
        Mapped_.reset();
        return mapped;
    }

private:
    struct TNoDependencyCache {
        void reset() noexcept {}
        void swap(TNoDependencyCache&) noexcept {}
    };
    using TDependencyCache = std::conditional_t<
        std::is_invocable_v<const TValuePolicy&, const TValue&>,
        std::optional<TUnorderedIUs>, TNoDependencyCache>;

    void ValidateValue(const TValue& value) const {
        if constexpr (requires { Policy_.Validate(value); }) {
            static_assert(std::is_void_v<decltype(Policy_.Validate(value))>, "Validate must throw on invalid values");
            Policy_.Validate(value);
        }
    }

    typename TMap::iterator InsertValue(TInfoUnitId id, TValue value) {
        Y_ENSURE(id != TUnorderedIUs::InvalidBit);
        ValidateValue(value);
        const auto [it, inserted] = Values_.try_emplace(id, std::move(value));
        Y_ENSURE(inserted, "Information unit " << id << " is already mapped");
        return it;
    }

    TMap Values_;
    TUnorderedIUs Keys_;
    [[no_unique_address]] TValuePolicy Policy_;
    [[no_unique_address]] mutable TDependencyCache Mapped_;
};

// Old ID -> new ID. Applying substitutions is simultaneous: each use is
// rewritten once, never through a chain of entries.
using TSubstitutions = TMappedIUs<TInfoUnitId>;

inline TInfoUnitId Substitute(TInfoUnitId id, const TSubstitutions& substitutions) {
    const auto* replacement = substitutions.Find(id);
    return replacement ? *replacement : id;
}

// Child position matters; IDs may repeat. No per-row membership set is needed.
struct TUnionInputRow {
    absl::InlinedVector<TInfoUnitId, 1> Inputs; // Four child IDs inline on x86-64.

    // Abseil's swap lacks noexcept, but allocates nothing and cannot throw for
    // plain IDs with the default allocator. Expose that guarantee to TMappedIUs.
    friend void swap(TUnionInputRow& left, TUnionInputRow& right) noexcept {
        left.Inputs.swap(right.Inputs);
    }
};

struct TUnionInputPolicy {
    size_t ChildCount = 0;

    void Validate(const TUnionInputRow& row) const {
        Y_ENSURE(row.Inputs.size() == ChildCount, "Expected " << ChildCount << " UnionAll inputs, got " << row.Inputs.size());
        for (const auto id : row.Inputs) {
            Y_ENSURE(id != TUnorderedIUs::InvalidBit);
        }
    }
};

// Unique output ID -> one input ID per child. Only output membership is cached;
// liveness projects the selected output rows onto each child separately.
using TUnionAllIUs = TMappedIUs<TUnionInputRow, TUnionInputPolicy>;

}
}
