#pragma once

#include <util/digest/multi.h>
#include <util/digest/sequence.h>
#include <util/generic/algorithm.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/stream/output.h>
#include <util/string/builder.h>
#include <util/string/strip.h>

#include <optional>
#include <type_traits>

namespace NMonitoring {
    struct ILabel {
        virtual ~ILabel() = default;

        virtual TStringBuf Name() const noexcept = 0;
        virtual TStringBuf Value() const noexcept = 0;
    };

    ///////////////////////////////////////////////////////////////////////////
    // TLabel
    ///////////////////////////////////////////////////////////////////////////
    template <typename TStringBackend>
    class TLabelImpl: public ILabel {
    public:
        using TStringType = TStringBackend;

        TLabelImpl() = default;

        inline TLabelImpl(TStringBuf name, TStringBuf value)
            : Name_{name}
            , Value_{value}
        {
        }

        inline bool operator==(const TLabelImpl& rhs) const noexcept {
            return Name_ == rhs.Name_ && Value_ == rhs.Value_;
        }

        inline bool operator!=(const TLabelImpl& rhs) const noexcept {
            return !(*this == rhs);
        }

        inline TStringBuf Name() const noexcept {
            return Name_;
        }

        inline TStringBuf Value() const noexcept {
            return Value_;
        }

        inline const TStringBackend& NameStr() const {
            return Name_;
        }

        inline const TStringBackend& ValueStr() const {
            return Value_;
        }

        inline size_t Hash() const noexcept {
            return MultiHash(Name_, Value_);
        }

        TStringBackend ToString() const {
            TStringBackend buf = Name_;
            buf += '=';
            buf += Value_;

            return buf;
        }

        static TLabelImpl FromString(TStringBuf str) {
            TStringBuf name, value;
            Y_ENSURE(str.TrySplit('=', name, value),
                     "invalid label string format: '" << str << '\'');

            TStringBuf nameStripped = StripString(name);
            Y_ENSURE(!nameStripped.empty(), "label name cannot be empty");

            TStringBuf valueStripped = StripString(value);
            Y_ENSURE(!valueStripped.empty(), "label value cannot be empty");

            return {nameStripped, valueStripped};
        }

        static bool TryFromString(TStringBuf str, TLabelImpl& label) {
            TStringBuf name, value;
            if (!str.TrySplit('=', name, value)) {
                return false;
            }

            TStringBuf nameStripped = StripString(name);
            if (nameStripped.empty()) {
                return false;
            }

            TStringBuf valueStripped = StripString(value);
            if (valueStripped.empty()) {
                return false;
            }

            label = {nameStripped, valueStripped};
            return true;
        }

    private:
        TStringBackend Name_;
        TStringBackend Value_;
    };

    using TLabel = TLabelImpl<TString>;

    struct ILabels : public TThrRefBase {
        struct TIterator {
            TIterator() = default;
            TIterator(const ILabels* labels, size_t idx = 0)
                : Labels_{labels}
                , Idx_{idx}
            {
            }

            TIterator& operator++() noexcept {
                Idx_++;
                return *this;
            }

            void operator+=(size_t i) noexcept {
                Idx_ += i;
            }

            bool operator==(const TIterator& other) const noexcept {
                return Idx_ == other.Idx_;
            }

            bool operator!=(const TIterator& other) const noexcept {
                return !(*this == other);
            }

            const ILabel* operator->() const noexcept {
                Y_DEBUG_ABORT_UNLESS(Labels_);
                return Labels_->Get(Idx_);
            }

            const ILabel& operator*() const noexcept {
                Y_DEBUG_ABORT_UNLESS(Labels_);
                return *Labels_->Get(Idx_);
            }


        private:
            const ILabels* Labels_{nullptr};
            size_t Idx_{0};
        };

        virtual ~ILabels() = default;

        virtual bool Add(TStringBuf name, TStringBuf value) noexcept = 0;
        virtual bool Add(const ILabel& label) noexcept {
            return Add(label.Name(), label.Value());
        }

        virtual bool Has(TStringBuf name) const noexcept = 0;

        virtual size_t Size() const noexcept = 0;
        virtual bool Empty() const noexcept = 0;
        virtual void Clear() noexcept = 0;

        virtual size_t Hash() const noexcept = 0;

        virtual std::optional<const ILabel*> Get(TStringBuf name) const = 0;

        // NB: there's no guarantee that indices are preserved after any object modification
        virtual const ILabel* Get(size_t idx) const = 0;

        TIterator begin() const {
            return TIterator{this};
        }

        TIterator end() const {
            return TIterator{this, Size()};
        }
    };

    bool TryLoadLabelsFromString(TStringBuf sb, ILabels& labels);
    bool TryLoadLabelsFromString(IInputStream& is, ILabels& labels);

    ///////////////////////////////////////////////////////////////////////////
    // TLabels
    ///////////////////////////////////////////////////////////////////////////
    template <typename TStringBackend>
    class TLabelsImpl: public ILabels {
    public:
        using value_type = TLabelImpl<TStringBackend>;

        TLabelsImpl() = default;

        explicit TLabelsImpl(::NDetail::TReserveTag rt)
            : Labels_(std::move(rt))
        {}

        explicit TLabelsImpl(size_t count)
            : Labels_(count)
        {}

        explicit TLabelsImpl(size_t count, const value_type& label)
            : Labels_(count, label)
        {}

        TLabelsImpl(std::initializer_list<value_type> il)
            : Labels_(std::move(il))
        {}

        TLabelsImpl(const TLabelsImpl&) = default;
        TLabelsImpl& operator=(const TLabelsImpl&) = default;

        TLabelsImpl(TLabelsImpl&&) noexcept = default;
        TLabelsImpl& operator=(TLabelsImpl&&) noexcept = default;

        inline bool operator==(const TLabelsImpl& rhs) const {
            return Labels_ == rhs.Labels_;
        }

        inline bool operator!=(const TLabelsImpl& rhs) const {
            return Labels_ != rhs.Labels_;
        }

        bool Add(TStringBuf name, TStringBuf value) noexcept override {
            if (Has(name)) {
                return false;
            }

            Labels_.emplace_back(name, value);
            return true;
        }

        using ILabels::Add;

        bool Has(TStringBuf name) const noexcept override {
            auto it = FindIf(Labels_, [name](const TLabelImpl<TStringBackend>& label) {
                return name == TStringBuf{label.Name()};
            });
            return it != Labels_.end();
        }

        bool Has(const TString& name) const noexcept {
            auto it = FindIf(Labels_, [name](const TLabelImpl<TStringBackend>& label) {
                return name == TStringBuf{label.Name()};
            });
            return it != Labels_.end();
        }

        // XXX for backward compatibility
        TMaybe<TLabelImpl<TStringBackend>> Find(TStringBuf name) const {
            auto it = FindIf(Labels_, [name](const TLabelImpl<TStringBackend>& label) {
                return name == TStringBuf{label.Name()};
            });
            if (it == Labels_.end()) {
                return Nothing();
            }
            return *it;
        }

        std::optional<const ILabel*> Get(TStringBuf name) const override {
            auto it = FindIf(Labels_, [name] (auto&& l) {
                return name == l.Name();
            });

            if (it == Labels_.end()) {
                return {};
            }

            return &*it;
        }

        const ILabel* Get(size_t idx) const noexcept override {
            return &(*this)[idx];
        }

        TMaybe<TLabelImpl<TStringBackend>> Extract(TStringBuf name) {
            auto it = FindIf(Labels_, [name](const TLabelImpl<TStringBackend>& label) {
                return name == TStringBuf{label.Name()};
            });
            if (it == Labels_.end()) {
                return Nothing();
            }
            TLabel tmp = *it;
            Labels_.erase(it);
            return tmp;
        }

        void SortByName() {
            std::sort(Labels_.begin(), Labels_.end(), [](const auto& lhs, const auto& rhs) {
                return lhs.Name() < rhs.Name();
            });
        }

        inline size_t Hash() const noexcept override {
            return TSimpleRangeHash()(Labels_);
        }

        inline TLabel* Data() const noexcept {
            return const_cast<TLabel*>(Labels_.data());
        }

        inline size_t Size() const noexcept override {
            return Labels_.size();
        }

        inline bool Empty() const noexcept override {
            return Labels_.empty();
        }

        inline void Clear() noexcept override {
            Labels_.clear();
        }

        TLabelImpl<TStringBackend>& front() {
            return Labels_.front();
        }

        const TLabelImpl<TStringBackend>& front() const {
            return Labels_.front();
        }

        TLabelImpl<TStringBackend>& back() {
            return Labels_.back();
        }

        const TLabelImpl<TStringBackend>& back() const {
            return Labels_.back();
        }

        TLabelImpl<TStringBackend>& operator[](size_t index) {
            return Labels_[index];
        }

        const TLabelImpl<TStringBackend>& operator[](size_t index) const {
            return Labels_[index];
        }

        TLabelImpl<TStringBackend>& at(size_t index) {
            return Labels_.at(index);
        }

        const TLabelImpl<TStringBackend>& at(size_t index) const {
            return Labels_.at(index);
        }

        size_t capacity() const {
            return Labels_.capacity();
        }

        TLabelImpl<TStringBackend>* data() {
            return Labels_.data();
        }

        const TLabelImpl<TStringBackend>* data() const {
            return Labels_.data();
        }

        size_t size() const {
            return Labels_.size();
        }

        bool empty() const {
            return Labels_.empty();
        }

        void clear() {
            Labels_.clear();
        }

        using ILabels::begin;
        using ILabels::end;

        using iterator = ILabels::TIterator;
        using const_iterator = iterator;

    protected:
        TVector<TLabelImpl<TStringBackend>>& AsVector() {
            return Labels_;
        }

        const TVector<TLabelImpl<TStringBackend>>& AsVector() const {
            return Labels_;
        }

    private:
        TVector<TLabelImpl<TStringBackend>> Labels_;
    };

    using TLabels = TLabelsImpl<TString>;
    using ILabelsPtr = TIntrusivePtr<ILabels>;

    template <typename T>
    ILabelsPtr MakeLabels() {
        return MakeIntrusive<TLabelsImpl<T>>();
    }

    template <typename T>
    ILabelsPtr MakeLabels(std::initializer_list<TLabelImpl<T>> labels) {
        return MakeIntrusive<TLabelsImpl<T>>(labels);
    }

    inline ILabelsPtr MakeLabels(TLabels&& labels) {
        return MakeIntrusive<TLabels>(std::move(labels));
    }
}

template<>
struct THash<NMonitoring::ILabelsPtr> {
    size_t operator()(const NMonitoring::ILabelsPtr& labels) const noexcept {
        return labels->Hash();
    }

    size_t operator()(const NMonitoring::ILabels& labels) const noexcept {
        return labels.Hash();
    }
};

template<typename TStringBackend>
struct THash<NMonitoring::TLabelsImpl<TStringBackend>> {
    size_t operator()(const NMonitoring::TLabelsImpl<TStringBackend>& labels) const noexcept {
        return labels.Hash();
    }
};

template <typename TStringBackend>
struct THash<NMonitoring::TLabelImpl<TStringBackend>> {
    inline size_t operator()(const NMonitoring::TLabelImpl<TStringBackend>& label) const noexcept {
        return label.Hash();
    }
};

inline bool operator==(const NMonitoring::ILabels& lhs, const NMonitoring::ILabels& rhs) {
    if (lhs.Size() != rhs.Size()) {
        return false;
    }

    for (auto&& l : lhs) {
        auto rl = rhs.Get(l.Name());
        if (!rl || (*rl)->Value() != l.Value()) {
            return false;
        }
    }

    return true;
}

bool operator==(const NMonitoring::ILabelsPtr& lhs, const NMonitoring::ILabelsPtr& rhs) = delete;
bool operator==(const NMonitoring::ILabels& lhs, const NMonitoring::ILabelsPtr& rhs) = delete;
bool operator==(const NMonitoring::ILabelsPtr& lhs, const NMonitoring::ILabels& rhs) = delete;

template<>
struct TEqualTo<NMonitoring::ILabelsPtr> {
    bool operator()(const NMonitoring::ILabelsPtr& lhs, const NMonitoring::ILabelsPtr& rhs) {
        return *lhs == *rhs;
    }

    bool operator()(const NMonitoring::ILabelsPtr& lhs, const NMonitoring::ILabels& rhs) {
        return *lhs == rhs;
    }

    bool operator()(const NMonitoring::ILabels& lhs, const NMonitoring::ILabelsPtr& rhs) {
        return lhs == *rhs;
    }
};

#define Y_MONLIB_DEFINE_LABELS_OUT(T) \
template <> \
void Out<T>(IOutputStream& out, const T& labels) { \
    Out<NMonitoring::ILabels>(out, labels); \
}

#define Y_MONLIB_DEFINE_LABEL_OUT(T) \
template <> \
void Out<T>(IOutputStream& out, const T& label) { \
    Out<NMonitoring::ILabel>(out, label); \
}
