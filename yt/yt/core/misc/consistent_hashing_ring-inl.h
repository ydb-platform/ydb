#ifndef CONSISTENT_HASHING_RING_INL_H_
#error "Direct inclusion of this file is not allowed, include consistent_hashing_ring.h"
// For the sake of sane code completion.
#include "consistent_hashing_ring.h"
#endif

#include <util/generic/algorithm.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NPrivate {

template <class H, class O>
class THashRangeIterator
{
public:
    THashRangeIterator(O object, H hasher, int iteration = 0)
        : Object_(object)
        , Hasher_(hasher)
        , Iteration_(iteration)
    { }

    ui64 operator*() const
    {
        return Hasher_(Object_, Iteration_);
    }

    THashRangeIterator& operator++()
    {
        ++Iteration_;
        return *this;
    }

    bool operator==(THashRangeIterator rhs) const
    {
        YT_ASSERT(Object_ == rhs.Object_);
        return Iteration_ == rhs.Iteration_;
    }

protected:
    int GetIteration() const
    {
        return Iteration_;
    }

    typename NMpl::TCallTraits<O>::TType GetObject() const
    {
        return Object_;
    }

private:
    O Object_;
    H Hasher_;
    int Iteration_;
};

template <class H, class O, class I>
class TItemRangeIterator
    : protected THashRangeIterator<H, O>
{
private:
    using TBase = THashRangeIterator<H, O>;

public:
    using TBase::TBase;

    std::pair<ui64, I> operator*() const{
        return {TBase::operator*(), I{TBase::GetObject(), TBase::GetIteration()}};
    }

    TItemRangeIterator& operator++()
    {
        TBase::operator++();
        return *this;
    }

    bool operator==(TItemRangeIterator rhs) const
    {
        return static_cast<const TBase&>(*this) == static_cast<const TBase&>(rhs);
    }
};

template <class H, class O>
using THashRange = std::pair<THashRangeIterator<H, O>, THashRangeIterator<H, O>>;

template <class H, class O, class I>
using TItemRange = std::pair<TItemRangeIterator<H, O, I>, TItemRangeIterator<H, O, I>>;

// TODO(shakurov): hashes alone are never sufficient. Transform hash range into token range?
template <class H, class O>
THashRange<H, O> GetHashRange(typename NMpl::TCallTraits<O>::TType object, int hashCount)
{
    return {
        THashRangeIterator(object, H()),
        THashRangeIterator(object, H(), hashCount)
    };
}

template <class H, class O, class I>
TItemRange<H, O, I> GetItemRange(typename NMpl::TCallTraits<O>::TType object, int hashCount)
{
    return {
        TItemRangeIterator<H, O, I>(object, H()),
        TItemRangeIterator<H, O, I>(object, H(), hashCount)
    };
}

} // namespace NPrivate

////////////////////////////////////////////////////////////////////////////////

// Just a marker - see operator== below.
template <class S, class F, class P, class H, int N>
class TConsistentHashingRing<S, F, P, H, N>::TFileRangeEndIterator
{ };

////////////////////////////////////////////////////////////////////////////////

template <class S, class F, class P, class H, int N>
class TConsistentHashingRing<S, F, P, H, N>::TFileRangeIterator
{
public:
    typename NMpl::TCallTraits<F>::TType operator*() const
    {
        const auto& ringItem = RingIt_->second;
        YT_ASSERT(std::holds_alternative<TFileReplica>(ringItem));
        return std::get<TFileReplica>(ringItem).File;
    }

    bool operator==(const TFileRangeIterator& rhs) const
    {
        YT_ASSERT(&Owner_ == &rhs.Owner_);
        return TokenRange_ == rhs.TokenRange_ && RingIt_ == rhs.RingIt_;
    }

    bool operator==(const TFileRangeEndIterator& /*rhs*/) const
    {
        return TokenRange_.first == TokenRange_.second;
    }

    TFileRangeIterator& operator++()
    {
        IncrementRingIterator();
        if (!std::holds_alternative<TFileReplica>(RingIt_->second)) {
            ++TokenRange_.first;
            SkipToNextFileReplica();
        }

        return *this;
    }

private:
    TFileRangeIterator(
        const TConsistentHashingRing<S, F, P, H, N>& owner,
        NPrivate::TItemRange<H, S, TServerToken> tokenRange)
        : Owner_(owner)
        , TokenRange_(std::move(tokenRange))
    {
        SkipToNextFileReplica();
    }

    void SkipToNextFileReplica()
    {
        while (TokenRange_.first != TokenRange_.second) {
            RingIt_ = Ring().find(*TokenRange_.first);
            YT_VERIFY(RingIt_ != Ring().end());
            IncrementRingIterator();
            if (std::holds_alternative<TFileReplica>(RingIt_->second)) {
                break;
            } // Otherwise this server token has zero associated files.
            ++TokenRange_.first;
        }
        // Just in case.
        if (TokenRange_.first == TokenRange_.second) {
            RingIt_ = Ring().end();
        }
    }

    void IncrementRingIterator()
    {
        if (++RingIt_ == Ring().end()) {
            RingIt_ = Ring().begin();
        }
    }

    const TConsistentHashingRing<S, F, P, H, N>::TRing& Ring() const
    {
        return Owner_.Ring_;
    }

    using TServerToken = typename TConsistentHashingRing::TServerToken;
    using TFileReplica = typename TConsistentHashingRing::TFileReplica;
    using TRingIterator = typename TConsistentHashingRing::TRing::const_iterator;

    const TConsistentHashingRing<S, F, P, H, N>& Owner_;
    NPrivate::TItemRange<H, S, TServerToken> TokenRange_;
    TRingIterator RingIt_;

    friend class TConsistentHashingRing;
};

////////////////////////////////////////////////////////////////////////////////

template <class S, class F, class P, class H, int N>
bool TConsistentHashingRing<S, F, P, H, N>::TServerToken::operator<(const TServerToken& rhs) const
{
    if (P()(Server, rhs.Server)) {
        return true;
    }
    if (P()(rhs.Server, Server)) {
        return false;
    }
    return Index < rhs.Index;
}

////////////////////////////////////////////////////////////////////////////////

template <class S, class F, class P, class H, int N>
bool TConsistentHashingRing<S, F, P, H, N>::TFileReplica::operator<(const TFileReplica& rhs) const
{
    if (P()(File, rhs.File)) {
        return true;
    }
    if (P()(rhs.File, File)) {
        return false;
    }
    return Index < rhs.Index;
}

////////////////////////////////////////////////////////////////////////////////

template <class S, class F, class P, class H, int N>
template <class TItem>
bool TConsistentHashingRing<S, F, P, H, N>::TRingCompare<TItem>::operator()(const std::pair<ui64, TItem>& lhs, const std::pair<ui64, TItem>& rhs) const
{
    return lhs < rhs;
}

template <class S, class F, class P, class H, int N>
template <class TItem>
bool TConsistentHashingRing<S, F, P, H, N>::TRingCompare<TItem>::operator()(ui64 lhs, const std::pair<ui64, TItem>& rhs) const
{
    // Naked hash is always less than corresponding items.
    if (lhs == rhs.first) {
        return true;
    }
    return lhs < rhs.first;
}

template <class S, class F, class P, class H, int N>
template <class TItem>
bool TConsistentHashingRing<S, F, P, H, N>::TRingCompare<TItem>::operator()(const std::pair<ui64, TItem>& lhs, ui64 rhs) const
{
    // Naked hash is always less than corresponding items.
    if (lhs.first == rhs) {
        return false;
    }
    return lhs.first < rhs;
}

////////////////////////////////////////////////////////////////////////////////

template <class S, class F, class P, class H, int N>
int TConsistentHashingRing<S, F, P, H, N>::GetSize() const
{
    return Ring_.size();
}

template <class S, class F, class P, class H, int N>
int TConsistentHashingRing<S, F, P, H, N>::GetTokenCount() const
{
    return TokenRing_.size();
}

template <class S, class F, class P, class H, int N>
void TConsistentHashingRing<S, F, P, H, N>::AddServer(typename NMpl::TCallTraits<S>::TType server, int tokenCount)
{
    YT_VERIFY(tokenCount > 0);

    for (auto [it, ite] = NPrivate::GetItemRange<H, S, TServerToken>(server, tokenCount); it != ite; ++it) {
        auto [hash, serverToken] = *it;
        EmplaceOrCrash(Ring_, hash, serverToken);
        EmplaceOrCrash(TokenRing_, hash, serverToken);
    }
}

template <class S, class F, class P, class H, int N>
void TConsistentHashingRing<S, F, P, H, N>::RemoveServer(typename NMpl::TCallTraits<S>::TType server, int tokenCount)
{
    YT_VERIFY(tokenCount > 0);

    for (auto [it, ite] = NPrivate::GetItemRange<H, S, TServerToken>(server, tokenCount); it != ite; ++it) {
        auto [hash, serverToken] = *it;
        EraseOrCrash(Ring_, std::pair(hash, serverToken));
        EraseOrCrash(TokenRing_, std::pair(hash, serverToken));
    }
}

template <class S, class F, class P, class H, int N>
void TConsistentHashingRing<S, F, P, H, N>::AddFile(typename NMpl::TCallTraits<F>::TType file, int replicaCount)
{
    for (auto [it, ite] = NPrivate::GetItemRange<H, F, TFileReplica>(file, replicaCount); it != ite; ++it) {
        auto [hash, fileReplica] = *it;
        EmplaceOrCrash(Ring_, hash, fileReplica);
    }
}

template <class S, class F, class P, class H, int N>
void TConsistentHashingRing<S, F, P, H, N>::RemoveFile(typename NMpl::TCallTraits<F>::TType file, int replicaCount)
{
    for (auto [it, ite] = NPrivate::GetItemRange<H, F, TFileReplica>(file, replicaCount); it != ite; ++it) {
        auto [hash, fileReplica] = *it;
        EraseOrCrash(Ring_, std::pair(hash, fileReplica));
    }
}

template <class S, class F, class P, class H, int N>
typename TConsistentHashingRing<S, F, P, H, N>::TFileRange
TConsistentHashingRing<S, F, P, H, N>::GetFileRangeForServer(typename NMpl::TCallTraits<S>::TType server, int tokenCount) const
{
    YT_VERIFY(tokenCount > 0);

    return {
        TFileRangeIterator(*this, NPrivate::GetItemRange<H, S, TServerToken>(server, tokenCount)),
        TFileRangeEndIterator()};
}

template <class S, class F, class P, class H, int N>
TCompactVector<S, N> TConsistentHashingRing<S, F, P, H, N>::GetServersForFile(typename NMpl::TCallTraits<F>::TType file, int replicaCount) const
{
    auto targetTokens = GetTokensForFile(file, replicaCount);
    TCompactVector<S, N> result;
    result.reserve(targetTokens.size());
    std::transform(
        targetTokens.begin(),
        targetTokens.end(),
        std::back_inserter(result),
        [] (const TServerToken& token) {
            return token.Server;
        });
    return result;
}

template <class S, class F, class P, class H, int N>
TCompactVector<typename TConsistentHashingRing<S, F, P, H, N>::TServerToken, N>
TConsistentHashingRing<S, F, P, H, N>::GetTokensForFile(typename NMpl::TCallTraits<F>::TType file, int replicaCount) const
{
    using TResult = TCompactVector<typename TConsistentHashingRing<S, F, P, H, N>::TServerToken, N>;

    if (TokenRing_.empty()) {
        return {};
    }

    TResult result;
    result.reserve(replicaCount);

    for (auto [hashIt, hashIte] = NPrivate::GetHashRange<H, F>(file, replicaCount); hashIt != hashIte; ++hashIt) {
        auto tokenIt = TokenRing_.upper_bound(*hashIt);
        if (tokenIt == TokenRing_.begin()) {
            tokenIt = --TokenRing_.end();
        } else {
            --tokenIt;
        }

        result.push_back(tokenIt->second);
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
