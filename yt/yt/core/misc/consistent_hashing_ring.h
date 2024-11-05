#pragma once

#include "public.h"

#include "mpl.h"

#include <library/cpp/yt/small_containers/compact_vector.h>

#include <variant>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! A device that tries to establish a consistent matching of a set of 'files' to a set of 'servers'.
/*!
   The algorithm tries to keep the number of files to relocate in case of a
   server outage (i.e. the server having been removed from the ring) manageable.
   This is achieved via placing ("marking") a server on the ring not once, but
   multiple times at multiple places (such individual mark is called a "token").

   Similarly, placing several replicas of a file on the ring is explicitly
   supported (an individual mark is called a "replica").

   Template parameter description:
   - S is server;
   - F is file;
   - P is comparator. It must be able to compare servers and files:
       p(s, s) -> bool
       p(f, f) -> bool
     where p, s and f are instances of P, S and F, respectively.
   - H is hasher. It must be able to produce a series of hashes from a server or a file:
       h(s, i) -> ui64
       h(f, i) -> ui64
     where i is an int, and h, s and f are instances of H, S and F, respectively.
     (NB: care should be taken to avoid serial hash collisions. That is, if
       h(s1, 0) == h(s2, 0) it should be likely that h(s1, 1) != h(s2, 1).
     This is the reason why a hasher is called with both and object to hash and
     an additional sequence number.)

   TODO(shakurov): express requirements to P and H as C++ concepts.
*/
template <class S, class F, class P, class H, int N>
class TConsistentHashingRing
{
public:
    class TFileRangeIterator;
    class TFileRangeEndIterator;
    using TFileRange = std::pair<TFileRangeIterator, TFileRangeEndIterator>;

    int GetSize() const;
    int GetTokenCount() const;

    //! Places #tokenCount tokens for this server on the ring.
    /*!
     *  The server must not have been added yet.
     *
     *  /note Adding a server is likely to change files-server associations.
     *  Consider calling #GetFileRangeForServer immediately after to identify
     *  affected files.
     */
    void AddServer(typename NMpl::TCallTraits<S>::TType server, int tokenCount);

    //! Removes server tokens from the ring.
    /*!
     *  The server must have been previously added (and not subsequently removed).
     *  Token count must be exactly the same as the number of tokens that have been added.
     *
     *  /note Removing a server is likely to change files-server associations.
     *  Consider calling #GetFileRangeForServer immediately before removal
     *  to identify affected files.
     */
    void RemoveServer(typename NMpl::TCallTraits<S>::TType server, int tokenCount);

    //! Places file replicas on the ring.
    void AddFile(typename NMpl::TCallTraits<F>::TType file, int replicaCount);

    //! Removes file replicas from the ring. #replicaCount must be exactly the
    //! same as has been previously added.
    void RemoveFile(typename NMpl::TCallTraits<F>::TType file, int replicaCount);

    //! Returns an iterator over a set of files matched to the specified server
    //! (according the current state of the ring).
    /*!
     *  The returned range doesn't distinguish same-file replicas and thus may
     *  produce duplicate files.
     *
     *  /note The range is invalidated if any change is made to the ring.
     */
    TFileRange GetFileRangeForServer(typename NMpl::TCallTraits<S>::TType server, int tokenCount) const;

    //! Returns the list of servers to which replicas of the specified files are
    //! matched (according the current state of the ring).
    /*!
     *  /note May return duplicates.
     */
    TCompactVector<S, N> GetServersForFile(typename NMpl::TCallTraits<F>::TType file, int replicaCount) const;

private:
    struct TServerToken
    {
        S Server;
        int Index;

        bool operator<(const TServerToken& rhs) const;
    };

    struct TFileReplica
    {
        F File;
        int Index;

        bool operator<(const TFileReplica& rhs) const;
    };

    // The order is important: files must be "greater than" servers so that, in
    // case a file and a server hash to the same value, the former is associated
    // with the latter (and not with some other server).
    using TRingItem = std::variant<TServerToken, TFileReplica>;

    // Naked hash is always less then corresponding items.
    template <class TItem>
    struct TRingCompare
    {
        using is_transparent = void;

        bool operator()(const std::pair<ui64, TItem>& lhs, const std::pair<ui64, TItem>& rhs) const;
        bool operator()(ui64 lhs, const std::pair<ui64, TItem>& rhs) const;
        bool operator()(const std::pair<ui64, TItem>& lhs, ui64 rhs) const;
    };

    using TRing = std::set<std::pair<ui64, TRingItem>, TRingCompare<TRingItem>>;
    using TServerTokenRing = std::set<std::pair<ui64, TServerToken>, TRingCompare<TServerToken>>;

    TCompactVector<TServerToken, N> GetTokensForFile(typename NMpl::TCallTraits<F>::TType file, int replicaCount) const;

    TRing Ring_;
    TServerTokenRing TokenRing_; // No, not *that* token ring :-)
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define CONSISTENT_HASHING_RING_INL_H_
#include "consistent_hashing_ring-inl.h"
#undef CONSISTENT_HASHING_RING_INL_H_
