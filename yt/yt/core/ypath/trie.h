#pragma once

#include "public.h"

#include <library/cpp/yt/compact_containers/compact_set.h>

namespace NYT::NYPath {

////////////////////////////////////////////////////////////////////////////////

std::vector<TStringBuf> TokenizeTriePath(const TYPath &path Y_LIFETIME_BOUND);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETrieNodeType,
    ((Outlier)      (0))
    ((Intermediary) (1))
    ((Leaf)         (2))
    ((AfterLeaf)    (3))
);

DEFINE_ENUM(ETrieNestedPathPolicy,
    ((Shorten)    (0))
    ((Extend)     (1))
);

////////////////////////////////////////////////////////////////////////////////

class TTrieView
{
public:
    TTrieView();

    TTrieView Visit(TStringBuf token) const;
    TTrieView VisitPath(const TYPath& path) const;

    TTrieTraversalFrame Traverse() const;

    std::vector<TYPath> GetNestedPaths(TYPath prefix = {}) const;

    ETrieNodeType GetType() const;

protected:
    friend class TTrieTraversalFrame;

    struct TNode
    {
        TCompactFlatMap<TStringBuf, TNode*, 1> Next;

        static const TNode AfterLeafSentinel;
    };

    using TState = std::variant<const TStringBuf*, const TNode*>;

    TState ViewState_;

    TStringBuf VisitInplace(TStringBuf token);
};

class TTrie
    : public TTrieView
{
public:
    explicit TTrie(
        const std::vector<TYPath>& paths = {},
        bool enableFlattening = true,
        ETrieNestedPathPolicy policy = ETrieNestedPathPolicy::Shorten);

    TTrie(const TTrie& other);
    TTrie& operator=(const TTrie& other);
    TTrie(TTrie&&) = default;
    TTrie& operator=(TTrie&&) = default;

    void AddPath(TYPath path);
    void Merge(const TTrie& other);
    void Clear(bool enableFlattening = true);

    bool IsEmpty() const;

private:
    using TState = std::variant<std::vector<TStringBuf>, std::deque<TTrieView::TNode>>;

    ETrieNestedPathPolicy NestedPathPolicy_;
    TState State_;
    std::deque<TYPath> OwnedPaths_;

    void AddPathUnsafe(std::vector<TStringBuf> tokenizedPath);
    void UpdateRoot();

    bool IsFlat() const;
};

class TTrieTraversalFrame
{
public:
    explicit TTrieTraversalFrame(TTrieView trie);

    TTrieView Visit(TStringBuf token);
    TCompactVector<TStringBuf, 1> GetUnvisitedTokens() const;

private:
    TTrieView View_;
    TCompactSet<TStringBuf, 1> VisitedTokens_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYPath
