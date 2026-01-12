#include "trie.h"

#include "tokenizer.h"
#include "helpers.h"

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/range_helpers.h>

#include <library/cpp/yt/misc/variant.h>

namespace NYT::NYPath {
namespace {

////////////////////////////////////////////////////////////////////////////////

static const std::string LeafString;
static const TStringBuf LeafSentinel = LeafString;
static const std::string AfterLeafString;
static const TStringBuf AfterLeafSentinel = AfterLeafString;
static const std::string OutlierString;
static const TStringBuf OutlierSentinel = OutlierString;

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

std::vector<TStringBuf> TokenizeTriePath(const TYPath &path Y_LIFETIME_BOUND)
{
    std::vector<TStringBuf> tokenizedPath;
    TTokenizer tokenizer(path);
    tokenizer.Expect(ETokenType::StartOfStream);
    tokenizer.Advance();

    while (tokenizer.GetType() != ETokenType::EndOfStream) {
        tokenizer.Expect(ETokenType::Slash);
        tokenizer.Advance();
        tokenizer.Expect(ETokenType::Literal);
        tokenizedPath.push_back(tokenizer.GetToken());
        tokenizer.Advance();
    }

    return tokenizedPath;
}

////////////////////////////////////////////////////////////////////////////////

const TTrieView::TNode TTrieView::TNode::AfterLeafSentinel;

////////////////////////////////////////////////////////////////////////////////

TTrieView::TTrieView()
    : ViewState_(&OutlierSentinel)
{ }

TTrieView TTrieView::Visit(TStringBuf token) const
{
    TTrieView view(*this);
    view.VisitInplace(token);
    return view;
}

// Returns an owned buffer for a copy of the `token` unless GetType() becomes `ETrieNodeType::Outlier`
// or `ETrieNode::AfterLeaf` after the call. Helpful for maintaining visited tokens without allocations,
// since after the call, the argument token could be deallocated. Instead, the wrapping code should use `ownedToken`.
TStringBuf TTrieView::VisitInplace(TStringBuf token)
{
    TStringBuf ownedToken;

    NYT::Visit(ViewState_,
        [token, &ownedToken] (const TStringBuf*& nextToken) {
            if (nextToken->begin() == AfterLeafSentinel.begin() || nextToken->begin() == OutlierSentinel.begin()) {
                return;
            } else if (nextToken->begin() == LeafSentinel.begin()) {
                ++nextToken;
            } else if (*nextToken == token) {
                ownedToken = *nextToken;
                ++nextToken;
            } else {
                nextToken = &OutlierSentinel;
            }
        },
        [token, &ownedToken] (const TNode*& currentNode) {
            if (currentNode == nullptr || currentNode == &TNode::AfterLeafSentinel) {
                return;
            }

            auto nextNode = currentNode->Next.find(token);
            if (nextNode != currentNode->Next.end()) {
                currentNode = nextNode->second;
                ownedToken = nextNode->first;
            } else if (currentNode->Next.empty()) {
                currentNode = &TNode::AfterLeafSentinel;
            } else {
                currentNode = nullptr;
            }
        });
    return ownedToken;
}

TTrieView TTrieView::VisitPath(const TYPath& path) const
{
    auto view = *this;
    for (auto token : TokenizeTriePath(path)) {
        view.VisitInplace(token);
    }
    return view;
}

ETrieNodeType TTrieView::GetType() const
{
    return NYT::Visit(ViewState_,
        [] (const TStringBuf* nextToken) {
            if (nextToken->begin() == OutlierSentinel.begin()) {
                return ETrieNodeType::Outlier;
            } else if (nextToken->begin() == AfterLeafSentinel.begin()) {
                return ETrieNodeType::AfterLeaf;
            }

            return nextToken->begin() == LeafSentinel.begin()
                ? ETrieNodeType::Leaf
                : ETrieNodeType::Intermediary;
        },
        [] (const TNode* currentNode) {
            if (currentNode == nullptr) {
                return ETrieNodeType::Outlier;
            } else if (currentNode == &TNode::AfterLeafSentinel) {
                return ETrieNodeType::AfterLeaf;
            }

            return currentNode->Next.empty()
                ? ETrieNodeType::Leaf
                : ETrieNodeType::Intermediary;
        });
}

TTrieTraversalFrame TTrieView::Traverse() const
{
    return TTrieTraversalFrame(*this);
}

std::vector<TYPath> TTrieView::GetNestedPaths(TYPath prefix) const
{
    if (GetType() == ETrieNodeType::Leaf) {
        return {prefix};
    }

    if (GetType() != ETrieNodeType::Intermediary) {
        return {};
    }

    std::vector<TYPath> nestedPaths;
    for (auto token : Traverse().GetUnvisitedTokens()) {
        nestedPaths = ConcatVectors(nestedPaths, Visit(token).GetNestedPaths(YPathJoin(prefix, token)));
    }

    return nestedPaths;
}

////////////////////////////////////////////////////////////////////////////////

TTrie::TTrie(const std::vector<TYPath>& paths, bool enableFlattening, ETrieNestedPathPolicy policy)
    : NestedPathPolicy_(policy)
    , State_(enableFlattening && std::ssize(paths) < 2
        ? TState{std::vector<TStringBuf>{}}
        : std::deque<TNode>{})
    , OwnedPaths_(paths.begin(), paths.end())
{
    YT_VERIFY(OutlierSentinel.begin() != AfterLeafSentinel.begin());

    for (const auto& path : OwnedPaths_) {
        AddPathUnsafe(TokenizeTriePath(path));
    }

    UpdateRoot();
}

TTrie::TTrie(const TTrie& other)
    : TTrie(
        RangeTo<std::vector<TYPath>>(other.OwnedPaths_),
        /*enableFlattening*/ other.IsFlat(),
        other.NestedPathPolicy_)
{ }

TTrie& TTrie::operator=(const TTrie& other)
{
    Clear(/*enableFlattening*/ other.IsFlat());
    Merge(other);
    NestedPathPolicy_ = other.NestedPathPolicy_;
    return *this;
}

void TTrie::AddPath(TYPath path)
{
    OwnedPaths_.push_back(std::move(path));
    AddPathUnsafe(TokenizeTriePath(OwnedPaths_.back()));
    UpdateRoot();
}


void TTrie::Merge(const TTrie& other)
{
    for (const auto& path : other.OwnedPaths_) {
        AddPath(path);
    }
}

void TTrie::Clear(bool enableFlattening)
{
    OwnedPaths_.clear();
    State_ = enableFlattening ? TState{std::vector<TStringBuf>{}} : std::deque<TNode>{};
}

bool TTrie::IsEmpty() const
{
    return GetType() == ETrieNodeType::Outlier;
}

bool TTrie::IsFlat() const
{
    return std::get_if<std::vector<TStringBuf>>(&State_);
}

void TTrie::AddPathUnsafe(std::vector<TStringBuf> tokenizedPath)
{
    auto addNodePath = [this] (std::deque<TNode>& nodes, std::vector<TStringBuf> tokenizedPath) {
        bool createdNode = false;

        if (nodes.empty()) {
            nodes.emplace_back();
            createdNode = true;
        }

        auto* current = &nodes.front();
        for (auto token : tokenizedPath) {
            if (!createdNode && current->Next.empty() && NestedPathPolicy_ == ETrieNestedPathPolicy::Shorten) {
                break;
            }

            if (auto iter = current->Next.find(token); iter != current->Next.end()) {
                current = iter->second;
            } else {
                nodes.emplace_back();
                createdNode = true;
                current->Next.emplace(token, &nodes.back());
                current = &nodes.back();
            }
        }

        if (NestedPathPolicy_ == ETrieNestedPathPolicy::Shorten) {
            current->Next.clear();
        }
    };

    if (auto state = std::get_if<std::vector<TStringBuf>>(&State_); state && !state->empty()) {
        auto tokenizedPath = std::move(*state);
        tokenizedPath.pop_back();
        tokenizedPath.pop_back();
        addNodePath(State_.emplace<std::deque<TNode>>(), std::move(tokenizedPath));
    }

    NYT::Visit(State_,
        [&tokenizedPath] (std::vector<TStringBuf>& tokens) {
            tokens = std::move(tokenizedPath);
            tokens.push_back(LeafSentinel);
            tokens.push_back(AfterLeafSentinel);
        },
        [&tokenizedPath, &addNodePath] (std::deque<TNode>& nodes) {
            addNodePath(nodes, std::move(tokenizedPath));
        });
}

void TTrie::UpdateRoot()
{
    ViewState_ = NYT::Visit(State_,
        [] (std::vector<TStringBuf>& tokens) -> TTrieView::TState{
            if (tokens.empty()) {
                return &OutlierSentinel;
            } else {
                return tokens.data();
            }
        },
        [] (std::deque<TNode>& nodes) -> TTrieView::TState{
            if (nodes.empty()) {
                return static_cast<TNode*>(nullptr);
            } else {
                return &nodes.front();
            }
        });
}
////////////////////////////////////////////////////////////////////////////////

TTrieTraversalFrame::TTrieTraversalFrame(TTrieView trie)
    : View_(trie)
{ }

TTrieView TTrieTraversalFrame::Visit(TStringBuf token)
{
    auto view = View_;
    auto ownedToken = view.VisitInplace(token);
    if (view.GetType() != ETrieNodeType::Outlier && view.GetType() != ETrieNodeType::AfterLeaf) {
        VisitedTokens_.insert(ownedToken);
    }
    return view;
}

TCompactVector<TStringBuf, 1> TTrieTraversalFrame::GetUnvisitedTokens() const
{
    if (View_.GetType() != ETrieNodeType::Intermediary) {
        return {};
    }

    return NYT::Visit(View_.ViewState_,
        [this] (const TStringBuf* nextToken) {
            return VisitedTokens_.empty()
                ? TCompactVector<TStringBuf, 1>{*nextToken}
                : TCompactVector<TStringBuf, 1>{};
        },
        [this] (const TTrieView::TNode* currentNode) -> TCompactVector<TStringBuf, 1> {
            TCompactVector<TStringBuf, 1> unvisitedTokens;
            if (currentNode->Next.size() == VisitedTokens_.size()) {
                return unvisitedTokens;
            }

            for (const auto& [token, _] : currentNode->Next) {
                if (!VisitedTokens_.contains(ToString(token))) {
                    unvisitedTokens.push_back(token);
                }
            }

            return unvisitedTokens;
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYPath
