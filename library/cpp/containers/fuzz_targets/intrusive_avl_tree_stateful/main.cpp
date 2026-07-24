#include <util/generic/noncopyable.h>
#include <util/system/types.h>
#include <util/system/yassert.h>

#define private public
#include <library/cpp/containers/intrusive_avl_tree/avltree.h>
#undef private

#include <algorithm>
#include <array>
#include <cstddef>
#include <vector>

namespace {

constexpr size_t MaxNodes = 64;

class TNode;

struct TCmp {
    static bool Compare(const TNode& l, const TNode& r) noexcept;
};

class TNode: public TAvlTreeItem<TNode, TCmp> {
public:
    explicit TNode(int key = 0) noexcept
        : Key(key)
    {
    }

    int Key = 0;
};

bool TCmp::Compare(const TNode& l, const TNode& r) noexcept {
    return l.Key < r.Key;
}

using TTree = TAvlTree<TNode, TCmp>;

struct TInput {
    const ui8* Data = nullptr;
    size_t Size = 0;
    size_t Pos = 0;

    ui8 Next(ui8 fallback = 0) noexcept {
        return Pos < Size ? Data[Pos++] : fallback;
    }

    bool Empty() const noexcept {
        return Pos >= Size;
    }
};

struct TAvlStats {
    size_t Count = 0;
    long Height = 0;
    const TNode* Min = nullptr;
    const TNode* Max = nullptr;
};

int ReadKey(TInput& input) noexcept {
    return static_cast<int>(input.Next() % 129) - 64;
}

bool Contains(const std::vector<int>& model, int key) {
    return std::binary_search(model.begin(), model.end(), key);
}

void InsertModel(std::vector<int>& model, int key) {
    model.insert(std::lower_bound(model.begin(), model.end(), key), key);
}

void RemoveModel(std::vector<int>& model, int key) {
    const auto it = std::lower_bound(model.begin(), model.end(), key);
    Y_ABORT_UNLESS(it != model.end() && *it == key);
    model.erase(it);
}

size_t LowerIndex(const std::vector<int>& model, int key) {
    return static_cast<size_t>(std::lower_bound(model.begin(), model.end(), key) - model.begin());
}

size_t UpperIndex(const std::vector<int>& model, int key) {
    return static_cast<size_t>(std::upper_bound(model.begin(), model.end(), key) - model.begin());
}

TAvlStats CheckAvlSubtree(const TTree& tree, const TNode* node, const TNode* parent, const int* minKey, const int* maxKey) {
    if (node == nullptr) {
        return {};
    }

    Y_ABORT_UNLESS(node->Parent_ == parent);
    Y_ABORT_UNLESS(node->Tree_ == &tree);
    if (minKey != nullptr) {
        Y_ABORT_UNLESS(*minKey < node->Key);
    }
    if (maxKey != nullptr) {
        Y_ABORT_UNLESS(node->Key < *maxKey);
    }

    const auto* left = static_cast<const TNode*>(node->Left_);
    const auto* right = static_cast<const TNode*>(node->Right_);

    const TAvlStats leftStats = CheckAvlSubtree(tree, left, node, minKey, &node->Key);
    const TAvlStats rightStats = CheckAvlSubtree(tree, right, node, &node->Key, maxKey);

    const long expectedHeight = std::max(leftStats.Height, rightStats.Height) + 1;
    Y_ABORT_UNLESS(node->Height_ == expectedHeight);

    const long balance = leftStats.Height - rightStats.Height;
    Y_ABORT_UNLESS(balance >= -1 && balance <= 1);

    return {
        1 + leftStats.Count + rightStats.Count,
        expectedHeight,
        leftStats.Min ? leftStats.Min : node,
        rightStats.Max ? rightStats.Max : node,
    };
}

void CheckTree(TTree& tree, const std::vector<int>& model) {
    Y_ABORT_UNLESS(tree.Empty() == model.empty());

    std::vector<int> traversal;
    traversal.reserve(model.size());
    for (const TNode& node : tree) {
        traversal.push_back(node.Key);
    }
    Y_ABORT_UNLESS(traversal == model);

    for (int key = -65; key <= 65; ++key) {
        TNode probe(key);

        TNode* const found = tree.Find(&probe);
        const bool expectedFound = Contains(model, key);
        Y_ABORT_UNLESS((found != nullptr) == expectedFound);
        if (found != nullptr) {
            Y_ABORT_UNLESS(found->Key == key);
        }

        TNode* const lower = tree.LowerBound(&probe);
        const size_t lowerIndex = LowerIndex(model, key);
        if (lowerIndex == model.size()) {
            Y_ABORT_UNLESS(lower == nullptr);
        } else {
            Y_ABORT_UNLESS(lower != nullptr);
            Y_ABORT_UNLESS(lower->Key == model[lowerIndex]);
        }

        TNode upperProbe(key + 1);
        TNode* const upper = tree.LowerBound(&upperProbe);
        const size_t upperIndex = UpperIndex(model, key);
        if (upperIndex == model.size()) {
            Y_ABORT_UNLESS(upper == nullptr);
        } else {
            Y_ABORT_UNLESS(upper != nullptr);
            Y_ABORT_UNLESS(upper->Key == model[upperIndex]);
        }
    }

    if (model.empty()) {
        Y_ABORT_UNLESS(tree.Root_ == nullptr);
        Y_ABORT_UNLESS(tree.Head_ == nullptr);
        Y_ABORT_UNLESS(tree.Tail_ == nullptr);
        return;
    }

    Y_ABORT_UNLESS(tree.Root_ != nullptr);
    Y_ABORT_UNLESS(tree.Root_->Parent_ == nullptr);
    const TAvlStats stats = CheckAvlSubtree(tree, static_cast<const TNode*>(tree.Root_), nullptr, nullptr, nullptr);
    Y_ABORT_UNLESS(stats.Count == model.size());
    Y_ABORT_UNLESS(stats.Min == static_cast<const TNode*>(tree.Head_));
    Y_ABORT_UNLESS(stats.Max == static_cast<const TNode*>(tree.Tail_));
    Y_ABORT_UNLESS(static_cast<const TNode*>(tree.Head_)->Key == model.front());
    Y_ABORT_UNLESS(static_cast<const TNode*>(tree.Tail_)->Key == model.back());
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    std::array<TNode, MaxNodes> nodes;
    std::array<bool, MaxNodes> active{};
    TTree tree;
    std::vector<int> model;

    TInput input{data, size, 0};
    size_t operations = 0;
    while (!input.Empty() && operations++ < 4096) {
        const ui8 op = input.Next();
        const size_t id = input.Next() % MaxNodes;
        const int key = ReadKey(input);

        switch (op % 8) {
            case 0:
                if (!Contains(model, key)) {
                    if (active[id]) {
                        tree.Erase(&nodes[id]);
                        RemoveModel(model, nodes[id].Key);
                        active[id] = false;
                    }
                    nodes[id].Key = key;
                    Y_ABORT_UNLESS(tree.Insert(&nodes[id]) == &nodes[id]);
                    active[id] = true;
                    InsertModel(model, key);
                }
                break;

            case 1:
                if (active[id]) {
                    tree.Erase(&nodes[id]);
                    RemoveModel(model, nodes[id].Key);
                    active[id] = false;
                }
                break;

            case 2: {
                TNode probe(key);
                if (TNode* found = tree.Find(&probe)) {
                    tree.Erase(found);
                    RemoveModel(model, found->Key);
                    for (size_t i = 0; i < nodes.size(); ++i) {
                        if (&nodes[i] == found) {
                            active[i] = false;
                            break;
                        }
                    }
                }
                break;
            }

            case 3: {
                TNode probe(key);
                (void)tree.Find(&probe);
                break;
            }

            case 4: {
                TNode probe(key);
                (void)tree.LowerBound(&probe);
                break;
            }

            case 5: {
                TNode probe(key + 1);
                (void)tree.LowerBound(&probe);
                break;
            }

            case 6:
                for (auto it = tree.Begin(); it != tree.End(); ++it) {
                    (void)it->Key;
                }
                break;

            case 7:
                tree.Clear();
                active.fill(false);
                model.clear();
                break;
        }

        CheckTree(tree, model);
    }

    return 0;
}
