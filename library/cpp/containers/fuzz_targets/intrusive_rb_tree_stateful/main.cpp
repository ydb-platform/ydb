#include <library/cpp/containers/intrusive_rb_tree/rb_tree.h>

#include <util/system/types.h>
#include <util/system/yassert.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <vector>

namespace {

constexpr size_t MaxNodes = 64;

class TNode;

struct TCmp {
    static bool Compare(const TNode& l, const TNode& r) noexcept;
    static bool Compare(const TNode& l, int r) noexcept;
    static bool Compare(int l, const TNode& r) noexcept;
};

class TNode: public TRbTreeItem<TNode, TCmp> {
public:
    int Key = 0;
    size_t Id = 0;
};

bool TCmp::Compare(const TNode& l, const TNode& r) noexcept {
    return l.Key < r.Key;
}

bool TCmp::Compare(const TNode& l, int r) noexcept {
    return l.Key < r;
}

bool TCmp::Compare(int l, const TNode& r) noexcept {
    return l < r.Key;
}

using TTree = TRbTree<TNode, TCmp>;

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

struct TEntry {
    int Key = 0;
    size_t Id = 0;
};

struct TRbStats {
    size_t Count = 0;
    size_t BlackHeight = 0;
};

int ReadKey(TInput& input) noexcept {
    return static_cast<int>(input.Next() % 129) - 64;
}

void SortModel(std::vector<TEntry>& model) {
    std::sort(model.begin(), model.end(), [](const TEntry& l, const TEntry& r) {
        if (l.Key != r.Key) {
            return l.Key < r.Key;
        }
        return l.Id < r.Id;
    });
}

void RemoveId(std::vector<TEntry>& model, size_t id) {
    model.erase(std::remove_if(model.begin(), model.end(), [id](const TEntry& entry) {
        return entry.Id == id;
    }), model.end());
}

size_t LowerIndex(const std::vector<TEntry>& model, int key) {
    return static_cast<size_t>(std::lower_bound(model.begin(), model.end(), key, [](const TEntry& entry, int value) {
        return entry.Key < value;
    }) - model.begin());
}

size_t UpperIndex(const std::vector<TEntry>& model, int key) {
    return static_cast<size_t>(std::upper_bound(model.begin(), model.end(), key, [](int value, const TEntry& entry) {
        return value < entry.Key;
    }) - model.begin());
}

TRbStats CheckRbSubtree(const TNode* node) {
    if (node == nullptr) {
        return {0, 1};
    }

    const auto* left = static_cast<const TNode*>(node->Left_);
    const auto* right = static_cast<const TNode*>(node->Right_);

    if (left != nullptr) {
        Y_ABORT_UNLESS(left->Parent_ == node);
        Y_ABORT_UNLESS(left->Key <= node->Key);
    }
    if (right != nullptr) {
        Y_ABORT_UNLESS(right->Parent_ == node);
        Y_ABORT_UNLESS(node->Key <= right->Key);
    }

    if (!node->Color_) {
        Y_ABORT_UNLESS(left == nullptr || left->Color_);
        Y_ABORT_UNLESS(right == nullptr || right->Color_);
    }

    const TRbStats leftStats = CheckRbSubtree(left);
    const TRbStats rightStats = CheckRbSubtree(right);
    Y_ABORT_UNLESS(leftStats.BlackHeight == rightStats.BlackHeight);

    const size_t count = 1 + leftStats.Count + rightStats.Count;
    Y_ABORT_UNLESS(node->Children_ == count);

    return {count, leftStats.BlackHeight + (node->Color_ ? 1 : 0)};
}

void CheckBounds(const TTree& tree, const std::vector<TEntry>& model, int key) {
    auto* const end = tree.End().Node_;

    auto* const lower = tree.LowerBound(key);
    const size_t lowerIndex = LowerIndex(model, key);
    if (lowerIndex == model.size()) {
        Y_ABORT_UNLESS(lower == end);
    } else {
        Y_ABORT_UNLESS(lower != end);
        Y_ABORT_UNLESS(static_cast<const TNode*>(lower)->Key == model[lowerIndex].Key);
    }

    auto* const upper = tree.UpperBound(key);
    const size_t upperIndex = UpperIndex(model, key);
    if (upperIndex == model.size()) {
        Y_ABORT_UNLESS(upper == end);
    } else {
        Y_ABORT_UNLESS(upper != end);
        Y_ABORT_UNLESS(static_cast<const TNode*>(upper)->Key == model[upperIndex].Key);
    }
}

void CheckTree(TTree& tree, const std::vector<TEntry>& model) {
    Y_ABORT_UNLESS(tree.Empty() == model.empty());

    std::vector<const TNode*> traversal;
    traversal.reserve(model.size());

    for (auto it = tree.Begin(); it != tree.End(); ++it) {
        if (!traversal.empty()) {
            Y_ABORT_UNLESS(traversal.back()->Key <= it->Key);
        }
        traversal.push_back(&*it);
    }

    Y_ABORT_UNLESS(traversal.size() == model.size());
    for (size_t i = 0; i < traversal.size(); ++i) {
        Y_ABORT_UNLESS(traversal[i]->Key == model[i].Key);
        Y_ABORT_UNLESS(tree.ByIndex(i) == traversal[i]);
        Y_ABORT_UNLESS(tree.GetIndex(const_cast<TNode*>(traversal[i])) == i);
    }

    for (int key = -65; key <= 65; ++key) {
        const size_t less = LowerIndex(model, key);
        const size_t notGreater = UpperIndex(model, key);
        Y_ABORT_UNLESS(tree.LessCount(key) == less);
        Y_ABORT_UNLESS(tree.GreaterCount(key) == model.size() - notGreater);
        CheckBounds(tree, model, key);

        if (!model.empty()) {
            Y_ABORT_UNLESS(tree.NotLessCount(key) == model.size() - less);
            Y_ABORT_UNLESS(tree.NotGreaterCount(key) == notGreater);
        }

        const TNode* const found = tree.Find(key);
        if (less != model.size() && model[less].Key == key) {
            Y_ABORT_UNLESS(found != nullptr);
            Y_ABORT_UNLESS(found->Key == key);
        } else {
            Y_ABORT_UNLESS(found == nullptr);
        }
    }

    if (model.empty()) {
        Y_ABORT_UNLESS(tree.End().Node_->Parent_ == nullptr);
        return;
    }

    const auto* root = static_cast<const TNode*>(tree.End().Node_->Parent_);
    Y_ABORT_UNLESS(root != nullptr);
    Y_ABORT_UNLESS(root->Parent_ == tree.End().Node_);
    Y_ABORT_UNLESS(root->Color_);
    Y_ABORT_UNLESS(CheckRbSubtree(root).Count == model.size());
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    std::array<TNode, MaxNodes> nodes;
    std::array<bool, MaxNodes> active{};
    TTree tree;
    std::vector<TEntry> model;

    for (size_t i = 0; i < nodes.size(); ++i) {
        nodes[i].Id = i;
    }

    TInput input{data, size, 0};
    size_t operations = 0;
    while (!input.Empty() && operations++ < 4096) {
        const ui8 op = input.Next();
        const size_t id = input.Next() % MaxNodes;
        const int key = ReadKey(input);

        switch (op % 9) {
            case 0:
                if (active[id]) {
                    tree.Erase(&nodes[id]);
                    RemoveId(model, id);
                    active[id] = false;
                }
                nodes[id].Key = key;
                tree.Insert(&nodes[id]);
                active[id] = true;
                model.push_back({key, id});
                SortModel(model);
                break;

            case 1:
                if (active[id]) {
                    tree.Insert(&nodes[id]);
                }
                break;

            case 2:
                if (active[id]) {
                    tree.Erase(&nodes[id]);
                    RemoveId(model, id);
                    active[id] = false;
                }
                break;

            case 3:
                if (TNode* found = tree.Find(key)) {
                    active[found->Id] = false;
                    RemoveId(model, found->Id);
                    tree.Erase(found);
                }
                break;

            case 4:
                (void)tree.Find(key);
                break;

            case 5:
                (void)tree.LowerBound(key);
                break;

            case 6:
                (void)tree.UpperBound(key);
                break;

            case 7:
                if (!model.empty()) {
                    (void)tree.ByIndex(input.Next() % model.size());
                }
                break;

            case 8:
                tree.Clear();
                active.fill(false);
                model.clear();
                break;
        }

        CheckTree(tree, model);
    }

    return 0;
}
