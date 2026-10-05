#include <library/cpp/testing/gtest/gtest.h>

#include <library/cpp/yt/containers/intrusive_linked_list.h>

#include <algorithm>
#include <deque>
#include <memory>
#include <vector>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

struct TItem
{
    explicit TItem(int value)
        : Value(value)
    { }

    int Value;
    TIntrusiveLinkedListNode<TItem> Node;
};

struct TItemToNode
{
    TIntrusiveLinkedListNode<TItem>* operator () (TItem* item) const
    {
        return &item->Node;
    }
};

using TList = TIntrusiveLinkedList<TItem, TItemToNode>;

////////////////////////////////////////////////////////////////////////////////

std::vector<int> ToVectorForward(const TList& list)
{
    std::vector<int> result;
    for (auto* item = list.GetFront(); item; item = item->Node.Next) {
        result.push_back(item->Value);
    }
    return result;
}

std::vector<int> ToVectorBackward(const TList& list)
{
    std::vector<int> result;
    for (auto* item = list.GetBack(); item; item = item->Node.Prev) {
        result.push_back(item->Value);
    }
    return result;
}

void ExpectContents(const TList& list, const std::vector<int>& expected)
{
    EXPECT_EQ(list.GetSize(), std::ssize(expected));

    auto forward = ToVectorForward(list);
    EXPECT_EQ(forward, expected);

    auto backward = ToVectorBackward(list);
    std::reverse(backward.begin(), backward.end());
    EXPECT_EQ(backward, expected);

    if (expected.empty()) {
        EXPECT_EQ(list.GetFront(), nullptr);
        EXPECT_EQ(list.GetBack(), nullptr);
    } else {
        EXPECT_EQ(list.GetFront()->Value, expected.front());
        EXPECT_EQ(list.GetBack()->Value, expected.back());
        EXPECT_EQ(list.GetFront()->Node.Prev, nullptr);
        EXPECT_EQ(list.GetBack()->Node.Next, nullptr);
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TIntrusiveLinkedListTest, Empty)
{
    TList list;
    ExpectContents(list, {});
}

TEST(TIntrusiveLinkedListTest, PushBack)
{
    TItem a(1);
    TItem b(2);
    TItem c(3);

    TList list;
    list.PushBack(&a);
    ExpectContents(list, {1});
    list.PushBack(&b);
    ExpectContents(list, {1, 2});
    list.PushBack(&c);
    ExpectContents(list, {1, 2, 3});
}

TEST(TIntrusiveLinkedListTest, PushFront)
{
    TItem a(1);
    TItem b(2);
    TItem c(3);

    TList list;
    list.PushFront(&a);
    ExpectContents(list, {1});
    list.PushFront(&b);
    ExpectContents(list, {2, 1});
    list.PushFront(&c);
    ExpectContents(list, {3, 2, 1});
}

TEST(TIntrusiveLinkedListTest, PushMixed)
{
    TItem a(1);
    TItem b(2);
    TItem c(3);
    TItem d(4);

    TList list;
    list.PushBack(&a);
    list.PushFront(&b);
    list.PushBack(&c);
    list.PushFront(&d);
    ExpectContents(list, {4, 2, 1, 3});
}

TEST(TIntrusiveLinkedListTest, PopFront)
{
    TItem a(1);
    TItem b(2);
    TItem c(3);

    TList list;
    list.PushBack(&a);
    list.PushBack(&b);
    list.PushBack(&c);

    list.PopFront();
    ExpectContents(list, {2, 3});
    list.PopFront();
    ExpectContents(list, {3});
    list.PopFront();
    ExpectContents(list, {});
}

TEST(TIntrusiveLinkedListTest, PopBack)
{
    TItem a(1);
    TItem b(2);
    TItem c(3);

    TList list;
    list.PushBack(&a);
    list.PushBack(&b);
    list.PushBack(&c);

    list.PopBack();
    ExpectContents(list, {1, 2});
    list.PopBack();
    ExpectContents(list, {1});
    list.PopBack();
    ExpectContents(list, {});
}

TEST(TIntrusiveLinkedListTest, RemoveSole)
{
    TItem a(1);

    TList list;
    list.PushBack(&a);
    list.Remove(&a);
    ExpectContents(list, {});
}

TEST(TIntrusiveLinkedListTest, RemoveFront)
{
    TItem a(1);
    TItem b(2);
    TItem c(3);

    TList list;
    list.PushBack(&a);
    list.PushBack(&b);
    list.PushBack(&c);

    list.Remove(&a);
    ExpectContents(list, {2, 3});
}

TEST(TIntrusiveLinkedListTest, RemoveBack)
{
    TItem a(1);
    TItem b(2);
    TItem c(3);

    TList list;
    list.PushBack(&a);
    list.PushBack(&b);
    list.PushBack(&c);

    list.Remove(&c);
    ExpectContents(list, {1, 2});
}

TEST(TIntrusiveLinkedListTest, RemoveMiddle)
{
    TItem a(1);
    TItem b(2);
    TItem c(3);

    TList list;
    list.PushBack(&a);
    list.PushBack(&b);
    list.PushBack(&c);

    list.Remove(&b);
    ExpectContents(list, {1, 3});
}

TEST(TIntrusiveLinkedListTest, RemoveAll)
{
    TItem a(1);
    TItem b(2);
    TItem c(3);

    TList list;
    list.PushBack(&a);
    list.PushBack(&b);
    list.PushBack(&c);

    list.Remove(&b);
    list.Remove(&c);
    list.Remove(&a);
    ExpectContents(list, {});
}

TEST(TIntrusiveLinkedListTest, ReinsertAfterRemove)
{
    TItem a(1);
    TItem b(2);

    TList list;
    list.PushBack(&a);
    list.PushBack(&b);

    list.Remove(&a);
    ExpectContents(list, {2});

    list.PushBack(&a);
    ExpectContents(list, {2, 1});
}

TEST(TIntrusiveLinkedListTest, Clear)
{
    TItem a(1);
    TItem b(2);

    TList list;
    list.PushBack(&a);
    list.PushBack(&b);

    list.Clear();
    ExpectContents(list, {});

    list.PushFront(&b);
    ExpectContents(list, {2});
}

TEST(TIntrusiveLinkedListTest, StatefulItemToNode)
{
    struct TCountingItemToNode
    {
        explicit TCountingItemToNode(int calls = 0)
            : Calls(std::make_shared<int>(calls))
        { }

        TIntrusiveLinkedListNode<TItem>* operator () (TItem* item) const
        {
            ++*Calls;
            return &item->Node;
        }

        std::shared_ptr<int> Calls;
    };

    TCountingItemToNode itemToNode;
    TIntrusiveLinkedList<TItem, TCountingItemToNode> list(itemToNode);

    TItem a(1);
    TItem b(2);
    list.PushBack(&a);
    list.PushBack(&b);

    EXPECT_EQ(list.GetSize(), 2);
    EXPECT_GT(*itemToNode.Calls, 0);
}

TEST(TIntrusiveLinkedListTest, RandomOperations)
{
    constexpr int ItemCount = 100;
    constexpr int IterationCount = 20000;

    std::vector<std::unique_ptr<TItem>> pool;
    pool.reserve(ItemCount);
    for (int index = 0; index < ItemCount; ++index) {
        pool.push_back(std::make_unique<TItem>(index));
    }

    std::vector<TItem*> free;
    for (const auto& item : pool) {
        free.push_back(item.get());
    }

    TList list;
    std::deque<TItem*> mirror;

    auto checkConsistency = [&] {
        std::vector<int> expected;
        for (auto* item : mirror) {
            expected.push_back(item->Value);
        }
        ExpectContents(list, expected);
    };

    ui64 state = 42;
    auto next = [&] (int bound) {
        state = state * 6364136223846793005ULL + 1442695040888963407ULL;
        return static_cast<int>((state >> 33) % bound);
    };

    for (int iteration = 0; iteration < IterationCount; ++iteration) {
        int op = next(100);
        if (op < 30 && !free.empty()) {
            auto* item = free.back();
            free.pop_back();
            list.PushBack(item);
            mirror.push_back(item);
        } else if (op < 60 && !free.empty()) {
            auto* item = free.back();
            free.pop_back();
            list.PushFront(item);
            mirror.push_front(item);
        } else if (op < 70 && !mirror.empty()) {
            free.push_back(mirror.front());
            mirror.pop_front();
            list.PopFront();
        } else if (op < 80 && !mirror.empty()) {
            free.push_back(mirror.back());
            mirror.pop_back();
            list.PopBack();
        } else if (op < 99 && !mirror.empty()) {
            int index = next(std::ssize(mirror));
            auto* item = mirror[index];
            mirror.erase(mirror.begin() + index);
            free.push_back(item);
            list.Remove(item);
        } else {
            for (auto* item : mirror) {
                free.push_back(item);
            }
            mirror.clear();
            list.Clear();
        }

        if (iteration % 100 == 0) {
            checkConsistency();
        }
    }

    checkConsistency();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
