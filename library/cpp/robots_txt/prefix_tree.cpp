#include <cstring>
#include <algorithm>

#include "prefix_tree.h"

TPrefixTreeNodeElement::TPrefixTreeNodeElement()
    : Key(nullptr)
    , KeyLen(0)
    , Val(-1)
    , Index(-1)
{
}

TPrefixTreeNodeElement::TPrefixTreeNodeElement(const char* key, i32 keyLen = 0, i32 val = -1, i32 index = -1)
    : Key(key)
    , KeyLen(keyLen)
    , Val(val)
    , Index(index)
{
}

TPrefixTreeNode::TPrefixTreeNode()
    : Elements()
{
}

int TPrefixTreeNode::Find(char ch) const {
    for (size_t i = 0; i < Elements.size(); ++i)
        if (ch == *(Elements[i].Key))
            return i;
    return -1;
}

void TPrefixTreeNode::Set(const char* key, i32 keyLen, i32 val, i32 index) {
    TPrefixTreeNodeElement element(key, keyLen, val, index);
    int i = Find(*key);
    if (i < 0)
        Elements.push_back(element);
    else
        Elements[i] = element;
}

void TPrefixTreeNode::Dump(FILE* logFile) const {
    if (!logFile)
        logFile = stderr;
    fprintf(logFile, "size=%" PRISZT "\n", Elements.size());
    static char b[1234];
    for (size_t i = 0; i < Elements.size(); ++i) {
        strncpy(b, Elements[i].Key, Elements[i].KeyLen);
        b[Elements[i].KeyLen] = 0;
        fprintf(logFile, "{key=[%s]:%d, val=%d, index=%d}\n", b, Elements[i].KeyLen, Elements[i].Val, Elements[i].Index);
    }
}

void TPrefixTree::Dump(FILE* logFile) const {
    if (!logFile)
        logFile = stderr;
    fprintf(logFile, "%" PRISZT " nodes\n", Nodes.size());
    for (size_t i = 0; i < Nodes.size(); ++i) {
        fprintf(logFile, "%" PRISZT ": ", i);
        Nodes[i].Dump(logFile);
        fprintf(logFile, "\n");
    }
}

TPrefixTree::TPrefixTree(int maxSize) {
    Init(maxSize);
}

void TPrefixTree::Init(int maxSize) {
    Nodes.clear();
    Nodes.reserve(std::max(maxSize + 1, 1));
    Nodes.push_back(TPrefixTreeNode());
}

void TPrefixTree::Clear() {
    Nodes.clear();
    Init(0);
}

void TPrefixTree::Add(const char* s, i32 index) {
    AddInternal(s, Nodes[0], index);
}

void TPrefixTree::AddInternal(const char* s, TPrefixTreeNode& node, i32 index) {
    if (!s || !*s)
        return;

    int i = node.Find(*s);
    if (i >= 0) {
        TPrefixTreeNodeElement& d = node.Elements[i];
        const char* p = d.Key;
        while (*s && (p - d.Key) < d.KeyLen && *s == *p)
            ++s, ++p;

        if (*s) {
            if ((p - d.Key) < d.KeyLen) {
                Nodes.push_back(TPrefixTreeNode());
                Nodes.back().Set(p, d.KeyLen - (p - d.Key), d.Val, d.Index);
                Nodes.back().Set(s, strlen(s), -1, index);

                d.Val = Nodes.size() - 1;
                d.KeyLen = p - d.Key;
                d.Index = INDEX_BOUND;
            } else {
                if (d.Val != -1 && index < d.Index)
                    AddInternal(s, Nodes[d.Val], index);
            }
        } else {
            if ((p - d.Key) < d.KeyLen) {
                Nodes.push_back(TPrefixTreeNode());
                Nodes.back().Set(p, d.KeyLen - (p - d.Key), d.Val, d.Index);
                d.Val = Nodes.size() - 1;
                d.KeyLen = p - d.Key;
                d.Index = index;
            } else {
                d.Index = std::min(d.Index, index);
            }
        }
    } else {
        node.Set(s, strlen(s), -1, index);
    }
}

int TPrefixTree::GetMemorySize() const {
    int res = Nodes.capacity() * sizeof(TPrefixTreeNode);
    for (size_t i = 0; i < Nodes.size(); ++i)
        res += Nodes[i].Elements.capacity() * sizeof(TPrefixTreeNodeElement);
    return res;
}

void TPrefixTree::Compress() {
    Nodes.shrink_to_fit();
    for (size_t i = 0; i < Nodes.size(); ++i)
        Nodes[i].Elements.shrink_to_fit();
}

i32 TPrefixTree::MinPrefixIndex(const char* s) const {
    if (!*s)
        return -1;
    int i = Nodes[0].Find(*s);
    if (i < 0)
        return -1;
    const TPrefixTreeNodeElement* d = &Nodes[0].Elements[i];

    const char* p = d->Key;
    if (!p || !*p)
        return -1;

    i32 result = INDEX_BOUND;
    i32 nodeIndex = 0;
    while (*s == *p) {
        if (++p - d->Key >= d->KeyLen)
            result = std::min(result, d->Index);
        if (!*++s)
            break;

        if (p - d->Key >= d->KeyLen) {
            nodeIndex = d->Val;
            if (nodeIndex == -1)
                break;
            i = Nodes[nodeIndex].Find(*s);
            if (i < 0)
                break;
            d = &Nodes[nodeIndex].Elements[i];
            p = d->Key;
            if (!p || !*p)
                break;
        }
    }
    return result < INDEX_BOUND ? result : -1;
}
