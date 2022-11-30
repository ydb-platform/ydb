#pragma once

#include <util/generic/ptr.h>
#include <util/generic/vector.h>
#include <cstdio>
#include <util/generic/noncopyable.h>

struct TPrefixTreeNodeElement {
    const char* Key;
    i32 KeyLen;
    i32 Val;
    i32 Index;

    TPrefixTreeNodeElement();
    TPrefixTreeNodeElement(const char*, i32, i32, i32);
};

class TPrefixTreeNode {
public:
    TVector<TPrefixTreeNodeElement> Elements;
    TPrefixTreeNode();

    int Find(char) const;
    void Set(const char*, i32, i32, i32);
    void Dump(FILE*) const;
};

class TPrefixTree : TNonCopyable {
private:
    static const i32 INDEX_BOUND = 1 << 30;

    TVector<TPrefixTreeNode> Nodes;

public:
    void Init(int);
    TPrefixTree(int);

    void Add(const char*, i32);
    i32 MinPrefixIndex(const char*) const;
    void Clear();
    void Dump(FILE*) const;
    int GetMemorySize() const;
    void Compress();

private:
    void AddInternal(const char*, TPrefixTreeNode&, i32);
};
