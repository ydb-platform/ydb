#pragma once

#include "ydb/public/api/protos/ydb_value.pb.h"
#include "yql_errors.h"

#include <library/cpp/deprecated/enum_codegen/enum_codegen.h>
#include <util/generic/ptr.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/stream/output.h>
#include <util/stream/str.h>
#include <util/memory/pool.h>
#include <util/generic/array_ref.h>

namespace NYql {

struct TNodeFlags {
    enum : ui16 {
        Default = 0,
        ArbitraryContent = 0x01,
        BinaryContent = 0x02,
        MultilineContent = 0x04,
    };

    static constexpr ui32 FlagsMask = 0x07; // all flags should fit here
};

struct TAstNode {
#define YQL_AST_NODE_TYPE_MAP(xx) \
    xx(List, 0) \
    xx(Atom, 1) \

    enum EType : ui32 {
        YQL_AST_NODE_TYPE_MAP(ENUM_VALUE_GEN)
    };


    static const ui32 SmallListCount = 2;

    void PrintTo(IOutputStream& out) const;
    void PrettyPrintTo(IOutputStream& out, ui32 prettyFlags) const;

    inline TString ToString() const {
        TStringStream str;
        PrintTo(str);
        return str.Str();
    }

    inline TString ToString(ui32 prettyFlags) const {
        TStringStream str;
        PrettyPrintTo(str, prettyFlags);
        return str.Str();
    }

    inline EType GetType() const {
        return Type;
    }

    inline bool IsAtom() const {
        return Type == Atom;
    }

    inline bool IsList() const {
        return Type == List;
    }

    inline bool IsListOfSize(ui32 len) const {
        return Type == List && ListCount == len;
    }

    inline TPosition GetPosition() const {
        return Position;
    }

    inline void SetPosition(TPosition position) {
        Position = position;
    }

    inline TStringBuf GetContent() const {
        Y_ABORT_UNLESS(IsAtom());
        return TStringBuf(Data.A.Content, Data.A.Size);
    }

    inline void SetContent(TStringBuf newContent, TMemoryPool& pool) {
        Y_ABORT_UNLESS(IsAtom());
        auto poolContent = pool.AppendString(newContent);
        Data.A.Content = poolContent.data();
        Data.A.Size = poolContent.size();
    }

    inline void SetLiteralContent(TStringBuf newContent) {
        Y_ABORT_UNLESS(IsAtom());
        Data.A.Content = newContent.data();
        Data.A.Size = newContent.size();
    }

    inline ui32 GetFlags() const {
        Y_ABORT_UNLESS(IsAtom());
        return Data.A.Flags;
    }

    inline void SetFlags(ui32 flags) {
        Y_ABORT_UNLESS(IsAtom());
        Data.A.Flags = flags;
    }

    inline ui32 GetChildrenCount() const {
        Y_ABORT_UNLESS(IsList());
        return ListCount;
    }

    inline const TAstNode* GetChild(ui32 index) const {
        Y_ABORT_UNLESS(IsList());
        Y_ABORT_UNLESS(index < ListCount);
        if (ListCount <= SmallListCount) {
            return Data.S.Children[index];
        } else {
            return Data.L.Children[index];
        }
    }

    inline TAstNode* GetChild(ui32 index) {
        Y_ABORT_UNLESS(IsList());
        Y_ABORT_UNLESS(index < ListCount);
        if (ListCount <= SmallListCount) {
            return Data.S.Children[index];
        } else {
            return Data.L.Children[index];
        }
    }
    
    inline TArrayRef<TAstNode* const> GetChildren() const {
        Y_ABORT_UNLESS(IsList());
        return {ListCount <= SmallListCount ? Data.S.Children : Data.L.Children, ListCount};
    }

    static inline TAstNode* NewAtom(TPosition position, TStringBuf content, TMemoryPool& pool, ui32 flags = TNodeFlags::Default) {
        auto poolContent = pool.AppendString(content);
        auto ret = pool.Allocate<TAstNode>();
        ::new(ret) TAstNode(position, poolContent, flags);
        return ret;
    }

    // atom with non-owning content, useful for literal strings
    static inline TAstNode* NewLiteralAtom(TPosition position, TStringBuf content, TMemoryPool& pool, ui32 flags = TNodeFlags::Default) {
        auto ret = pool.Allocate<TAstNode>();
        ::new(ret) TAstNode(position, content, flags);
        return ret;
    }

    static inline TAstNode* NewList(TPosition position, TAstNode** children, ui32 childrenCount, TMemoryPool& pool) {
        TAstNode** poolChildren = nullptr;
        if (childrenCount) {
            if (childrenCount > SmallListCount) {
                poolChildren = pool.AllocateArray<TAstNode*>(childrenCount);
                memcpy(poolChildren, children, sizeof(void*) * childrenCount);
            } else {
                poolChildren = children;
            }

            for (ui32 index = 0; index < childrenCount; ++index) {
                Y_ABORT_UNLESS(poolChildren[index]);
            }
        }

        auto ret = pool.Allocate<TAstNode>();
        ::new(ret) TAstNode(position, poolChildren, childrenCount);
        return ret;
    }

    template <typename... TNodes>
    static inline TAstNode* NewList(TPosition position, TMemoryPool& pool, TNodes... nodes) {
        TAstNode* children[] = { nodes... };
        return NewList(position, children, sizeof...(nodes), pool);
    }

    static inline TAstNode* NewList(TPosition position, TMemoryPool& pool) {
        return NewList(position, nullptr, 0, pool);
    }

    static TAstNode QuoteAtom;

    static inline TAstNode* Quote(TPosition position, TMemoryPool& pool, TAstNode* node) {
        return NewList(position, pool, &QuoteAtom, node);
    }

    inline ~TAstNode() {}

    void Destroy() {
        TString().swap(Position.File);
    }

private:
    inline TAstNode(TPosition position, TStringBuf content, ui32 flags)
        : Position(position)
        , Type(Atom)
        , ListCount(0)
    {
        Data.A.Content = content.data();
        Data.A.Size = content.size();
        Data.A.Flags = flags;
    }

    inline TAstNode(TPosition position, TAstNode** children, ui32 childrenCount)
        : Position(position)
        , Type(List)
        , ListCount(childrenCount)
    {
        if (childrenCount <= SmallListCount) {
            for (ui32 index = 0; index < childrenCount; ++index) {
                Data.S.Children[index] = children[index];
            }
        } else {
            Data.L.Children = children;
        }
    }

    TPosition Position;
    const EType Type;
    const ui32 ListCount;

    struct TAtom {
        const char* Content;
        ui32 Size;
        ui32 Flags;
    };

    struct TListType {
        TAstNode** Children;
    };

    struct TSmallList {
        TAstNode* Children[SmallListCount];
    };

    union {
        TAtom A;
        TListType L;
        TSmallList S;
    } Data;
};

enum class ESyntaxType {
    YQLv0,
    YQLv1,
    Pg,
};

struct TAstParseResult {
    std::unique_ptr<TMemoryPool> Pool;
    TAstNode* Root = nullptr;
    TIssues Issues;
    TMaybe<THashMap<TString, Ydb::TypedValue>> PgAutoParamValues = Nothing();
    ESyntaxType ActualSyntaxType = ESyntaxType::YQLv1;

    inline bool IsOk() const {
        return !!Root;
    }

    TAstParseResult() = default;
    ~TAstParseResult();
    TAstParseResult(const TAstParseResult&) = delete;
    TAstParseResult& operator=(const TAstParseResult&) = delete;

    TAstParseResult(TAstParseResult&&);
    TAstParseResult& operator=(TAstParseResult&&);

    void Destroy();
};

struct TStmtParseInfo {
    bool KeepInCache = true;
    TMaybe<TString> CommandTagName = {};
};

struct TAstPrintFlags {
    enum {
        Default = 0,
        PerLine = 0x01,
        ShortQuote = 0x02,
        AdaptArbitraryContent = 0x04,
    };
};

TAstParseResult ParseAst(const TStringBuf& str, TMemoryPool* externalPool = nullptr, const TString& file = {});

} // namespace NYql

template<>
void Out<NYql::TAstNode::EType>(class IOutputStream &o, NYql::TAstNode::EType x);
