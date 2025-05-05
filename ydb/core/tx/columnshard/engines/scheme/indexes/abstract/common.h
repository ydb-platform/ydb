#pragma once
#include <ydb/library/accessor/accessor.h>

#include <util/digest/fnv.h>
#include <util/digest/numeric.h>
#include <util/generic/refcount.h>
#include <util/generic/string.h>

namespace NKikimr::NOlap::NIndexes::NRequest {

enum class ENodeType : ui32 {
    Aggregation,
    OriginalColumn,
    SubColumn,
    Root,
    Operation,
    Constant
};

class TOriginalDataAddress {
private:
    YDB_READONLY(ui32, ColumnId, 0);
    YDB_READONLY_DEF(TString, SubColumnName);

public:
    static ui64 CalcSubColumnHash(const std::string_view sv);

    static ui64 CalcSubColumnHash(const TString& path) {
        return CalcSubColumnHash(std::string_view(path.data(), path.size()));
    }

    explicit TOriginalDataAddress(const ui32 columnId, const TString& subColumnName = "")
        : ColumnId(columnId)
        , SubColumnName(subColumnName) {
    }

    bool operator<(const TOriginalDataAddress& item) const {
        return std::tie(ColumnId, SubColumnName) < std::tie(item.ColumnId, item.SubColumnName);
    }

    bool operator==(const TOriginalDataAddress& item) const {
        return std::tie(ColumnId, SubColumnName) == std::tie(item.ColumnId, item.SubColumnName);
    }

    explicit operator size_t() const {
        if (SubColumnName) {
            return CombineHashes<ui64>(ColumnId, FnvHash<ui64>(SubColumnName.data(), SubColumnName.size()));
        } else {
            return ColumnId;
        }
    }

    TString DebugString() const;
};

class TNodeId {
private:
    YDB_READONLY(ui32, ColumnId, 0);
    YDB_READONLY_DEF(TString, SubColumnName);
    YDB_READONLY(ui32, GenerationId, 0);
    YDB_READONLY(ENodeType, NodeType, ENodeType::OriginalColumn);

    static inline TAtomicCounter Counter = 0;

    TNodeId(const ui32 columnId, const ui32 generationId, const ENodeType type)
        : ColumnId(columnId)
        , GenerationId(generationId)
        , NodeType(type) {
    }

public:
    bool operator==(const TNodeId& item) const {
        return ColumnId == item.ColumnId && GenerationId == item.GenerationId && NodeType == item.NodeType &&
               SubColumnName == item.SubColumnName;
    }

    TOriginalDataAddress BuildOriginalDataAddress() const;

    TNodeId BuildCopy() const {
        return TNodeId(ColumnId, Counter.Inc(), NodeType);
    }

    TString ToString() const;

    static TNodeId RootNodeId() {
        return TNodeId(0, 0, ENodeType::Root);
    }

    static TNodeId Constant(const ui32 columnId) {
        return TNodeId(columnId, Counter.Inc(), ENodeType::Constant);
    }

    static TNodeId Original(const ui32 columnId, const TString& subColumnName = "");

    static TNodeId Aggregation() {
        return TNodeId(0, Counter.Inc(), ENodeType::Aggregation);
    }

    static TNodeId Operation(const ui32 columnId) {
        return TNodeId(columnId, Counter.Inc(), ENodeType::Operation);
    }

    bool operator<(const TNodeId& item) const {
        return std::tie(ColumnId, GenerationId, NodeType, SubColumnName) <
               std::tie(item.ColumnId, item.GenerationId, item.NodeType, item.SubColumnName);
    }
};

}   // namespace NKikimr::NOlap::NIndexes::NRequest
