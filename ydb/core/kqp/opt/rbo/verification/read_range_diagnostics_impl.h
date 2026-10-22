#pragma once

#include "semantic_snapshot.h"
#include <ydb/core/kqp/opt/rbo/kqp_operator.h>
#include <yql/essentials/ast/yql_expr.h>
#include <util/generic/hash.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <algorithm>

namespace NKikimr::NKqp::NDetail {

// Failure-only evidence. Never reconstruct a predicate or participate in admission.
// Avoid generic AST/type printers: clipping their completed output is not bounded work.
class TDerivedIndexRangeDiagnostic {
    using TExprNode = NYql::TExprNode;
    using TTypeAnnotationNode = NYql::TTypeAnnotationNode;
    static constexpr size_t MaxBytes = 16 * 1024;
    static constexpr size_t FooterBytes = 512;
    static constexpr size_t MaxNodes = 192;
    static constexpr size_t MaxEdges = 512;
    static constexpr size_t MaxDepth = 24;
    static constexpr size_t MaxFragment = 96;
    static constexpr size_t MaxKeys = 32;
    static constexpr size_t MaxColumnScan = 128;
    enum ETruncation : ui32 {
        Text = 1, Fragment = 2, Nodes = 4, Edges = 8, Depth = 16,
        Types = 32, TypeEdges = 64, TypeDepth = 128, Metadata = 256,
        TypeDetails = 512,
    };

public:
    static TString Render(const TOpRead& read, const TSemanticSnapshotCatalogTableV1& table,
                          const TOpRead::TRangeInfo& range) {
        TDerivedIndexRangeDiagnostic out;
        out.Buffer.reserve(MaxBytes);
        out.Put(" ; derived_index_range_v1{storage=");
        out.Put(read.StorageType == NYql::EStorageType::RowStorage ? "RowStorage" : "other");
        out.Put(";used_prefix="); out.Number(range.UsedPrefixLen);
        out.Put(";point_prefix="); out.Number(range.PointPrefixLen);
        out.Put(";expected_max_ranges=");
        if (range.ExpectedMaxRanges) { out.Number(*range.ExpectedMaxRanges); } else { out.Put("absent"); }
        out.Put(";selected=");
        out.Put(read.StorageType == NYql::EStorageType::RowStorage && range.LiteralRange
            ? "LiteralRange" : range.ComputeNode ? "ComputeNode" : "Void");
        // Selection and both root summaries precede expandable metadata and either tree.
        out.Put(";LiteralRange_root="); out.Root(range.LiteralRange.Get());
        out.Put(";ComputeNode_root="); out.Root(range.ComputeNode.Get());
        out.Put(";table="); out.Quote(table.Name);
        out.Put(";derived_from=");
        if (table.DerivedFrom) { out.Quote(*table.DerivedFrom); } else { out.Put("absent"); }
        out.Put(";physical_key=");
        if (table.PrimaryKey) { out.Keys(*table.PrimaryKey, &table); } else { out.Put("absent"); }
        out.Put(";range_keys="); out.Keys(range.KeyColumns, nullptr);
        out.Put(";LiteralRange="); out.Node(range.LiteralRange.Get(), 0);
        out.Put(";ComputeNode="); out.Node(range.ComputeNode.Get(), 0);
        out.Put("}");
        // Reserve this independently of all expandable text. Even a mid-token cutoff is explicit.
        out.Buffer += TStringBuilder() << " ; END derived_index_range_v1 truncated=" << bool(out.Truncation)
            << " flags=" << out.Truncation << " expr_nodes=" << out.NodeIds.size()
            << " expr_edges=" << out.NodeEdges << " type_nodes=" << out.TypeIds.size()
            << " type_edges=" << out.TypeEdgeCount;
        return out.Buffer;
    }

private:
    bool Room() const { return Buffer.size() < MaxBytes - FooterBytes; }

    void Put(TStringBuf value) {
        const size_t count = std::min(value.size(), MaxBytes - FooterBytes - Buffer.size());
        Buffer.append(value.data(), count);
        if (count != value.size()) { Truncation |= Text; }
    }

    void Number(ui64 value) { Put(ToString(value)); }

    void Cut(ETruncation reason, TStringBuf marker) { Truncation |= reason; Put(marker); }

    void Quote(TStringBuf value) {
        static constexpr char Hex[] = "0123456789abcdef";
        Put("\"");
        for (size_t i = 0; i < std::min(value.size(), MaxFragment) && Room(); ++i) {
            const auto byte = static_cast<unsigned char>(value[i]);
            if (byte >= 0x20 && byte <= 0x7e && byte != '"' && byte != '\\') {
                const char character = static_cast<char>(byte);
                Put(TStringBuf(&character, 1));
            } else {
                const char escaped[] = {'\\', 'x', Hex[byte >> 4], Hex[byte & 15]};
                Put(TStringBuf(escaped, 4));
            }
        }
        Put("\"");
        if (value.size() > MaxFragment) {
            Cut(Fragment, "<fragment-cut:bytes="); Number(value.size()); Put(">");
        }
    }

    void Keys(const TVector<TString>& keys, const TSemanticSnapshotCatalogTableV1* table) {
        Put("count="); Number(keys.size()); Put("[");
        for (size_t i = 0; i < std::min(keys.size(), MaxKeys) && Room(); ++i) {
            if (i) { Put(","); }
            Quote(keys[i]);
            if (!table) { continue; }
            const TSemanticSnapshotCatalogColumnV1* found = nullptr;
            // Bound the search as well as the rendered columns; long names cannot cause long comparisons.
            if (keys[i].size() <= MaxFragment) {
                for (size_t j = 0; j < std::min(table->Columns.size(), MaxColumnScan); ++j) {
                    const auto& column = table->Columns[j];
                    if (column.Name.size() <= MaxFragment && column.Name == keys[i]) { found = &column; break; }
                }
            }
            if (found) {
                Put(":type="); Quote(found->Type); Put(":nullable="); Number(found->Nullable);
            } else {
                Put(":type=unresolved");
                if (keys[i].size() > MaxFragment || table->Columns.size() > MaxColumnScan) {
                    Cut(Metadata, "<column-search-cut>");
                }
            }
        }
        if (keys.size() > MaxKeys) { Cut(Metadata, "<keys-cut>"); }
        Put("]");
    }

    static TStringBuf Kind(const TExprNode& node) {
        switch (node.Type()) {
            case TExprNode::Atom: return "Atom";
            case TExprNode::List: return "List";
            case TExprNode::Callable: return "Callable";
            case TExprNode::Lambda: return "Lambda";
            case TExprNode::Argument: return "Argument";
            case TExprNode::Arguments: return "Arguments";
            case TExprNode::World: return "World";
        }
        return "unknown";
    }

    void Root(const TExprNode* node) {
        if (!node) { Put("absent"); return; }
        Put("present("); Put(Kind(*node)); Put(","); Quote(node->Content());
        Put(",children="); Number(node->ChildrenSize()); Put(")");
    }

    void Node(const TExprNode* node, size_t depth) {
        if (!Room()) { Truncation |= Text; return; }
        if (!node) { Put("absent"); return; }
        if (depth > MaxDepth) { Cut(Depth, "<depth-cut>"); return; }
        if (const auto found = NodeIds.find(node); found != NodeIds.end()) {
            Put("@n"); Number(found->second); return;
        }
        if (NodeIds.size() == MaxNodes) { Cut(Nodes, "<nodes-cut>"); return; }
        const size_t id = NodeIds.size();
        NodeIds.emplace(node, id);
        Put("n"); Number(id); Put("{"); Put(Kind(*node)); Put(","); Quote(node->Content());
        Put(";type="); Type(node->GetTypeAnn(), 0);
        Put(";children="); Number(node->ChildrenSize()); Put("[");
        for (size_t i = 0; i < node->ChildrenSize() && Room(); ++i) {
            if (NodeEdges == MaxEdges) { Cut(Edges, "<edges-cut>"); break; }
            ++NodeEdges;
            if (i) { Put(","); }
            Node(node->Child(i), depth + 1);
            if (Truncation & Nodes) { break; }
        }
        Put("]}");
    }

    template <class TItems>
    void TypeItems(const TItems& items, size_t depth) {
        Put("count="); Number(items.size()); Put("[");
        for (size_t i = 0; i < items.size() && Room(); ++i) {
            if (i) { Put(","); }
            if (!TypeChild(items[i], depth)) { break; }
        }
        Put("]");
    }

    bool TypeChild(const TTypeAnnotationNode* type, size_t depth) {
        if (TypeEdgeCount == MaxEdges) { Cut(TypeEdges, "<type-edges-cut>"); return false; }
        ++TypeEdgeCount;
        Type(type, depth + 1);
        return !(Truncation & Types) && Room();
    }

    void Type(const TTypeAnnotationNode* type, size_t depth) {
        using namespace NYql;
        if (!Room()) { Truncation |= Text; return; }
        if (!type) { Put("unannotated"); return; }
        if (depth > MaxDepth) { Cut(TypeDepth, "<type-depth-cut>"); return; }
        if (const auto found = TypeIds.find(type); found != TypeIds.end()) {
            Put("@t"); Number(found->second); return;
        }
        if (TypeIds.size() == MaxNodes) { Cut(Types, "<types-cut>"); return; }
        const size_t id = TypeIds.size();
        TypeIds.emplace(type, id);
        Put("t"); Number(id); Put("=");
        Put(ToString(type->GetKind())); Put("(");
        switch (type->GetKind()) {
            case ETypeAnnotationKind::Data: {
                const auto* data = type->Cast<TDataExprType>();
                Quote(data->GetName());
                if (const auto* params = dynamic_cast<const TDataExprParamsType*>(data)) {
                    Put(",param1="); Quote(params->GetParamOne()); Put(",param2="); Quote(params->GetParamTwo());
                }
                break;
            }
            case ETypeAnnotationKind::Optional: TypeChild(type->Cast<TOptionalExprType>()->GetItemType(), depth); break;
            case ETypeAnnotationKind::List: TypeChild(type->Cast<TListExprType>()->GetItemType(), depth); break;
            case ETypeAnnotationKind::Stream: TypeChild(type->Cast<TStreamExprType>()->GetItemType(), depth); break;
            case ETypeAnnotationKind::Flow: TypeChild(type->Cast<TFlowExprType>()->GetItemType(), depth); break;
            case ETypeAnnotationKind::Type: TypeChild(type->Cast<TTypeExprType>()->GetType(), depth); break;
            case ETypeAnnotationKind::Tuple: TypeItems(type->Cast<TTupleExprType>()->GetItems(), depth); break;
            case ETypeAnnotationKind::Struct: TypeItems(type->Cast<TStructExprType>()->GetItems(), depth); break;
            case ETypeAnnotationKind::Item:
                Quote(type->Cast<TItemExprType>()->GetName()); Put(",");
                TypeChild(type->Cast<TItemExprType>()->GetItemType(), depth); break;
            case ETypeAnnotationKind::Void:
            case ETypeAnnotationKind::Null:
            case ETypeAnnotationKind::Unit:
            case ETypeAnnotationKind::World: break;
            default: Cut(TypeDetails, "<type-details-omitted>"); break;
        }
        Put(")");
    }

    TString Buffer;
    ui32 Truncation = 0;
    size_t NodeEdges = 0;
    size_t TypeEdgeCount = 0;
    THashMap<const TExprNode*, size_t> NodeIds;
    THashMap<const TTypeAnnotationNode*, size_t> TypeIds;
};

} // namespace NKikimr::NKqp::NDetail
