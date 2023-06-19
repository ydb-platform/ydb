#include "yql_lineage.h"
#include <ydb/library/yql/core/yql_type_annotation.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/core/yql_join.h>

#include <util/system/env.h>

namespace NYql {

namespace {

class TLineageScanner {
public:
    TLineageScanner(const TExprNode& root, const TTypeAnnotationContext& ctx)
        : Root_(root)
        , Ctx_(ctx)
    {}

    TString Process() {
        VisitExpr(Root_, [&](const TExprNode& node) {
            for (auto& p : Ctx_.DataSources) {
                if (p->IsRead(node)) {
                    Reads_[&node] = p.Get();
                }
            }

            for (auto& p : Ctx_.DataSinks) {
                if (p->IsWrite(node)) {
                    Writes_[&node] = p.Get();
                }
            }

            return true;
        });

        TStringStream s;
        NYson::TYsonWriter writer(&s, NYson::EYsonFormat::Text);
        writer.OnBeginMap();
        writer.OnKeyedItem("Reads");
        writer.OnBeginList();
        for (const auto& r : Reads_) {
            TVector<TPinInfo> inputs;
            r.second->GetPlanFormatter().GetInputs(*r.first, inputs);
            for (const auto& i : inputs) {
                auto id = ++NextReadId_;
                ReadIds_[r.first].push_back(id);
                writer.OnListItem();
                writer.OnBeginMap();
                writer.OnKeyedItem("Id");
                writer.OnInt64Scalar(id);
                writer.OnKeyedItem("Name");
                writer.OnStringScalar(i.DisplayName);
                writer.OnKeyedItem("Schema");
                WriteSchema(writer, *r.first->GetTypeAnn()->Cast<TTupleExprType>()->GetItems()[1]->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>());
                writer.OnEndMap();
            }
        }

        writer.OnEndList();
        writer.OnKeyedItem("Writes");
        writer.OnBeginList();
        for (const auto& w : Writes_) {
            auto data = w.first->Child(3);
            TVector<TPinInfo> outputs;
            w.second->GetPlanFormatter().GetOutputs(*w.first, outputs);
            YQL_ENSURE(outputs.size() == 1);
            auto id = ++NextWriteId_;
            WriteIds_[w.first] = id;
            writer.OnListItem();
            writer.OnBeginMap();
            writer.OnKeyedItem("Id");
            writer.OnInt64Scalar(id);
            writer.OnKeyedItem("Name");
            writer.OnStringScalar(outputs.front().DisplayName);
            writer.OnKeyedItem("Schema");
            WriteSchema(writer, *data->GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>());
            writer.OnKeyedItem("Lineage");
            auto lineage = CollectLineage(*data);
            WriteLineage(writer, *lineage);
            writer.OnEndMap();
        }

        writer.OnEndList();
        writer.OnEndMap();
        return s.Str();
    }

private:
    void WriteSchema(NYson::TYsonWriter& writer, const TStructExprType& structType) {
        writer.OnBeginMap();
        for (const auto& i : structType.GetItems()) {
            if (i->GetName().StartsWith("_yql_sys_")) {
                continue;
            }

            writer.OnKeyedItem(i->GetName());
            writer.OnStringScalar(FormatType(i->GetItemType()));
        }

        writer.OnEndMap();
    }

    using TFieldLineage = std::pair<ui32, TString>;
        
    struct TFieldsLineage {
        THashSet<TFieldLineage> Items;
    };

    struct TLineage {
        // null - can't calculcate
        TMaybe<THashMap<TString, TFieldsLineage>> Fields;
    };

    const TLineage* CollectLineage(const TExprNode& node) {
        if (auto it = Lineages_.find(&node); it != Lineages_.end()) {
            return &it->second;
        }

        auto& lineage = Lineages_[&node];
        if (auto readIt = ReadIds_.find(&node); readIt != ReadIds_.end()) {
            lineage.Fields.ConstructInPlace();
            auto type = node.GetTypeAnn()->Cast<TTupleExprType>()->GetItems()[1]->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
            for (const auto& i : type->GetItems()) {
                if (i->GetName().StartsWith("_yql_sys_")) {
                    continue;
                }

                TString fieldName(i->GetName());
                auto& v = (*lineage.Fields)[fieldName];
                for (const auto& r : readIt->second) {
                    v.Items.insert({ r, fieldName });
                }
            }

            return &lineage;
        }

        if (node.IsCallable({"Unordered", "Right!", "Skip", "Take", "Sort", "AssumeSorted"})) {
            lineage = *CollectLineage(node.Head());
            return &lineage;
        } else if (node.IsCallable("ExtractMembers")) {
            HandleExtractMembers(lineage, node);
        } else if (node.IsCallable({"FlatMap", "OrderedFlatMap"})) {
            HandleFlatMap(lineage, node);
        } else if (node.IsCallable("Aggregate")) {
            HandleAggregate(lineage, node);
        } else if (node.IsCallable("Extend")) {
            HandleExtend(lineage, node);
        } else if (node.IsCallable({"CalcOverWindow","CalcOverSessionWindow","CalcOverWindowGroup"})) {
            HandleWindow(lineage, node);
        } else if (node.IsCallable("EquiJoin")) {
            HandleEquiJoin(lineage, node);
        }

        return &lineage;
    }

    void HandleExtractMembers(TLineage& lineage, const TExprNode& node) {
        auto innerLineage = *CollectLineage(node.Head());
        if (innerLineage.Fields.Defined()) {
            lineage.Fields.ConstructInPlace();
            for (const auto& atom : node.Child(1)->Children()) {
                TString fieldName(atom->Content());
                (*lineage.Fields)[fieldName] = (*innerLineage.Fields)[fieldName];
            }
        }
    }

    // returns false if all fields are used
    bool GetUsedFields(const TExprNode& expr, const TExprNode& arg, THashSet<TString>& fields) {
        fields.clear();
        if (!IsDepended(expr, arg)) {
            return true;
        }

        TParentsMap parentsMap;
        GatherParents(expr, parentsMap);

        auto parentsIt = parentsMap.find(&arg);
        if (parentsIt == parentsMap.end()) {
            return false;
        } else {
            for (const auto& p : parentsIt->second) {
                if (p->IsCallable("Member")) {
                    fields.insert(TString(p->Tail().Content()));
                } else {
                    return false;
                }
            }
        }

        return true;
    }

    void MergeLineageFromUsedFields(const TExprNode& expr, const TExprNode& arg, const TLineage& src, 
        TFieldsLineage& dst) {
        THashSet<TString> usedFields;
        auto needAllFields = !GetUsedFields(expr, arg, usedFields);
        if (needAllFields) {
            for (const auto& f : *src.Fields) {
                for (const auto& i: f.second.Items) {
                    dst.Items.insert(i);
                }
            }
        } else {
            for (const auto& f : usedFields) {
                for (const auto& i: (*src.Fields).FindPtr(f)->Items) {
                    dst.Items.insert(i);
                }
            }
        }
    }

    void HandleFlatMap(TLineage& lineage, const TExprNode& node) {
        auto innerLineage = *CollectLineage(node.Head());
        if (!innerLineage.Fields.Defined()) {
            return;
        }

        const auto& lambda = node.Tail();
        const auto& arg = lambda.Head().Head();
        const auto& body = lambda.Tail();
        const TExprNode* value;
        if (body.IsCallable("OptionalIf")) {
            value = &body.Tail();
        } else if (body.IsCallable("Just")) {
            value = &body.Head();
        } else {
            return;
        }

        if (value == &arg) {
            lineage.Fields = *innerLineage.Fields;
            return;
        }

        if (!value->IsCallable("AsStruct")) {
            return;
        }

        lineage.Fields.ConstructInPlace();
        for (const auto& child : value->Children()) {
            TString field(child->Head().Content());
            auto& res = (*lineage.Fields)[field];
            const auto& expr = child->Tail();
            MergeLineageFromUsedFields(expr, arg, innerLineage, res);
        }
    }

    void HandleAggregate(TLineage& lineage, const TExprNode& node) {
        auto innerLineage = *CollectLineage(node.Head());
        if (!innerLineage.Fields.Defined()) {
            return;
        }

        lineage.Fields.ConstructInPlace();
        for (const auto& key : node.Child(1)->Children()) {
            TString field(key->Content());
            (*lineage.Fields)[field] = (*innerLineage.Fields)[field];
        }

        for (const auto& payload: node.Child(2)->Children()) {
            TVector<TString> fields;
            if (payload->Child(0)->IsList()) {
                for (const auto& child : payload->Child(0)->Children()) {
                    fields.push_back(TString(child->Content()));
                }
            } else { 
                fields.push_back(TString(payload->Child(0)->Content()));
            }

            TFieldsLineage source;
            if (payload->ChildrenSize() == 3) {
                // distinct
                source = (*innerLineage.Fields)[payload->Child(2)->Content()];
            } else {
                if (payload->Child(1)->IsCallable("AggregationTraits")) {
                    // merge all used fields from init/update handlers
                    auto initHandler = payload->Child(1)->Child(1);
                    auto updateHandler = payload->Child(1)->Child(2);
                    MergeLineageFromUsedFields(initHandler->Tail(), initHandler->Head().Head(), innerLineage, source);
                    MergeLineageFromUsedFields(updateHandler->Tail(), updateHandler->Head().Head(), innerLineage, source);
                } else if (payload->Child(1)->IsCallable("AggApply")) {
                    auto extractHandler = payload->Child(1)->Child(2);
                    MergeLineageFromUsedFields(extractHandler->Tail(), extractHandler->Head().Head(), innerLineage, source);
                } else {
                    lineage.Fields.Clear();
                    return;
                }
            }

            for (const auto& field : fields) {
                (*lineage.Fields)[field] = source;
            }
        }
    }

    void HandleExtend(TLineage& lineage, const TExprNode& node) {
        TVector<TLineage> inners;
        for (const auto& child : node.Children()) {
            inners.push_back(*CollectLineage(*child));
            if (!inners.back().Fields.Defined()) {
                return;
            }
        }

        if (inners.empty()) {
            return;
        }

        lineage.Fields.ConstructInPlace();
        for (const auto& x : *inners.front().Fields) {
            auto& res = (*lineage.Fields)[x.first];
            for (const auto& i : inners) {
                for (const auto& x : (*i.Fields).FindPtr(x.first)->Items) {
                    res.Items.insert(x);
                }
            }
        }
    }

    void HandleWindow(TLineage& lineage, const TExprNode& node) {
        auto innerLineage = *CollectLineage(node.Head());
        if (!innerLineage.Fields.Defined()) {
            return;
        }

        lineage.Fields = *innerLineage.Fields;
        TExprNode::TListType frameGroups;
        if (node.IsCallable("CalcOverWindowGroup")) {
            for (const auto& g : node.Child(1)->Children()) {
                frameGroups.push_back(g->Child(2));
            }
        } else {
            frameGroups.push_back(node.Child(3));
        }

        for (const auto& g : frameGroups) {
            for (const auto& f : g->Children()) {
                if (!f->IsCallable("WinOnRows")) {
                    lineage.Fields.Clear();
                    return;
                }

                for (ui32 i = 1; i < f->ChildrenSize(); ++i) {
                    const auto& list = f->Child(i);
                    auto field = list->Head().Content();
                    auto& res = (*lineage.Fields)[field];
                    if (list->Tail().IsCallable("RowNumber")) {
                        continue;
                    } else if (list->Tail().IsCallable({"Lag","Lead","Rank","DenseRank"})) {
                        const auto& lambda = list->Tail().Child(1);
                        MergeLineageFromUsedFields(lambda->Tail(), lambda->Head().Head(), innerLineage, res);
                    } else if (list->Tail().IsCallable("WindowTraits")) {
                        const auto& initHandler = list->Tail().Child(1);
                        const auto& updateHandler = list->Tail().Child(2);
                        MergeLineageFromUsedFields(initHandler->Tail(), initHandler->Head().Head(), innerLineage, res);
                        MergeLineageFromUsedFields(updateHandler->Tail(), updateHandler->Head().Head(), innerLineage, res);
                    } else {
                        lineage.Fields.Clear();
                        return;
                    }
                }
            }
        }
    }

    void HandleEquiJoin(TLineage& lineage, const TExprNode& node) {
        TVector<TLineage> inners;
        THashMap<TStringBuf, ui32> inputLabels;
        for (ui32 i = 0; i < node.ChildrenSize() - 2; ++i) {
            inners.push_back(*CollectLineage(node.Child(i)->Head()));
            if (!inners.back().Fields.Defined()) {
                return;
            }

            if (node.Child(i)->Tail().IsAtom()) {
                inputLabels[node.Child(i)->Tail().Content()] = i;
            } else {
                for (const auto& label : node.Child(i)->Tail().Children()) {
                    inputLabels[label->Content()] = i;
                }
            }
        }

        THashMap<TStringBuf, TStringBuf> backRename;
        for (auto setting : node.Tail().Children()) {
            if (setting->Head().Content() != "rename") {
                continue;
            }

            if (setting->Child(2)->Content().Empty()) {
                continue;
            }

            backRename[setting->Child(2)->Content()] = setting->Child(1)->Content();
        }

        lineage.Fields.ConstructInPlace();
        auto structType = node.GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
        for (const auto& field : structType->GetItems()) {
            TStringBuf originalName = field->GetName();
            if (auto it = backRename.find(originalName); it != backRename.end()) {
                originalName = it->second;
            }

            TStringBuf table, column;
            SplitTableName(originalName, table, column);
            ui32 index = *inputLabels.FindPtr(table);
            auto& res = (*lineage.Fields)[field->GetName()];
            for (const auto& i: (*inners[index].Fields).FindPtr(column)->Items) {
                res.Items.insert(i);
            }
        }
    }

    void WriteLineage(NYson::TYsonWriter& writer, const TLineage& lineage) {
        if (!lineage.Fields.Defined()) {
            YQL_ENSURE(!GetEnv("YQL_DETERMINISTIC_MODE"), "Can't calculate lineage");
            writer.OnEntity();
            return;
        }

        writer.OnBeginMap();
        TVector<TString> fields;
        for (const auto& f : *lineage.Fields) {
            fields.push_back(f.first);
        }

        Sort(fields);
        for (const auto& f : fields) {
            writer.OnKeyedItem(f);
            writer.OnBeginList();
            TVector<std::pair<ui32, TString>> items;
            for (const auto& i : lineage.Fields->FindPtr(f)->Items) {
                items.push_back(i);
            }

            Sort(items);
            for (const auto& i: items) {
                writer.OnListItem();
                writer.OnBeginMap();
                writer.OnKeyedItem("Input");
                writer.OnInt64Scalar(i.first);
                writer.OnKeyedItem("Field");
                writer.OnStringScalar(i.second);
                writer.OnEndMap();
            }
            writer.OnEndList();
        }

        writer.OnEndMap();
    }

private:
    const TExprNode& Root_;
    const TTypeAnnotationContext& Ctx_;
    TNodeMap<IDataProvider*> Reads_, Writes_;
    ui32 NextReadId_ = 0;
    ui32 NextWriteId_ = 0;
    TNodeMap<TVector<ui32>> ReadIds_;
    TNodeMap<ui32> WriteIds_;
    TNodeMap<TLineage> Lineages_;
};

}

TString CalculateLineage(const TExprNode& root, const TTypeAnnotationContext& ctx) {
    TLineageScanner scanner(root, ctx);
    return scanner.Process();
}

}
