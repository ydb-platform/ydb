
#include "visualizer.h"

#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/type_ann/type_ann_core.h>
#include <yql/essentials/utils/log/log.h>

#include <library/cpp/string_utils/base64/base64.h>
#include <util/stream/file.h>
#include <util/folder/path.h>
#include <util/system/env.h>
#include <util/string/builder.h>
#include <util/string/printf.h>

namespace NKikimr::NKqp::NOpt {

namespace {
    // Глобальные настройки визуализации
    bool AstVisualizationEnabled = false;
    TString AstOutputDirectory = "/tmp/ast_graphs";
    ui32 DumpCounter = 0;
}

TAstTypeVisualizer::TAstTypeVisualizer()
    : Options_()
{
}

TAstTypeVisualizer::TAstTypeVisualizer(const TVisualizationOptions& options)
    : Options_(options)
{
}

TString TAstTypeVisualizer::VisualizeAst(const NYql::TExprNode::TPtr& root, const TString& graphName) {
    VisitedNodes_.clear();
    NodeIds_.clear();
    NextNodeId_ = 0;

    THashMap<TString, TNodeInfo> nodes;
    CollectNodeInfo(root, nodes);
    
    return GenerateDotGraph(nodes, graphName);
}

TString TAstTypeVisualizer::VisualizeAst(const NYql::NNodes::TExprBase& root, const TString& graphName) {
    return VisualizeAst(root.Ptr(), graphName);
}

TString TAstTypeVisualizer::VisualizeSubtree(const NYql::TExprNode::TPtr& node, const TString& graphName) {
    return VisualizeAst(node, graphName);
}

TString TAstTypeVisualizer::VisualizeSubtree(const NYql::NNodes::TExprBase& node, const TString& graphName) {
    return VisualizeAst(node.Ptr(), graphName);
}

void TAstTypeVisualizer::DumpAstToFile(const NYql::TExprNode::TPtr& root, const TString& filename) {
    TVisualizationOptions options;
    DumpAstToFile(root, filename, options);
}

void TAstTypeVisualizer::DumpAstToFile(const NYql::NNodes::TExprBase& root, const TString& filename) {
    DumpAstToFile(root.Ptr(), filename);
}

void TAstTypeVisualizer::DumpAstToFile(const NYql::TExprNode::TPtr& root, const TString& filename, 
                                       const TVisualizationOptions& options) {
    TAstTypeVisualizer visualizer(options);
    TString dotContent = visualizer.VisualizeAst(root, "AST_" + filename);
    
    TString fullPath = TString::Join(AstOutputDirectory, "/", filename, ".dot");
    
    // Создаем директорию если её нет
    TFsPath(AstOutputDirectory).MkDirs();
    
    try {
        TFileOutput file(fullPath);
        file.Write(dotContent);
        file.Finish();
        
        YQL_LOG(INFO) << "AST visualization saved to: " << fullPath;
    } catch (const std::exception& e) {
        YQL_LOG(ERROR) << "Failed to save AST visualization to " << fullPath << ": " << e.what();
    }
}

void TAstTypeVisualizer::DumpAstToFile(const NYql::NNodes::TExprBase& root, const TString& filename, 
                                       const TVisualizationOptions& options) {
    DumpAstToFile(root.Ptr(), filename, options);
}

void TAstTypeVisualizer::DumpAstIfEnabled(const NYql::TExprNode::TPtr& root, const TString& stageName) {
    TVisualizationOptions options;
    DumpAstIfEnabled(root, stageName, options);
}

void TAstTypeVisualizer::DumpAstIfEnabled(const NYql::NNodes::TExprBase& root, const TString& stageName) {
    DumpAstIfEnabled(root.Ptr(), stageName);
}

void TAstTypeVisualizer::DumpAstIfEnabled(const NYql::TExprNode::TPtr& root, const TString& stageName,
                                          const TVisualizationOptions& options) {
    if (!IsAstVisualizationEnabled()) {
        return;
    }
    
    TString filename = TStringBuilder() << Sprintf("%04d", ++DumpCounter) << "_" << stageName;
    DumpAstToFile(root, filename, options);
}

void TAstTypeVisualizer::DumpAstIfEnabled(const NYql::NNodes::TExprBase& root, const TString& stageName,
                                          const TVisualizationOptions& options) {
    DumpAstIfEnabled(root.Ptr(), stageName, options);
}

void TAstTypeVisualizer::CollectNodeInfo(const NYql::TExprNode::TPtr& node, THashMap<TString, TNodeInfo>& nodes) {
    if (!node || VisitedNodes_.contains(node.Get())) {
        return;
    }
    
    VisitedNodes_.insert(node.Get());
    
    TNodeInfo info;
    info.NodeId = GetOrCreateNodeId(node.Get());
    
    // Более детальное имя узла
    TString nodeContent = TString(node->Content());
    if (node->IsCallable()) {
        info.NodeType = nodeContent.empty() ? "Callable" : nodeContent;
    } else if (node->IsAtom()) {
        if (nodeContent.empty()) {
            info.NodeType = "EmptyAtom";
        } else {
            TString shortContent = nodeContent.size() > 25 ? nodeContent.substr(0, 25) + "..." : nodeContent;
            info.NodeType = TStringBuilder() << "\"" << shortContent << "\"";
        }
    } else if (node->IsArgument()) {
        info.NodeType = nodeContent.empty() ? "Argument" : nodeContent;
    } else if (node->IsList()) {
        info.NodeType = TStringBuilder() << "List[" << node->ChildrenSize() << "]";
    } else {
        info.NodeType = nodeContent.empty() ? "Unknown" : nodeContent;
    }
    
    info.OutputType = FormatType(node->GetTypeAnn());
    info.InputTypes = FormatInputTypes(*node);
    info.Details = GetNodeDetails(*node);
    info.IsFiltered = ShouldFilterNode(*node);
    info.HasError = (node->GetTypeAnn() && node->GetTypeAnn()->GetKind() == NYql::ETypeAnnotationKind::Error);
    
    // Собираем детей
    for (const auto& child : node->Children()) {
        if (child) {
            info.Children.push_back(GetOrCreateNodeId(child.Get()));
            CollectNodeInfo(child, nodes);
        }
    }
    
    nodes[info.NodeId] = std::move(info);
}

TString TAstTypeVisualizer::GetOrCreateNodeId(const NYql::TExprNode* node) {
    auto it = NodeIds_.find(node);
    if (it != NodeIds_.end()) {
        return it->second;
    }
    
    TString nodeId = TStringBuilder() << "n" << NextNodeId_++;
    NodeIds_[node] = nodeId;
    return nodeId;
}

TString TAstTypeVisualizer::FormatType(const NYql::TTypeAnnotationNode* type) const {
    if (!type) {
        return "Unknown";
    }
    
    TString typeStr;
    try {
        typeStr = NYql::FormatType(type);
    } catch (...) {
        // Fallback если FormatType не работает
        switch (type->GetKind()) {
            case NYql::ETypeAnnotationKind::Unit:
                typeStr = "Unit";
                break;
            case NYql::ETypeAnnotationKind::Data:
                typeStr = "Data";
                break;
            case NYql::ETypeAnnotationKind::List:
                typeStr = "List<...>";
                break;
            case NYql::ETypeAnnotationKind::Stream:
                typeStr = "Stream<...>";
                break;
            case NYql::ETypeAnnotationKind::Flow:
                typeStr = "Flow<...>";
                break;
            case NYql::ETypeAnnotationKind::Struct:
                typeStr = "Struct<...>";
                break;
            case NYql::ETypeAnnotationKind::Tuple:
                typeStr = "Tuple<...>";
                break;
            case NYql::ETypeAnnotationKind::Optional:
                typeStr = "Optional<...>";
                break;
            case NYql::ETypeAnnotationKind::Error:
                typeStr = "ERROR";
                break;
            default:
                typeStr = TStringBuilder() << "Kind_" << (ui32)type->GetKind();
        }
    }
    
    // Очищаем и упрощаем тип
    SubstGlobal(typeStr, "NYql::", "");
    SubstGlobal(typeStr, "NKikimr::", "");
    
    if (!Options_.ShowFullTypes) {
        // Упрощаем сложные типы
        if (typeStr.size() > 45) {
            // Более умное сокращение для структур
            if (typeStr.StartsWith("Struct<")) {
                typeStr = "Struct<...>";
            } else if (typeStr.StartsWith("List<")) {
                typeStr = "List<...>";
            } else if (typeStr.StartsWith("Stream<")) {
                typeStr = "Stream<...>";
            } else if (typeStr.StartsWith("Flow<")) {
                typeStr = "Flow<...>";
            } else {
                typeStr = typeStr.substr(0, 42) + "...";
            }
        }
    }
    
    if (!Options_.ShowBuiltinTypes && typeStr != "Unit" && typeStr != "Unknown") {
        // Скрываем простые встроенные типы
        if (typeStr == "String" || typeStr == "Int32" || typeStr == "Uint64" || 
            typeStr == "Bool" || typeStr == "Double") {
            return "Scalar";
        }
    }
    
    return typeStr;
}

TString TAstTypeVisualizer::FormatInputTypes(const NYql::TExprNode& node) const {
    TStringBuilder result;
    
    for (size_t i = 0; i < node.ChildrenSize(); ++i) {
        if (i > 0) result << ", ";
        
        const auto& child = node.Child(i);
        if (child && child->GetTypeAnn()) {
            result << FormatType(child->GetTypeAnn());
        } else {
            result << "?";
        }
    }
    
    return result;
}

TString TAstTypeVisualizer::GetTypeCategory(const NYql::TTypeAnnotationNode* type) const {
    if (!type) return "unknown";
    
    if (IsBlockType(type)) return "block";
    if (IsStreamType(type)) return "stream";
    
    switch (type->GetKind()) {
        case NYql::ETypeAnnotationKind::Struct:
        case NYql::ETypeAnnotationKind::Tuple:
            return "composite";
        case NYql::ETypeAnnotationKind::List:
            return "list";
        case NYql::ETypeAnnotationKind::Stream:
            return "stream";
        case NYql::ETypeAnnotationKind::Error:
            return "error";
        default:
            return "scalar";
    }
}

TString TAstTypeVisualizer::GetNodeDetails(const NYql::TExprNode& node) const {
    if (!Options_.ShowNodeDetails) {
        return "";
    }
    
    TStringBuilder details;
    
    // Количество детей
    if (node.ChildrenSize() > 0) {
        details << "children: " << node.ChildrenSize();
    }
    
    // Тип узла (callable, atom, etc.)
    if (node.IsCallable()) {
        if (details.size() > 0) details << ", ";
        details << "callable";
    } else if (node.IsAtom()) {
        if (details.size() > 0) details << ", ";
        details << "atom: " << TString(node.Content()).substr(0, 20);
        if (node.Content().size() > 20) {
            details << "...";
        }
    } else if (node.IsArgument()) {
        if (details.size() > 0) details << ", ";
        details << "arg: " << TString(node.Content());
    } else if (node.IsList()) {
        if (details.size() > 0) details << ", ";
        details << "list";
    }
    
    // Специальная обработка для некоторых узлов
    if (node.Content() == "Member" && node.ChildrenSize() >= 2) {
        if (auto atom = node.Child(1); atom && atom->IsAtom()) {
            if (details.size() > 0) details << ", ";
            details << "field: " << atom->Content();
        }
    }
    
    if (node.Content() == "Take" && node.ChildrenSize() >= 2) {
        if (auto countNode = node.Child(1); countNode && countNode->IsAtom()) {
            if (details.size() > 0) details << ", ";
            details << "count: " << countNode->Content();
        }
    }
    
    // Определяем тип операции с помощью новых методов
    if (IsBlockOperation(node)) {
        if (details.size() > 0) details << ", ";
        details << "[BLOCK]";
    } else if (IsWideOperation(node)) {
        if (details.size() > 0) details << ", ";
        details << "[WIDE]";
    } else if (IsConversionOperation(node)) {
        if (details.size() > 0) details << ", ";
        details << "[CONVERSION]";
    }
    
    // Дополнительные метки для конкретных типов операций
    TString content = TString(node.Content());
    if (content.Contains("Map") && !IsConversionOperation(node)) {
        if (details.size() > 0) details << ", ";
        details << "[MAP]";
    }
    
    if (content.Contains("Join") || content.Contains("HashJoin")) {
        if (details.size() > 0) details << ", ";
        details << "[JOIN]";
    }
    
    if (content.Contains("Stage") || content.Contains("Dq")) {
        if (details.size() > 0) details << ", ";
        details << "[STAGE]";
    }
    
    if (content.Contains("Filter")) {
        if (details.size() > 0) details << ", ";
        details << "[FILTER]";
    }
    
    if (content.Contains("Agg") || content.Contains("Group")) {
        if (details.size() > 0) details << ", ";
        details << "[AGG]";
    }
    
    return details;
}

bool TAstTypeVisualizer::ShouldFilterNode(const NYql::TExprNode& node) const {
    if (Options_.FilterNodeType.empty()) {
        return false;
    }
    
    TString content = TString(node.Content());
    return content.Contains(Options_.FilterNodeType);
}

bool TAstTypeVisualizer::IsBlockType(const NYql::TTypeAnnotationNode* type) const {
    if (!type) return false;
    
    // Простая эвристика для определения блочных типов
    TString typeStr = NYql::FormatType(type);
    return typeStr.Contains("Block") || typeStr.Contains("Wide");
}

bool TAstTypeVisualizer::IsStreamType(const NYql::TTypeAnnotationNode* type) const {
    if (!type) return false;
    
    return type->GetKind() == NYql::ETypeAnnotationKind::Stream ||
           type->GetKind() == NYql::ETypeAnnotationKind::Flow;
}

bool TAstTypeVisualizer::IsBlockOperation(const NYql::TExprNode& node) const {
    if (!node.IsCallable()) {
        return false;
    }
    
    TString callableName = TString(node.Content());
    
    // Проверяем по точным именам блочных операций
    static const THashSet<TString> blockOperations = {
        "DqPhyBlockHashJoin",
        "BlockHashJoin", 
        "BlockJoin",
        "BlockExist",
        "BlockFunc",
        "BlockCoalesce",
        "BlockIf",
        "BlockAgg",
        "BlockTop",
        "BlockSort",
        "BlockMergeFinalizeHashes",
        "BlockMergeManyFinalizeHashes",
        "BlockCombineAll",
        "BlockCombineHashes"
    };
    
    if (blockOperations.contains(callableName)) {
        return true;
    }
    
    // Проверяем по префиксам
    if (callableName.StartsWith("Block") || callableName.StartsWith("DqPhyBlock")) {
        return true;
    }
    
    return false;
}

bool TAstTypeVisualizer::IsWideOperation(const NYql::TExprNode& node) const {
    if (!node.IsCallable()) {
        return false;
    }
    
    TString callableName = TString(node.Content());
    
    // Wide операции
    static const THashSet<TString> wideOperations = {
        "WideMap",
        "WideTakeBlocks", 
        "WideSkipBlocks",
        "WideCombiner",
        "WideCondense1",
        "WideFilter",
        "WideTop",
        "WideSort",
        "ExpandMap"
    };
    
    if (wideOperations.contains(callableName)) {
        return true;
    }
    
    if (callableName.StartsWith("Wide")) {
        return true;
    }
    
    return false;
}

bool TAstTypeVisualizer::IsConversionOperation(const NYql::TExprNode& node) const {
    if (!node.IsCallable()) {
        return false;
    }
    
    TString callableName = TString(node.Content());
    
    // Операции конвертации между форматами
    static const THashSet<TString> conversionOperations = {
        "WideToBlocks",
        "WideFromBlocks", 
        "ToBlocks",
        "FromBlocks",
        "NarrowMap",
        "ExpandMap",
        "ToFlow",
        "FromFlow",
        "ToStream",
        "Extend",
        "AsStruct"
    };
    
    return conversionOperations.contains(callableName);
}

TString TAstTypeVisualizer::GenerateDotGraph(const THashMap<TString, TNodeInfo>& nodes, const TString& graphName) const {
    TStringBuilder dot;
    
    dot << "digraph " << EscapeDotString(graphName) << " {\n";
    dot << "  rankdir=TB;\n";
    dot << "  node [shape=rectangle, style=filled];\n";
    dot << "  edge [fontsize=8];\n\n";
    
    // Объявление узлов
    for (const auto& [nodeId, info] : nodes) {
        dot << "  " << nodeId << " [";
        dot << "label=\"" << FormatNodeLabel(info) << "\", ";
        dot << GetNodeStyle(info);
        dot << "];\n";
    }
    
    dot << "\n";
    
    // Рёбра
    for (const auto& [nodeId, info] : nodes) {
        for (const auto& childId : info.Children) {
            dot << "  " << nodeId << " -> " << childId;
            
            // Добавляем информацию о типах на рёбра
            auto childIt = nodes.find(childId);
            if (childIt != nodes.end() && !childIt->second.OutputType.empty()) {
                TString edgeLabel = childIt->second.OutputType;
                // Упрощаем длинные типы для рёбер
                if (edgeLabel.size() > 18) {
                    if (edgeLabel.StartsWith("Struct<")) {
                        edgeLabel = "Struct";
                    } else if (edgeLabel.StartsWith("List<")) {
                        edgeLabel = "List";
                    } else if (edgeLabel.StartsWith("Stream<")) {
                        edgeLabel = "Stream";
                    } else if (edgeLabel.StartsWith("Flow<")) {
                        edgeLabel = "Flow";
                    } else {
                        edgeLabel = edgeLabel.substr(0, 15);
                    }
                }
                dot << " [label=\"" << EscapeDotString(edgeLabel) << "\"]";
            }
            
            dot << ";\n";
        }
    }
    
    // Легенда
    dot << "\n  // Legend\n";
    dot << "  subgraph cluster_legend {\n";
    dot << "    label=\"Operation Types\";\n";
    dot << "    style=dashed;\n";
    dot << "    legend_block [label=\"Block Operations\", fillcolor=\"" << Options_.BlockTypeColor << "\"];\n";
    dot << "    legend_wide [label=\"Wide Operations\", fillcolor=\"lightcyan\"];\n";
    dot << "    legend_conversion [label=\"Conversion Ops\", fillcolor=\"lightyellow\"];\n";
    dot << "    legend_stream [label=\"Stream/Flow Ops\", fillcolor=\"" << Options_.StreamTypeColor << "\"];\n";
    dot << "    legend_scalar [label=\"Other Operations\", fillcolor=\"" << Options_.ScalarTypeColor << "\"];\n";
    if (!Options_.FilterNodeType.empty()) {
        dot << "    legend_filtered [label=\"Filtered: " << Options_.FilterNodeType << "\", fillcolor=\"" << Options_.FilteredNodeColor << "\"];\n";
    }
    dot << "  }\n";
    
    dot << "}\n";
    
    return dot;
}

TString TAstTypeVisualizer::FormatNodeLabel(const TNodeInfo& info) const {
    TStringBuilder label;
    
    if (Options_.CompactMode) {
        label << info.NodeType;
        if (!info.OutputType.empty() && info.OutputType != "Unknown" && info.OutputType != "Unit") {
            label << "\n" << info.OutputType;
        }
    } else {
        label << info.NodeType;
        
        // Показываем выходной тип только если он не Unit и не Unknown
        if (!info.OutputType.empty() && info.OutputType != "Unknown" && info.OutputType != "Unit") {
            label << "\nOut: " << info.OutputType;
        }
        
        // Показываем входные типы только если они не все неизвестны
        if (!info.InputTypes.empty() && info.InputTypes != "?" && info.InputTypes.find("Unknown") == TString::npos) {
            TString shortInputs = info.InputTypes;
            if (shortInputs.size() > 50) {
                shortInputs = shortInputs.substr(0, 47) + "...";
            }
            label << "\nIn: " << shortInputs;
        }
        
        // Всегда показываем детали, если они есть
        if (!info.Details.empty()) {
            label << "\n[" << info.Details << "]";
        }
    }
    
    return EscapeDotString(label);
}

TString TAstTypeVisualizer::GetNodeStyle(const TNodeInfo& info) const {
    TString fillColor = Options_.ScalarTypeColor;  // по умолчанию
    
    if (info.HasError) {
        fillColor = Options_.ErrorTypeColor;
    } else if (info.IsFiltered) {
        fillColor = Options_.FilteredNodeColor;
    } else {
        // Определяем цвет по типу операции (не по типу данных!)
        bool isBlockOp = info.Details.Contains("[BLOCK]");
        bool isWideOp = info.Details.Contains("[WIDE]");
        bool isConversionOp = info.Details.Contains("[CONVERSION]");
        bool isStreamOp = info.OutputType.Contains("Stream") || 
                         info.OutputType.Contains("Flow") || 
                         info.OutputType.Contains("List");
        
        if (isBlockOp) {
            fillColor = Options_.BlockTypeColor;  // голубой для блочных операций
        } else if (isWideOp) {
            fillColor = "lightcyan";  // светло-голубой для wide операций
        } else if (isConversionOp) {
            fillColor = "lightyellow";  // жёлтый для конвертации
        } else if (isStreamOp) {
            fillColor = Options_.StreamTypeColor;  // зелёный для потоков
        }
    }
    
    TStringBuilder style;
    style << "fillcolor=\"" << fillColor << "\"";
    
    if (info.IsFiltered) {
        style << ", penwidth=3, color=red";
    }
    
    return style;
}

TString TAstTypeVisualizer::EscapeDotString(const TString& str) const {
    TString result = str;
    
    // ВАЖНО: НЕ экранируем \n - в DOT label'ах \n используется для переноса строки!
    
    // Экранируем только кавычки и удаляем проблемные символы
    SubstGlobal(result, "\"", "\\\"");  // Двойные кавычки
    SubstGlobal(result, "\r", "");      // Удаляем \r
    SubstGlobal(result, "\t", " ");     // Табы в пробелы
    
    // Заменяем проблемные символы на безопасные
    SubstGlobal(result, "<", "{");
    SubstGlobal(result, ">", "}");
    
    return result;
}

// Глобальные функции
void EnableAstVisualization(const TString& outputDir) {
    AstVisualizationEnabled = true;
    AstOutputDirectory = outputDir;
    DumpCounter = 0;
    
    // Создаем директорию
    TFsPath(AstOutputDirectory).MkDirs();
    
    YQL_LOG(INFO) << "AST visualization enabled, output directory: " << AstOutputDirectory;
}

void DisableAstVisualization() {
    AstVisualizationEnabled = false;
    YQL_LOG(INFO) << "AST visualization disabled";
}

bool IsAstVisualizationEnabled() {
    return true;
}

} // namespace NKikimr::NKqp::NOpt
