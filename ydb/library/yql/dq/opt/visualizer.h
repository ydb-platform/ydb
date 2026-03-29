#pragma once

#include <ydb/core/kqp/common/kqp_yql.h>
#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_type_annotation.h>

#include <util/stream/output.h>
#include <util/generic/hash_set.h>
#include <util/generic/string.h>

namespace NKikimr::NKqp::NOpt {

/**
 * AST Type Visualizer - утилита для создания DOT графов AST с информацией о типах
 * 
 * Основные возможности:
 * - Обход AST и извлечение информации о типах узлов
 * - Генерация DOT графа с типами входов и выходов узлов
 * - Выделение узлов с проблемными или интересными типами
 * - Встраивание в существующие трансформеры как debug-функция
 */

class TAstTypeVisualizer {
public:
    struct TVisualizationOptions {
        bool ShowFullTypes = false;          // Показывать полные типы или сокращенные
        bool ShowBuiltinTypes = true;        // Показывать встроенные типы (String, Int32, etc.)
        bool HighlightBlockTypes = true;     // Выделять блочные типы
        bool ShowNodeDetails = true;         // Показывать детали узлов (arities, etc.)
        bool CompactMode = false;            // Компактный режим вывода
        TString FilterNodeType = "";        // Фильтр по типу узла (например, "DqJoin", "BlockHashJoin")
        
        // Настройки стилей DOT
        TString BlockTypeColor = "lightblue";
        TString StreamTypeColor = "lightgreen"; 
        TString ScalarTypeColor = "white";
        TString ErrorTypeColor = "red";
        TString FilteredNodeColor = "yellow";
    };

    TAstTypeVisualizer();
    explicit TAstTypeVisualizer(const TVisualizationOptions& options);

    // Главная функция - создание DOT графа для AST
    TString VisualizeAst(const NYql::TExprNode::TPtr& root, const TString& graphName = "AST");
    TString VisualizeAst(const NYql::NNodes::TExprBase& root, const TString& graphName = "AST");
    
    // Создание DOT графа только для определенного поддерева
    TString VisualizeSubtree(const NYql::TExprNode::TPtr& node, const TString& graphName = "Subtree");
    TString VisualizeSubtree(const NYql::NNodes::TExprBase& node, const TString& graphName = "Subtree");

    // Утилитарные функции для встраивания в трансформеры
    static void DumpAstToFile(const NYql::TExprNode::TPtr& root, const TString& filename);
    static void DumpAstToFile(const NYql::NNodes::TExprBase& root, const TString& filename);
    static void DumpAstToFile(const NYql::TExprNode::TPtr& root, const TString& filename, 
                              const TVisualizationOptions& options);
    static void DumpAstToFile(const NYql::NNodes::TExprBase& root, const TString& filename, 
                              const TVisualizationOptions& options);
    
    static void DumpAstIfEnabled(const NYql::TExprNode::TPtr& root, const TString& stageName);
    static void DumpAstIfEnabled(const NYql::NNodes::TExprBase& root, const TString& stageName);
    static void DumpAstIfEnabled(const NYql::TExprNode::TPtr& root, const TString& stageName,
                                 const TVisualizationOptions& options);
    static void DumpAstIfEnabled(const NYql::NNodes::TExprBase& root, const TString& stageName,
                                 const TVisualizationOptions& options);

private:
    struct TNodeInfo {
        TString NodeId;
        TString NodeType;
        TString InputTypes;
        TString OutputType;
        TString Details;
        TVector<TString> Children;
        bool IsFiltered = false;
        bool HasError = false;
    };

    TVisualizationOptions Options_;
    THashSet<const NYql::TExprNode*> VisitedNodes_;
    THashMap<const NYql::TExprNode*, TString> NodeIds_;
    ui32 NextNodeId_ = 0;

    // Основные методы обхода и анализа
    void CollectNodeInfo(const NYql::TExprNode::TPtr& node, THashMap<TString, TNodeInfo>& nodes);
    TString GetOrCreateNodeId(const NYql::TExprNode* node);
    
    // Анализ типов
    TString FormatType(const NYql::TTypeAnnotationNode* type) const;
    TString FormatInputTypes(const NYql::TExprNode& node) const;
    TString GetTypeCategory(const NYql::TTypeAnnotationNode* type) const;
    
    // Анализ узлов
    TString GetNodeDetails(const NYql::TExprNode& node) const;
    bool ShouldFilterNode(const NYql::TExprNode& node) const;
    bool IsBlockType(const NYql::TTypeAnnotationNode* type) const;
    bool IsStreamType(const NYql::TTypeAnnotationNode* type) const;
    bool IsBlockOperation(const NYql::TExprNode& node) const;
    bool IsWideOperation(const NYql::TExprNode& node) const;
    bool IsConversionOperation(const NYql::TExprNode& node) const;
    
    // Генерация DOT
    TString GenerateDotGraph(const THashMap<TString, TNodeInfo>& nodes, const TString& graphName) const;
    TString FormatNodeLabel(const TNodeInfo& info) const;
    TString GetNodeStyle(const TNodeInfo& info) const;
    TString EscapeDotString(const TString& str) const;
};

// Глобальные функции для удобства использования
void EnableAstVisualization(const TString& outputDir = "/tmp/ast_graphs");
void DisableAstVisualization();
bool IsAstVisualizationEnabled();

// Макросы для встраивания в код
#define AST_VISUALIZE_IF_ENABLED(node, name) \
    do { \
        if (IsAstVisualizationEnabled()) { \
            TAstTypeVisualizer::DumpAstIfEnabled(node, name); \
        } \
    } while(0)

#define AST_VISUALIZE_STAGE(node, stage_name) \
    AST_VISUALIZE_IF_ENABLED(node, TStringBuilder() << "stage_" << stage_name)

#define AST_VISUALIZE_TRANSFORM(node, transform_name) \
    AST_VISUALIZE_IF_ENABLED(node, TStringBuilder() << "transform_" << transform_name)

// Макросы для TExprBase (автоматически извлекают TExprNode::TPtr)
#define AST_VISUALIZE_BASE_IF_ENABLED(exprbase, name) \
    AST_VISUALIZE_IF_ENABLED((exprbase).Ptr(), name)

#define AST_VISUALIZE_BASE_STAGE(exprbase, stage_name) \
    AST_VISUALIZE_BASE_IF_ENABLED(exprbase, TStringBuilder() << "stage_" << stage_name)

#define AST_VISUALIZE_BASE_TRANSFORM(exprbase, transform_name) \
    AST_VISUALIZE_BASE_IF_ENABLED(exprbase, TStringBuilder() << "transform_" << transform_name)

} // namespace NKikimr::NKqp::NOpt
