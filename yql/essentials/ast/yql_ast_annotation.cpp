#include "yql_ast_annotation.h"
#include <util/string/printf.h>
#include <util/string/split.h>
#include <util/string/cast.h>
#include <util/string/builder.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>

namespace NYql {

namespace {

TAstNode* AnnotateNodePosition(TAstNode& node, TMemoryPool& pool) {
    auto newPosition = node.GetPosition();
    TAstNode* pos = PositionAsNode(node.GetPosition(), pool);
    TAstNode* shallowClone = &node;
    if (node.IsList()) {
        TSmallVec<TAstNode*> listChildren(node.GetChildrenCount());
        for (ui32 index = 0; index < node.GetChildrenCount(); ++index) {
            listChildren[index] = AnnotateNodePosition(*node.GetChild(index), pool);
        }

        shallowClone = TAstNode::NewList(node.GetPosition(), listChildren.data(), listChildren.size(), pool);
    }

    return TAstNode::NewList(newPosition, pool, pos, shallowClone);
}

TAstNode* RemoveNodeAnnotations(TAstNode& node, TMemoryPool& pool) {
    if (!node.IsList())
        return nullptr;

    if (node.GetChildrenCount() == 0)
        return nullptr;

    auto lastNode = node.GetChild(node.GetChildrenCount() - 1);
    auto res = lastNode;
    if (lastNode->IsList()) {
        TSmallVec<TAstNode*> listChildren(lastNode->GetChildrenCount());
        for (ui32 index = 0; index < lastNode->GetChildrenCount(); ++index) {
            auto item = RemoveNodeAnnotations(*lastNode->GetChild(index), pool);
            if (!item)
                return nullptr;

            listChildren[index] = item;
        }

        res = TAstNode::NewList(lastNode->GetPosition(), listChildren.data(), listChildren.size(), pool);
    }

    return res;
}

TAstNode* ExtractNodeAnnotations(TAstNode& node, TAnnotationNodeMap& annotations, TMemoryPool& pool) {
    if (!node.IsList())
        return nullptr;

    if (node.GetChildrenCount() == 0)
        return nullptr;

    auto lastNode = node.GetChild(node.GetChildrenCount() - 1);
    auto res = lastNode;
    if (lastNode->IsList()) {
        TSmallVec<TAstNode*> listChildren(lastNode->GetChildrenCount());
        for (ui32 index = 0; index < lastNode->GetChildrenCount(); ++index) {
            auto item = ExtractNodeAnnotations(*lastNode->GetChild(index), annotations, pool);
            if (!item)
                return nullptr;

            listChildren[index] = item;
        }

        res = TAstNode::NewList(lastNode->GetPosition(), listChildren.data(), listChildren.size(), pool);
    }

    auto& v = annotations[res];
    v.resize(node.GetChildrenCount() - 1);
    for (ui32 index = 0; index + 1 < node.GetChildrenCount(); ++index) {
        v[index] = node.GetChild(index);
    }

    return res;
}

TAstNode* ApplyNodePositionAnnotations(TAstNode& node, ui32 annotationIndex, TMemoryPool& pool) {
    if (!node.IsList())
        return nullptr;

    if (node.GetChildrenCount() < annotationIndex + 2)
        return nullptr;

    auto annotation = node.GetChild(annotationIndex);
    auto str = annotation->GetContent();
    TStringBuf rowPart;
    TStringBuf colPart;
    TString filePart;
    GetNext(str, ':', rowPart);
    GetNext(str, ':', colPart);
    filePart = str;

    ui32 row = 0, col = 0;
    if (!TryFromString(rowPart, row) || !TryFromString(colPart, col))
        return nullptr;

    TSmallVec<TAstNode*> listChildren(node.GetChildrenCount());
    for (ui32 index = 0; index < node.GetChildrenCount() - 1; ++index) {
        listChildren[index] = node.GetChild(index);
    }

    auto lastNode = node.GetChild(node.GetChildrenCount() - 1);
    TAstNode* lastResNode;
    if (lastNode->IsAtom()) {
        lastResNode = TAstNode::NewAtom(TPosition(col, row, filePart), lastNode->GetContent(), pool, lastNode->GetFlags());
    } else {
        TSmallVec<TAstNode*> lastNodeChildren(lastNode->GetChildrenCount());
        for (ui32 index = 0; index < lastNode->GetChildrenCount(); ++index) {
            lastNodeChildren[index] = ApplyNodePositionAnnotations(*lastNode->GetChild(index), annotationIndex, pool);
        }

        lastResNode = TAstNode::NewList(TPosition(col, row, filePart), lastNodeChildren.data(), lastNodeChildren.size(), pool);
    }

    listChildren[node.GetChildrenCount() - 1] = lastResNode;
    return TAstNode::NewList(node.GetPosition(), listChildren.data(), listChildren.size(), pool);
}

bool ApplyNodePositionAnnotationsInplace(TAstNode& node, ui32 annotationIndex) {
    if (!node.IsList())
        return false;

    if (node.GetChildrenCount() < annotationIndex + 2)
        return false;

    auto annotation = node.GetChild(annotationIndex);
    TStringBuf str = annotation->GetContent();
    TStringBuf rowPart;
    TStringBuf colPart;
    TString filePart;
    GetNext(str, ':', rowPart);
    GetNext(str, ':', colPart);
    filePart = str;
    ui32 row = 0, col = 0;
    if (!TryFromString(rowPart, row) || !TryFromString(colPart, col))
        return false;

    auto lastNode = node.GetChild(node.GetChildrenCount() - 1);
    lastNode->SetPosition(TPosition(col, row, filePart));
    if (lastNode->IsList()) {
        for (ui32 index = 0; index < lastNode->GetChildrenCount(); ++index) {
            if (!ApplyNodePositionAnnotationsInplace(*lastNode->GetChild(index), annotationIndex))
                return false;
        }
    }

    return true;
}

}

TAstNode* AnnotatePositions(TAstNode& root, TMemoryPool& pool) {
    return AnnotateNodePosition(root, pool);
}

TAstNode* RemoveAnnotations(TAstNode& root, TMemoryPool& pool) {
    return RemoveNodeAnnotations(root, pool);
}

TAstNode* ApplyPositionAnnotations(TAstNode& root, ui32 annotationIndex, TMemoryPool& pool) {
    return ApplyNodePositionAnnotations(root, annotationIndex, pool);
}

bool ApplyPositionAnnotationsInplace(TAstNode& root, ui32 annotationIndex) {
    return ApplyNodePositionAnnotationsInplace(root, annotationIndex);
}

TAstNode* PositionAsNode(TPosition position, TMemoryPool& pool) {
    TStringBuilder str;
    str << position.Row << ':' << position.Column;
    if (!position.File.empty()) {
        str << ':' << position.File;
    }

    return TAstNode::NewAtom(position, str, pool);
}

TAstNode* ExtractAnnotations(TAstNode& root, TAnnotationNodeMap& annotations, TMemoryPool& pool) {
    return ExtractNodeAnnotations(root, annotations, pool);
}

}
