#pragma once
#include "yql_ast.h"
#include <util/generic/hash.h>

namespace NYql {

TAstNode* PositionAsNode(TPosition position, TMemoryPool& pool);

TAstNode* AnnotatePositions(TAstNode& root, TMemoryPool& pool);
// returns nullptr in case of error
TAstNode* RemoveAnnotations(TAstNode& root, TMemoryPool& pool);
// returns nullptr in case of error
TAstNode* ApplyPositionAnnotations(TAstNode& root, ui32 annotationIndex, TMemoryPool& pool);
// returns false in case of error
bool ApplyPositionAnnotationsInplace(TAstNode& root, ui32 annotationIndex);

using TAnnotationNodeMap = THashMap<const TAstNode*, TVector<const TAstNode*>>;

// returns nullptr in case of error
TAstNode* ExtractAnnotations(TAstNode& root, TAnnotationNodeMap& annotations, TMemoryPool& pool);

} // namespace NYql
