// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxcrt/xml/cfx_xmlnode.h"

CFX_XMLNode::CFX_XMLNode() = default;

CFX_XMLNode::~CFX_XMLNode() = default;

void CFX_XMLNode::InsertChildNode(CFX_XMLNode* pNode, int32_t index) {
  InsertBefore(pNode, GetNthChild(index));
}

CFX_XMLNode* CFX_XMLNode::GetRoot() {
  CFX_XMLNode* pParent = this;
  while (pParent->GetParent())
    pParent = pParent->GetParent();

  return pParent;
}
