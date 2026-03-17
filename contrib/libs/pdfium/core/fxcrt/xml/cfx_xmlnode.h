// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXCRT_XML_CFX_XMLNODE_H_
#define CORE_FXCRT_XML_CFX_XMLNODE_H_

#include "core/fxcrt/fx_stream.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/tree_node.h"

class CFX_XMLDocument;

class CFX_XMLNode : public TreeNode<CFX_XMLNode> {
 public:
  enum class Type {
    kInstruction = 0,
    kElement,
    kText,
    kCharData,
  };

  CFX_XMLNode();
  ~CFX_XMLNode() override;

  virtual Type GetType() const = 0;
  virtual CFX_XMLNode* Clone(CFX_XMLDocument* doc) = 0;
  virtual void Save(const RetainPtr<IFX_RetainableWriteStream>& pXMLStream) = 0;

  CFX_XMLNode* GetRoot();
  void InsertChildNode(CFX_XMLNode* pNode, int32_t index);
};

#endif  // CORE_FXCRT_XML_CFX_XMLNODE_H_
