// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXCRT_XML_CFX_XMLTEXT_H_
#define CORE_FXCRT_XML_CFX_XMLTEXT_H_

#include "core/fxcrt/widestring.h"
#include "core/fxcrt/xml/cfx_xmlnode.h"

class CFX_XMLDocument;

class CFX_XMLText : public CFX_XMLNode {
 public:
  explicit CFX_XMLText(const WideString& wsText);
  ~CFX_XMLText() override;

  // CFX_XMLNode
  Type GetType() const override;
  CFX_XMLNode* Clone(CFX_XMLDocument* doc) override;
  void Save(const RetainPtr<IFX_RetainableWriteStream>& pXMLStream) override;

  const WideString& GetText() const { return text_; }
  void SetText(const WideString& wsText) { text_ = wsText; }

 private:
  WideString text_;
};

inline bool IsXMLText(const CFX_XMLNode* pNode) {
  CFX_XMLNode::Type type = pNode->GetType();
  return type == CFX_XMLNode::Type::kText ||
         type == CFX_XMLNode::Type::kCharData;
}

inline CFX_XMLText* ToXMLText(CFX_XMLNode* pNode) {
  return pNode && IsXMLText(pNode) ? static_cast<CFX_XMLText*>(pNode) : nullptr;
}

inline const CFX_XMLText* ToXMLText(const CFX_XMLNode* pNode) {
  return pNode && IsXMLText(pNode) ? static_cast<const CFX_XMLText*>(pNode)
                                   : nullptr;
}

#endif  // CORE_FXCRT_XML_CFX_XMLTEXT_H_
