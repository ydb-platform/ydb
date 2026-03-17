// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxcrt/xml/cfx_xmltext.h"

#include "core/fxcrt/xml/cfx_xmldocument.h"

CFX_XMLText::CFX_XMLText(const WideString& wsText) : text_(wsText) {}

CFX_XMLText::~CFX_XMLText() = default;

CFX_XMLNode::Type CFX_XMLText::GetType() const {
  return Type::kText;
}

CFX_XMLNode* CFX_XMLText::Clone(CFX_XMLDocument* doc) {
  return doc->CreateNode<CFX_XMLText>(text_);
}

void CFX_XMLText::Save(const RetainPtr<IFX_RetainableWriteStream>& pXMLStream) {
  pXMLStream->WriteString(GetText().EncodeEntities().ToUTF8().AsStringView());
}
