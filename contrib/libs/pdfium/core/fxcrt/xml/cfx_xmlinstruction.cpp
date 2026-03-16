// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxcrt/xml/cfx_xmlinstruction.h"

#include "core/fxcrt/fx_codepage.h"
#include "core/fxcrt/fx_extension.h"
#include "core/fxcrt/xml/cfx_xmldocument.h"

CFX_XMLInstruction::CFX_XMLInstruction(const WideString& wsTarget)
    : name_(wsTarget) {}

CFX_XMLInstruction::~CFX_XMLInstruction() = default;

CFX_XMLNode::Type CFX_XMLInstruction::GetType() const {
  return Type::kInstruction;
}

CFX_XMLNode* CFX_XMLInstruction::Clone(CFX_XMLDocument* doc) {
  auto* node = doc->CreateNode<CFX_XMLInstruction>(name_);
  node->target_data_ = target_data_;
  return node;
}

void CFX_XMLInstruction::AppendData(const WideString& wsData) {
  target_data_.push_back(wsData);
}

bool CFX_XMLInstruction::IsOriginalXFAVersion() const {
  return name_.EqualsASCII("originalXFAVersion");
}

bool CFX_XMLInstruction::IsAcrobat() const {
  return name_.EqualsASCII("acrobat");
}

void CFX_XMLInstruction::Save(
    const RetainPtr<IFX_RetainableWriteStream>& pXMLStream) {
  if (name_.EqualsASCIINoCase("xml")) {
    pXMLStream->WriteString("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    return;
  }

  pXMLStream->WriteString("<?");
  pXMLStream->WriteString(name_.ToUTF8().AsStringView());
  pXMLStream->WriteString(" ");

  for (const WideString& target : target_data_) {
    pXMLStream->WriteString(target.ToUTF8().AsStringView());
    pXMLStream->WriteString(" ");
  }

  pXMLStream->WriteString("?>\n");
}
