// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfdoc/cpdf_metadata.h"

#include <memory>
#include <utility>

#include "core/fpdfapi/parser/cpdf_stream.h"
#include "core/fpdfapi/parser/cpdf_stream_acc.h"
#include "core/fxcrt/cfx_read_only_span_stream.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/fx_codepage.h"
#include "core/fxcrt/xml/cfx_xmldocument.h"
#include "core/fxcrt/xml/cfx_xmlelement.h"
#include "core/fxcrt/xml/cfx_xmlparser.h"

namespace {

constexpr int kMaxMetaDataDepth = 128;

bool CheckForSharedFormInternal(int depth,
                                CFX_XMLElement* element,
                                std::vector<UnsupportedFeature>* unsupported) {
  if (depth >= kMaxMetaDataDepth) {
    return false;
  }

  WideString attr =
      element->GetAttribute(WideString::FromASCII("xmlns:adhocwf"));
  if (attr.EqualsASCII("http://ns.adobe.com/AcrobatAdhocWorkflow/1.0/")) {
    for (const auto* child = element->GetFirstChild(); child;
         child = child->GetNextSibling()) {
      if (child->GetType() != CFX_XMLNode::Type::kElement)
        continue;

      const auto* child_elem = static_cast<const CFX_XMLElement*>(child);
      if (!child_elem->GetName().EqualsASCII("adhocwf:workflowType"))
        continue;

      switch (child_elem->GetTextData().GetInteger()) {
        case 0:
          unsupported->push_back(UnsupportedFeature::kDocumentSharedFormEmail);
          break;
        case 1:
          unsupported->push_back(
              UnsupportedFeature::kDocumentSharedFormAcrobat);
          break;
        case 2:
          unsupported->push_back(
              UnsupportedFeature::kDocumentSharedFormFilesystem);
          break;
      }
      // We only care about the first one we find.
      break;
    }
  }

  for (auto* child = element->GetFirstChild(); child;
       child = child->GetNextSibling()) {
    CFX_XMLElement* xml_element = ToXMLElement(child);
    if (xml_element &&
        !CheckForSharedFormInternal(depth + 1, xml_element, unsupported)) {
      return false;
    }
  }
  return true;
}

}  // namespace

CPDF_Metadata::CPDF_Metadata(RetainPtr<const CPDF_Stream> pStream)
    : stream_(std::move(pStream)) {
  DCHECK(stream_);
}

CPDF_Metadata::~CPDF_Metadata() = default;

std::vector<UnsupportedFeature> CPDF_Metadata::CheckForSharedForm() const {
  auto pAcc = pdfium::MakeRetain<CPDF_StreamAcc>(stream_);
  pAcc->LoadAllDataFiltered();

  auto stream = pdfium::MakeRetain<CFX_ReadOnlySpanStream>(pAcc->GetSpan());
  CFX_XMLParser parser(stream);
  std::unique_ptr<CFX_XMLDocument> doc = parser.Parse();
  if (!doc)
    return {};

  std::vector<UnsupportedFeature> unsupported;
  CheckForSharedFormInternal(/*depth=*/0, doc->GetRoot(), &unsupported);
  return unsupported;
}
