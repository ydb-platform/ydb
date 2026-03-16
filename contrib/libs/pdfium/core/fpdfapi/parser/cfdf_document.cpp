// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/parser/cfdf_document.h"

#include <memory>
#include <sstream>
#include <utility>

#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_syntax_parser.h"
#include "core/fpdfapi/parser/fpdf_parser_utility.h"
#include "core/fxcrt/cfx_read_only_span_stream.h"
#include "core/fxcrt/fx_string_wrappers.h"
#include "core/fxcrt/span.h"

CFDF_Document::CFDF_Document() = default;

CFDF_Document::~CFDF_Document() = default;

std::unique_ptr<CFDF_Document> CFDF_Document::CreateNewDoc() {
  auto pDoc = std::make_unique<CFDF_Document>();
  pDoc->m_pRootDict = pDoc->NewIndirect<CPDF_Dictionary>();
  pDoc->m_pRootDict->SetNewFor<CPDF_Dictionary>("FDF");
  return pDoc;
}

std::unique_ptr<CFDF_Document> CFDF_Document::ParseMemory(
    pdfium::span<const uint8_t> span) {
  auto pDoc = std::make_unique<CFDF_Document>();
  pDoc->ParseStream(pdfium::MakeRetain<CFX_ReadOnlySpanStream>(span));
  if (!pDoc->m_pRootDict) {
    return nullptr;
  }
  return pDoc;
}

void CFDF_Document::ParseStream(RetainPtr<IFX_SeekableReadStream> pFile) {
  m_pFile = std::move(pFile);
  CPDF_SyntaxParser parser(m_pFile);
  while (true) {
    CPDF_SyntaxParser::WordResult word_result = parser.GetNextWord();
    if (word_result.is_number) {
      uint32_t objnum = FXSYS_atoui(word_result.word.c_str());
      if (!objnum)
        break;

      word_result = parser.GetNextWord();
      if (!word_result.is_number)
        break;

      word_result = parser.GetNextWord();
      if (word_result.word != "obj")
        break;

      RetainPtr<CPDF_Object> pObj = parser.GetObjectBody(this);
      if (!pObj)
        break;

      ReplaceIndirectObjectIfHigherGeneration(objnum, std::move(pObj));
      word_result = parser.GetNextWord();
      if (word_result.word != "endobj")
        break;
    } else {
      if (word_result.word != "trailer")
        break;

      RetainPtr<CPDF_Dictionary> pMainDict =
          ToDictionary(parser.GetObjectBody(this));
      if (pMainDict)
        m_pRootDict = pMainDict->GetMutableDictFor("Root");

      break;
    }
  }
}

ByteString CFDF_Document::WriteToString() const {
  if (!m_pRootDict)
    return ByteString();

  fxcrt::ostringstream buf;
  buf << "%FDF-1.2\r\n";
  for (const auto& pair : *this)
    buf << pair.first << " 0 obj\r\n"
        << pair.second.Get() << "\r\nendobj\r\n\r\n";

  buf << "trailer\r\n<</Root " << m_pRootDict->GetObjNum()
      << " 0 R>>\r\n%%EOF\r\n";

  return ByteString(buf);
}
