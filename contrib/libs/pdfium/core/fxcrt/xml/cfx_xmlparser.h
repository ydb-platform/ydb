// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXCRT_XML_CFX_XMLPARSER_H_
#define CORE_FXCRT_XML_CFX_XMLPARSER_H_

#include <memory>
#include <optional>

#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/unowned_ptr.h"
#include "core/fxcrt/widestring.h"

class CFX_SeekableStreamProxy;
class CFX_XMLDocument;
class CFX_XMLNode;
class IFX_SeekableReadStream;

class CFX_XMLParser final {
 public:
  static bool IsXMLNameChar(wchar_t ch, bool bFirstChar);

  explicit CFX_XMLParser(const RetainPtr<IFX_SeekableReadStream>& pStream);
  ~CFX_XMLParser();

  std::unique_ptr<CFX_XMLDocument> Parse();

 private:
  enum class FDE_XmlSyntaxState {
    Text,
    Node,
    Target,
    Tag,
    AttriName,
    AttriEqualSign,
    AttriQuotation,
    AttriValue,
    CloseInstruction,
    BreakElement,
    CloseElement,
    SkipDeclNode,
    SkipComment,
    SkipCommentOrDecl,
    SkipCData,
    TargetData
  };

  bool DoSyntaxParse(CFX_XMLDocument* doc);
  WideString GetTextData();
  void ProcessTextChar(wchar_t ch);
  void ProcessTargetData();

  UnownedPtr<CFX_XMLNode> current_node_;
  RetainPtr<CFX_SeekableStreamProxy> stream_;
  WideString current_text_;
  size_t xml_plane_size_ = 1024;
  std::optional<size_t> entity_start_;
};

#endif  // CORE_FXCRT_XML_CFX_XMLPARSER_H_
