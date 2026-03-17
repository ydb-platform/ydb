// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxcrt/xml/cfx_xmlparser.h"

#include <stdint.h>

#include <algorithm>
#include <iterator>
#include <stack>
#include <utility>

#include "core/fxcrt/autorestorer.h"
#include "core/fxcrt/cfx_seekablestreamproxy.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/data_vector.h"
#include "core/fxcrt/fx_codepage.h"
#include "core/fxcrt/fx_extension.h"
#include "core/fxcrt/fx_safe_types.h"
#include "core/fxcrt/notreached.h"
#include "core/fxcrt/xml/cfx_xmlchardata.h"
#include "core/fxcrt/xml/cfx_xmldocument.h"
#include "core/fxcrt/xml/cfx_xmlelement.h"
#include "core/fxcrt/xml/cfx_xmlinstruction.h"
#include "core/fxcrt/xml/cfx_xmlnode.h"
#include "core/fxcrt/xml/cfx_xmltext.h"

namespace {

constexpr size_t kCurrentTextReserve = 128;
constexpr uint32_t kMaxCharRange = 0x10ffff;

bool IsXMLWhiteSpace(wchar_t ch) {
  return ch == L' ' || ch == 0x0A || ch == 0x0D || ch == 0x09;
}

struct FX_XMLNAMECHAR {
  uint16_t wStart;
  uint16_t wEnd;
  bool bStartChar;
};

constexpr FX_XMLNAMECHAR kXMLNameChars[] = {
    {L'-', L'.', false},    {L'0', L'9', false},     {L':', L':', false},
    {L'A', L'Z', true},     {L'_', L'_', true},      {L'a', L'z', true},
    {0xB7, 0xB7, false},    {0xC0, 0xD6, true},      {0xD8, 0xF6, true},
    {0xF8, 0x02FF, true},   {0x0300, 0x036F, false}, {0x0370, 0x037D, true},
    {0x037F, 0x1FFF, true}, {0x200C, 0x200D, true},  {0x203F, 0x2040, false},
    {0x2070, 0x218F, true}, {0x2C00, 0x2FEF, true},  {0x3001, 0xD7FF, true},
    {0xF900, 0xFDCF, true}, {0xFDF0, 0xFFFD, true},
};

}  // namespace

// static
bool CFX_XMLParser::IsXMLNameChar(wchar_t ch, bool bFirstChar) {
  auto* it = std::lower_bound(
      std::begin(kXMLNameChars), std::end(kXMLNameChars), ch,
      [](const FX_XMLNAMECHAR& arg, wchar_t ch) { return arg.wEnd < ch; });
  return it != std::end(kXMLNameChars) && ch >= it->wStart &&
         (!bFirstChar || it->bStartChar);
}

CFX_XMLParser::CFX_XMLParser(const RetainPtr<IFX_SeekableReadStream>& pStream) {
  DCHECK(pStream);

  auto proxy = pdfium::MakeRetain<CFX_SeekableStreamProxy>(pStream);
  FX_CodePage wCodePage = proxy->GetCodePage();
  if (wCodePage != FX_CodePage::kUTF16LE &&
      wCodePage != FX_CodePage::kUTF16BE && wCodePage != FX_CodePage::kUTF8) {
    proxy->SetCodePage(FX_CodePage::kUTF8);
  }
  stream_ = proxy;

  xml_plane_size_ = std::min(xml_plane_size_,
                             pdfium::checked_cast<size_t>(stream_->GetSize()));

  current_text_.Reserve(kCurrentTextReserve);
}

CFX_XMLParser::~CFX_XMLParser() = default;

std::unique_ptr<CFX_XMLDocument> CFX_XMLParser::Parse() {
  auto doc = std::make_unique<CFX_XMLDocument>();
  AutoRestorer<UnownedPtr<CFX_XMLNode>> restorer(&current_node_);
  current_node_ = doc->GetRoot();
  if (!DoSyntaxParse(doc.get())) {
    return nullptr;
  }
  return doc;
}

bool CFX_XMLParser::DoSyntaxParse(CFX_XMLDocument* doc) {
  if (xml_plane_size_ <= 0)
    return false;

  FX_SAFE_SIZE_T alloc_size_safe = xml_plane_size_;
  alloc_size_safe += 1;  // For NUL.
  if (!alloc_size_safe.IsValid())
    return false;

  size_t current_buffer_idx = 0;
  size_t buffer_size = 0;

  DataVector<wchar_t> buffer;
  buffer.resize(alloc_size_safe.ValueOrDie());

  std::stack<wchar_t> character_to_skip_too_stack;
  std::stack<CFX_XMLNode::Type> node_type_stack;
  WideString current_attribute_name;
  FDE_XmlSyntaxState current_parser_state = FDE_XmlSyntaxState::Text;
  wchar_t current_quote_character = 0;
  wchar_t current_character_to_skip_to = 0;

  while (true) {
    if (current_buffer_idx >= buffer_size) {
      if (stream_->IsEOF())
        return true;

      size_t buffer_chars =
          stream_->ReadBlock(pdfium::make_span(buffer).first(xml_plane_size_));
      if (buffer_chars == 0)
        return true;

      current_buffer_idx = 0;
      buffer_size = buffer_chars;
    }

    while (current_buffer_idx < buffer_size) {
      wchar_t ch = buffer[current_buffer_idx];
      switch (current_parser_state) {
        case FDE_XmlSyntaxState::Text:
          if (ch == L'<') {
            if (!current_text_.IsEmpty()) {
              current_node_->AppendLastChild(
                  doc->CreateNode<CFX_XMLText>(GetTextData()));
            } else {
              current_buffer_idx++;
              current_parser_state = FDE_XmlSyntaxState::Node;
            }
          } else {
            // Fail if there is text outside of the root element, ignore
            // whitespace/null.
            if (node_type_stack.empty() && ch && !FXSYS_iswspace(ch))
              return false;
            ProcessTextChar(ch);
            current_buffer_idx++;
          }
          break;
        case FDE_XmlSyntaxState::Node:
          if (ch == L'!') {
            current_buffer_idx++;
            current_parser_state = FDE_XmlSyntaxState::SkipCommentOrDecl;
          } else if (ch == L'/') {
            current_buffer_idx++;
            current_parser_state = FDE_XmlSyntaxState::CloseElement;
          } else if (ch == L'?') {
            node_type_stack.push(CFX_XMLNode::Type::kInstruction);
            current_buffer_idx++;
            current_parser_state = FDE_XmlSyntaxState::Target;
          } else {
            node_type_stack.push(CFX_XMLNode::Type::kElement);
            current_parser_state = FDE_XmlSyntaxState::Tag;
          }
          break;
        case FDE_XmlSyntaxState::Target:
          if (!IsXMLNameChar(ch, current_text_.IsEmpty())) {
            if (current_text_.IsEmpty()) {
              return false;
            }

            current_parser_state = FDE_XmlSyntaxState::TargetData;

            WideString target_name = GetTextData();
            if (target_name.EqualsASCII("originalXFAVersion") ||
                target_name.EqualsASCII("acrobat")) {
              auto* node = doc->CreateNode<CFX_XMLInstruction>(target_name);
              current_node_->AppendLastChild(node);
              current_node_ = node;
            }
          } else {
            current_text_ += ch;
            current_buffer_idx++;
          }
          break;
        case FDE_XmlSyntaxState::Tag:
          if (!IsXMLNameChar(ch, current_text_.IsEmpty())) {
            if (current_text_.IsEmpty()) {
              return false;
            }

            current_parser_state = FDE_XmlSyntaxState::AttriName;

            auto* child = doc->CreateNode<CFX_XMLElement>(GetTextData());
            current_node_->AppendLastChild(child);
            current_node_ = child;
          } else {
            current_text_ += ch;
            current_buffer_idx++;
          }
          break;
        case FDE_XmlSyntaxState::AttriName:
          if (current_text_.IsEmpty() && IsXMLWhiteSpace(ch)) {
            current_buffer_idx++;
            break;
          }
          if (!IsXMLNameChar(ch, current_text_.IsEmpty())) {
            if (current_text_.IsEmpty()) {
              if (node_type_stack.top() == CFX_XMLNode::Type::kElement) {
                if (ch == L'>' || ch == L'/') {
                  current_parser_state = FDE_XmlSyntaxState::BreakElement;
                  break;
                }
              } else if (node_type_stack.top() ==
                         CFX_XMLNode::Type::kInstruction) {
                if (ch == L'?') {
                  current_parser_state = FDE_XmlSyntaxState::CloseInstruction;
                  current_buffer_idx++;
                } else {
                  current_parser_state = FDE_XmlSyntaxState::TargetData;
                }
                break;
              }
              return false;
            } else {
              if (node_type_stack.top() == CFX_XMLNode::Type::kInstruction) {
                if (ch != '=' && !IsXMLWhiteSpace(ch)) {
                  current_parser_state = FDE_XmlSyntaxState::TargetData;
                  break;
                }
              }
              current_parser_state = FDE_XmlSyntaxState::AttriEqualSign;
              current_attribute_name = GetTextData();
            }
          } else {
            current_text_ += ch;
            current_buffer_idx++;
          }
          break;
        case FDE_XmlSyntaxState::AttriEqualSign:
          if (IsXMLWhiteSpace(ch)) {
            current_buffer_idx++;
            break;
          }
          if (ch != L'=') {
            if (node_type_stack.top() == CFX_XMLNode::Type::kInstruction) {
              current_parser_state = FDE_XmlSyntaxState::TargetData;
              break;
            }
            return false;
          } else {
            current_parser_state = FDE_XmlSyntaxState::AttriQuotation;
            current_buffer_idx++;
          }
          break;
        case FDE_XmlSyntaxState::AttriQuotation:
          if (IsXMLWhiteSpace(ch)) {
            current_buffer_idx++;
            break;
          }
          if (ch != L'\"' && ch != L'\'') {
            return false;
          }

          current_quote_character = ch;
          current_parser_state = FDE_XmlSyntaxState::AttriValue;
          current_buffer_idx++;
          break;
        case FDE_XmlSyntaxState::AttriValue:
          if (ch == current_quote_character) {
            if (entity_start_.has_value())
              return false;

            current_quote_character = 0;
            current_buffer_idx++;
            current_parser_state = FDE_XmlSyntaxState::AttriName;

            CFX_XMLElement* elem = ToXMLElement(current_node_);
            if (elem)
              elem->SetAttribute(current_attribute_name, GetTextData());

            current_attribute_name.clear();
          } else {
            ProcessTextChar(ch);
            current_buffer_idx++;
          }
          break;
        case FDE_XmlSyntaxState::CloseInstruction:
          if (ch != L'>') {
            current_text_ += ch;
            current_parser_state = FDE_XmlSyntaxState::TargetData;
          } else if (!current_text_.IsEmpty()) {
            ProcessTargetData();
          } else {
            current_buffer_idx++;
            if (node_type_stack.empty())
              return false;

            node_type_stack.pop();
            current_parser_state = FDE_XmlSyntaxState::Text;

            if (current_node_ &&
                current_node_->GetType() == CFX_XMLNode::Type::kInstruction)
              current_node_ = current_node_->GetParent();
          }
          break;
        case FDE_XmlSyntaxState::BreakElement:
          if (ch == L'>') {
            current_parser_state = FDE_XmlSyntaxState::Text;
          } else if (ch == L'/') {
            current_parser_state = FDE_XmlSyntaxState::CloseElement;
          } else {
            return false;
          }
          current_buffer_idx++;
          break;
        case FDE_XmlSyntaxState::CloseElement:
          if (!IsXMLNameChar(ch, current_text_.IsEmpty())) {
            if (ch == L'>') {
              if (node_type_stack.empty())
                return false;

              node_type_stack.pop();
              current_parser_state = FDE_XmlSyntaxState::Text;

              CFX_XMLElement* element = ToXMLElement(current_node_);
              if (!element)
                return false;

              WideString element_name = GetTextData();
              if (element_name.GetLength() > 0 &&
                  element_name != element->GetName()) {
                return false;
              }

              current_node_ = current_node_->GetParent();
            } else if (!IsXMLWhiteSpace(ch)) {
              return false;
            }
          } else {
            current_text_ += ch;
          }
          current_buffer_idx++;
          break;
        case FDE_XmlSyntaxState::SkipCommentOrDecl: {
          auto current_view = WideStringView(
              pdfium::make_span(buffer).subspan(current_buffer_idx));
          if (current_view.First(2).EqualsASCII("--")) {
            current_buffer_idx += 2;
            current_parser_state = FDE_XmlSyntaxState::SkipComment;
          } else if (current_view.First(7).EqualsASCIINoCase("[CDATA[")) {
            current_buffer_idx += 7;
            current_parser_state = FDE_XmlSyntaxState::SkipCData;
          } else {
            current_parser_state = FDE_XmlSyntaxState::SkipDeclNode;
            current_character_to_skip_to = L'>';
            character_to_skip_too_stack.push(L'>');
          }
          break;
        }
        case FDE_XmlSyntaxState::SkipCData: {
          auto current_view = WideStringView(
              pdfium::make_span(buffer).subspan(current_buffer_idx));
          if (current_view.First(3).EqualsASCII("]]>")) {
            current_buffer_idx += 3;
            current_parser_state = FDE_XmlSyntaxState::Text;
            current_node_->AppendLastChild(
                doc->CreateNode<CFX_XMLCharData>(GetTextData()));
          } else {
            current_text_ += ch;
            current_buffer_idx++;
          }
          break;
        }
        case FDE_XmlSyntaxState::SkipDeclNode:
          if (current_character_to_skip_to == L'\'' ||
              current_character_to_skip_to == L'\"') {
            current_buffer_idx++;
            if (ch != current_character_to_skip_to)
              break;

            character_to_skip_too_stack.pop();
            if (character_to_skip_too_stack.empty())
              current_parser_state = FDE_XmlSyntaxState::Text;
            else
              current_character_to_skip_to = character_to_skip_too_stack.top();
          } else {
            switch (ch) {
              case L'<':
                current_character_to_skip_to = L'>';
                character_to_skip_too_stack.push(L'>');
                break;
              case L'[':
                current_character_to_skip_to = L']';
                character_to_skip_too_stack.push(L']');
                break;
              case L'(':
                current_character_to_skip_to = L')';
                character_to_skip_too_stack.push(L')');
                break;
              case L'\'':
                current_character_to_skip_to = L'\'';
                character_to_skip_too_stack.push(L'\'');
                break;
              case L'\"':
                current_character_to_skip_to = L'\"';
                character_to_skip_too_stack.push(L'\"');
                break;
              default:
                if (ch == current_character_to_skip_to) {
                  character_to_skip_too_stack.pop();
                  if (character_to_skip_too_stack.empty()) {
                    current_parser_state = FDE_XmlSyntaxState::Text;
                  } else {
                    current_character_to_skip_to =
                        character_to_skip_too_stack.top();
                  }
                }
                break;
            }
            current_buffer_idx++;
          }
          break;
        case FDE_XmlSyntaxState::SkipComment: {
          auto current_view = WideStringView(
              pdfium::make_span(buffer).subspan(current_buffer_idx));
          if (current_view.First(3).EqualsASCII("-->")) {
            current_buffer_idx += 2;
            current_parser_state = FDE_XmlSyntaxState::Text;
          }
          current_buffer_idx++;
          break;
        }
        case FDE_XmlSyntaxState::TargetData:
          if (IsXMLWhiteSpace(ch)) {
            if (current_text_.IsEmpty()) {
              current_buffer_idx++;
              break;
            }
            if (current_quote_character == 0) {
              current_buffer_idx++;
              ProcessTargetData();
              break;
            }
          }
          if (ch == '?') {
            current_parser_state = FDE_XmlSyntaxState::CloseInstruction;
            current_buffer_idx++;
          } else if (ch == '\"') {
            if (current_quote_character == 0) {
              current_quote_character = ch;
              current_buffer_idx++;
            } else if (ch == current_quote_character) {
              current_quote_character = 0;
              current_buffer_idx++;
              ProcessTargetData();
            } else {
              return false;
            }
          } else {
            current_text_ += ch;
            current_buffer_idx++;
          }
          break;
      }
    }
  }

  NOTREACHED_NORETURN();
}

void CFX_XMLParser::ProcessTextChar(wchar_t character) {
  current_text_ += character;

  if (entity_start_.has_value() && character == L';') {
    // Copy the entity out into a string and remove from the current text. When
    // we copy the entity we don't want to copy out the & or the ; so we start
    // shifted by one and want to copy 2 less characters in total.
    WideString csEntity = current_text_.Substr(
        entity_start_.value() + 1,
        current_text_.GetLength() - entity_start_.value() - 2);

    current_text_.Delete(entity_start_.value(),
                         current_text_.GetLength() - entity_start_.value());

    size_t iLen = csEntity.GetLength();
    if (iLen > 0) {
      if (csEntity[0] == L'#') {
        uint32_t ch = 0;
        if (iLen > 1 && csEntity[1] == L'x') {
          for (size_t i = 2; i < iLen; i++) {
            if (!FXSYS_IsHexDigit(csEntity[i]))
              break;
            ch = (ch << 4) + FXSYS_HexCharToInt(csEntity[i]);
          }
        } else {
          for (size_t i = 1; i < iLen; i++) {
            if (!FXSYS_IsDecimalDigit(csEntity[i]))
              break;
            ch = ch * 10 + FXSYS_DecimalCharToInt(csEntity[i]);
          }
        }
        if (ch > kMaxCharRange)
          ch = ' ';

        character = static_cast<wchar_t>(ch);
        if (character != 0)
          current_text_ += character;
      } else {
        if (csEntity.EqualsASCII("amp")) {
          current_text_ += L'&';
        } else if (csEntity.EqualsASCII("lt")) {
          current_text_ += L'<';
        } else if (csEntity.EqualsASCII("gt")) {
          current_text_ += L'>';
        } else if (csEntity.EqualsASCII("apos")) {
          current_text_ += L'\'';
        } else if (csEntity.EqualsASCII("quot")) {
          current_text_ += L'"';
        }
      }
    }
    entity_start_ = std::nullopt;
  } else if (!entity_start_.has_value() && character == L'&') {
    entity_start_ = current_text_.GetLength() - 1;
  }
}

void CFX_XMLParser::ProcessTargetData() {
  WideString target_data = GetTextData();
  if (target_data.IsEmpty())
    return;

  CFX_XMLInstruction* instruction = ToXMLInstruction(current_node_);
  if (instruction)
    instruction->AppendData(target_data);
}

WideString CFX_XMLParser::GetTextData() {
  WideString ret = std::move(current_text_);
  current_text_.Reserve(kCurrentTextReserve);
  entity_start_ = std::nullopt;
  return ret;
}
