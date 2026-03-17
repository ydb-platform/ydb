// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_PAGE_CPDF_PAGEOBJECT_H_
#define CORE_FPDFAPI_PAGE_CPDF_PAGEOBJECT_H_

#include <stdint.h>

#include "core/fpdfapi/page/cpdf_contentmarks.h"
#include "core/fpdfapi/page/cpdf_graphicstates.h"
#include "core/fxcrt/bytestring.h"
#include "core/fxcrt/fx_coordinates.h"
#include "core/fxcrt/span.h"

class CPDF_FormObject;
class CPDF_ImageObject;
class CPDF_PathObject;
class CPDF_ShadingObject;
class CPDF_TextObject;

// Represents an object within the page, like a form or image. Not to be
// confused with the PDF spec's page object that lives in a page tree, which is
// represented by CPDF_Page.
class CPDF_PageObject {
 public:
  // Values must match corresponding values in //public.
  enum class Type {
    kText = 1,
    kPath,
    kImage,
    kShading,
    kForm,
  };

  static constexpr int32_t kNoContentStream = -1;

  explicit CPDF_PageObject(int32_t content_stream);
  CPDF_PageObject(const CPDF_PageObject& src) = delete;
  CPDF_PageObject& operator=(const CPDF_PageObject& src) = delete;
  virtual ~CPDF_PageObject();

  virtual Type GetType() const = 0;
  virtual void Transform(const CFX_Matrix& matrix) = 0;
  virtual bool IsText() const;
  virtual bool IsPath() const;
  virtual bool IsImage() const;
  virtual bool IsShading() const;
  virtual bool IsForm() const;
  virtual CPDF_TextObject* AsText();
  virtual const CPDF_TextObject* AsText() const;
  virtual CPDF_PathObject* AsPath();
  virtual const CPDF_PathObject* AsPath() const;
  virtual CPDF_ImageObject* AsImage();
  virtual const CPDF_ImageObject* AsImage() const;
  virtual CPDF_ShadingObject* AsShading();
  virtual const CPDF_ShadingObject* AsShading() const;
  virtual CPDF_FormObject* AsForm();
  virtual const CPDF_FormObject* AsForm() const;

  void SetDirty(bool value) { m_bDirty = value; }
  bool IsDirty() const { return m_bDirty; }
  void TransformClipPath(const CFX_Matrix& matrix);

  void SetOriginalRect(const CFX_FloatRect& rect) { m_OriginalRect = rect; }
  const CFX_FloatRect& GetOriginalRect() const { return m_OriginalRect; }
  void SetRect(const CFX_FloatRect& rect) { m_Rect = rect; }
  const CFX_FloatRect& GetRect() const { return m_Rect; }
  FX_RECT GetBBox() const;
  FX_RECT GetTransformedBBox(const CFX_Matrix& matrix) const;

  CPDF_ContentMarks* GetContentMarks() { return &m_ContentMarks; }
  const CPDF_ContentMarks* GetContentMarks() const { return &m_ContentMarks; }
  void SetContentMarks(const CPDF_ContentMarks& marks) {
    m_ContentMarks = marks;
  }

  // Get what content stream the object was parsed from in its page. This number
  // is the index of the content stream in the "Contents" array, or 0 if there
  // is a single content stream. If the object is newly created,
  // |kNoContentStream| is returned.
  //
  // If the object is spread among more than one content stream, this is the
  // index of the last stream.
  int32_t GetContentStream() const { return m_ContentStream; }
  void SetContentStream(int32_t new_content_stream) {
    m_ContentStream = new_content_stream;
  }

  const ByteString& GetResourceName() const { return m_ResourceName; }
  void SetResourceName(const ByteString& resource_name) {
    m_ResourceName = resource_name;
  }

  pdfium::span<const ByteString> GetGraphicsResourceNames() const;

  const CPDF_ClipPath& clip_path() const { return m_GraphicStates.clip_path(); }
  CPDF_ClipPath& mutable_clip_path() {
    return m_GraphicStates.mutable_clip_path();
  }

  const CFX_GraphState& graph_state() const {
    return m_GraphicStates.graph_state();
  }
  CFX_GraphState& mutable_graph_state() {
    return m_GraphicStates.mutable_graph_state();
  }

  const CPDF_ColorState& color_state() const {
    return m_GraphicStates.color_state();
  }
  CPDF_ColorState& mutable_color_state() {
    return m_GraphicStates.mutable_color_state();
  }

  const CPDF_TextState& text_state() const {
    return m_GraphicStates.text_state();
  }
  CPDF_TextState& mutable_text_state() {
    return m_GraphicStates.mutable_text_state();
  }

  const CPDF_GeneralState& general_state() const {
    return m_GraphicStates.general_state();
  }
  CPDF_GeneralState& mutable_general_state() {
    return m_GraphicStates.mutable_general_state();
  }

  const CPDF_GraphicStates& graphic_states() const { return m_GraphicStates; }

  void SetDefaultStates();

 protected:
  void CopyData(const CPDF_PageObject* pSrcObject);

 private:
  CPDF_GraphicStates m_GraphicStates;
  CFX_FloatRect m_Rect;
  CFX_FloatRect m_OriginalRect;
  CPDF_ContentMarks m_ContentMarks;
  bool m_bDirty = false;
  int32_t m_ContentStream;
  // The resource name for this object.
  ByteString m_ResourceName;
};

#endif  // CORE_FPDFAPI_PAGE_CPDF_PAGEOBJECT_H_
