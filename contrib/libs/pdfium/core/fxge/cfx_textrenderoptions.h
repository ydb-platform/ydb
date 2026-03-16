// Copyright 2020 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef CORE_FXGE_CFX_TEXTRENDEROPTIONS_H_
#define CORE_FXGE_CFX_TEXTRENDEROPTIONS_H_

struct CFX_TextRenderOptions {
  // AliasingType defines the options for drawing pixels on the edges of the
  // text. The values are defined in an incrementing order due to the latter
  // aliasing type's dependency on the previous one.
  enum AliasingType {
    // No transparent pixels on glyph edges.
    kAliasing,

    // May have transparent pixels on glyph edges.
    kAntiAliasing,

    // LCD optimization, can be enabled when anti-aliasing is allowed.
    kLcd,
  };

  constexpr CFX_TextRenderOptions() = default;
  constexpr explicit CFX_TextRenderOptions(AliasingType type)
      : aliasing_type(type) {}
  constexpr CFX_TextRenderOptions(const CFX_TextRenderOptions& other) = default;
  CFX_TextRenderOptions& operator=(const CFX_TextRenderOptions& other) =
      default;

  // Indicates whether anti-aliasing is enabled.
  bool IsSmooth() const {
    return aliasing_type == kAntiAliasing || aliasing_type == kLcd;
  }

  // Aliasing option for fonts.
  AliasingType aliasing_type = kAntiAliasing;

  // Font is CID font.
  bool font_is_cid = false;

  // Using the native text output available on some platforms.
  bool native_text = true;
};

inline bool operator==(const CFX_TextRenderOptions& lhs,
                       const CFX_TextRenderOptions& rhs) {
  return lhs.aliasing_type == rhs.aliasing_type &&
         lhs.font_is_cid == rhs.font_is_cid &&
         lhs.native_text == rhs.native_text;
}

inline bool operator!=(const CFX_TextRenderOptions& lhs,
                       const CFX_TextRenderOptions& rhs) {
  return !(lhs == rhs);
}

#endif  // CORE_FXGE_CFX_TEXTRENDEROPTIONS_H_
