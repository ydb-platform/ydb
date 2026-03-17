// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef FPDFSDK_CPDFSDK_APPSTREAM_H_
#define FPDFSDK_CPDFSDK_APPSTREAM_H_

#include <optional>

#include "core/fxcrt/fx_string.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/unowned_ptr.h"

class CPDFSDK_Widget;
class CPDF_Dictionary;
class CPDF_Stream;

class CPDFSDK_AppStream {
 public:
  CPDFSDK_AppStream(CPDFSDK_Widget* widget, CPDF_Dictionary* dict);
  ~CPDFSDK_AppStream();

  void SetAsPushButton();
  void SetAsCheckBox();
  void SetAsRadioButton();
  void SetAsComboBox(std::optional<WideString> sValue);
  void SetAsListBox();
  void SetAsTextField(std::optional<WideString> sValue);

 private:
  void AddImage(const ByteString& sAPType, const CPDF_Stream* pImage);
  void Write(const ByteString& sAPType,
             const ByteString& sContents,
             const ByteString& sAPState);
  void Remove(ByteStringView sAPType);

  ByteString GetBackgroundAppStream() const;
  ByteString GetBorderAppStream() const;

  UnownedPtr<CPDFSDK_Widget> const widget_;
  RetainPtr<CPDF_Dictionary> const dict_;
};

#endif  // FPDFSDK_CPDFSDK_APPSTREAM_H_
