// Copyright 2018 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef CORE_FPDFAPI_PARSER_CPDF_CROSS_REF_TABLE_H_
#define CORE_FPDFAPI_PARSER_CPDF_CROSS_REF_TABLE_H_

#include <stdint.h>

#include <map>
#include <memory>

#include "core/fxcrt/fx_types.h"
#include "core/fxcrt/retain_ptr.h"

class CPDF_Dictionary;

class CPDF_CrossRefTable {
 public:
  // See ISO 32000-1:2008 table 18.
  enum class ObjectType : uint8_t {
    kFree = 0,
    kNormal = 1,
    kCompressed = 2,
  };

  struct ObjectInfo {
    ObjectType type = ObjectType::kFree;
    bool is_object_stream_flag = false;
    uint16_t gennum = 0;
    // If `type` is `ObjectType::kCompressed`, `archive` should be used.
    // If `type` is `ObjectType::kNormal`, `pos` should be used.
    // In other cases, it is unused.
    union {
      FX_FILESIZE pos = 0;
      struct {
        uint32_t obj_num;
        uint32_t obj_index;
      } archive;
    };
  };

  // Merge cross reference tables.  Apply top on current.
  static std::unique_ptr<CPDF_CrossRefTable> MergeUp(
      std::unique_ptr<CPDF_CrossRefTable> current,
      std::unique_ptr<CPDF_CrossRefTable> top);

  CPDF_CrossRefTable();
  CPDF_CrossRefTable(RetainPtr<CPDF_Dictionary> trailer,
                     uint32_t trailer_object_number);
  ~CPDF_CrossRefTable();

  void AddCompressed(uint32_t obj_num,
                     uint32_t archive_obj_num,
                     uint32_t archive_obj_index);
  void AddNormal(uint32_t obj_num,
                 uint16_t gen_num,
                 bool is_object_stream,
                 FX_FILESIZE pos);
  void SetFree(uint32_t obj_num, uint16_t gen_num);

  void SetTrailer(RetainPtr<CPDF_Dictionary> trailer,
                  uint32_t trailer_object_number);
  uint32_t trailer_object_number() const { return trailer_object_number_; }
  const CPDF_Dictionary* trailer() const { return trailer_.Get(); }
  CPDF_Dictionary* GetMutableTrailerForTesting() { return trailer_.Get(); }

  const ObjectInfo* GetObjectInfo(uint32_t obj_num) const;

  const std::map<uint32_t, ObjectInfo>& objects_info() const {
    return objects_info_;
  }

  void Update(std::unique_ptr<CPDF_CrossRefTable> new_cross_ref);

  // Objects with object number >= `size` will be removed.
  void SetObjectMapSize(uint32_t size);

 private:
  void UpdateInfo(std::map<uint32_t, ObjectInfo> new_objects_info);
  void UpdateTrailer(RetainPtr<CPDF_Dictionary> new_trailer);

  RetainPtr<CPDF_Dictionary> trailer_;
  // `trailer_` can be the dictionary part of a XRef stream object. Since it is
  // inline, it has no object number. Store the stream's object number, or 0 if
  // there is none.
  uint32_t trailer_object_number_ = 0;
  std::map<uint32_t, ObjectInfo> objects_info_;
};

#endif  // CORE_FPDFAPI_PARSER_CPDF_CROSS_REF_TABLE_H_
