// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_PARSER_CPDF_OBJECT_H_
#define CORE_FPDFAPI_PARSER_CPDF_OBJECT_H_

#include <stdint.h>

#include <set>
#include <type_traits>

#include "core/fxcrt/fx_string.h"
#include "core/fxcrt/retain_ptr.h"

class CPDF_Array;
class CPDF_Boolean;
class CPDF_Dictionary;
class CPDF_Encryptor;
class CPDF_IndirectObjectHolder;
class CPDF_Name;
class CPDF_Null;
class CPDF_Number;
class CPDF_Reference;
class CPDF_Stream;
class CPDF_String;
class IFX_ArchiveStream;

// ISO 32000-1:2008 defines PDF objects. When CPDF_Parser parses a PDF object,
// it represents the PDF object using CPDF_Objects. Take this PDF object for
// example:
//
// 4 0 obj <<
//   /Type /Pages
//   /Count 1
//   /Kids [9 0 R]
// >>
//
// Multiple CPDF_Objects instances are necessary to represent this PDF object:
// 1) A CPDF_Dictionary with object number 4 that contains 3 elements.
// 2) A CPDF_Name for /Pages.
// 3) A CPDF_Number for the count of 1.
// 4) A CPDF_Array for [9 0 R], which contains 1 element.
// 5) A CPDF_Reference that references object 9 0.
//
// CPDF_Object (1) has an object number of 4. All the other CPDF_Objects are
// inline objects. CPDF_Object represent that by using an object number of 0.
class CPDF_Object : public Retainable {
 public:
  static constexpr uint32_t kInvalidObjNum = static_cast<uint32_t>(-1);
  enum Type {
    kBoolean = 1,
    kNumber,
    kString,
    kName,
    kArray,
    kDictionary,
    kStream,
    kNullobj,
    kReference
  };

  uint32_t GetObjNum() const { return m_ObjNum; }
  void SetObjNum(uint32_t objnum) { m_ObjNum = objnum; }
  uint32_t GetGenNum() const { return m_GenNum; }
  void SetGenNum(uint32_t gennum) { m_GenNum = gennum; }
  bool IsInline() const { return m_ObjNum == 0; }
  uint64_t KeyForCache() const;

  virtual Type GetType() const = 0;

  // Create a deep copy of the object.
  virtual RetainPtr<CPDF_Object> Clone() const = 0;

  // Create a deep copy of the object except any reference object be
  // copied to the object it points to directly.
  RetainPtr<CPDF_Object> CloneDirectObject() const;

  virtual ByteString GetString() const;
  virtual WideString GetUnicodeText() const;
  virtual float GetNumber() const;
  virtual int GetInteger() const;

  // Can only be called for objects of types: `kBoolean`, `kNumber`, `kName`,
  // and `kString`.
  virtual void SetString(const ByteString& str);

  virtual CPDF_Array* AsMutableArray();
  virtual CPDF_Boolean* AsMutableBoolean();
  virtual CPDF_Dictionary* AsMutableDictionary();
  virtual CPDF_Name* AsMutableName();
  virtual CPDF_Null* AsMutableNull();
  virtual CPDF_Number* AsMutableNumber();
  virtual CPDF_Reference* AsMutableReference();
  virtual CPDF_Stream* AsMutableStream();
  virtual CPDF_String* AsMutableString();

  virtual bool WriteTo(IFX_ArchiveStream* archive,
                       const CPDF_Encryptor* encryptor) const = 0;

  // Create a deep copy of the object with the option to either
  // copy a reference object or directly copy the object it refers to
  // when |bDirect| is true.
  // Also check cyclic reference against |pVisited|, no copy if it is found.
  // Complex objects should implement their own CloneNonCyclic()
  // function to properly check for possible loop.
  virtual RetainPtr<CPDF_Object> CloneNonCyclic(
      bool bDirect,
      std::set<const CPDF_Object*>* pVisited) const;

  // Return a reference to itself.
  // The object must be direct (!IsInlined).
  virtual RetainPtr<CPDF_Reference> MakeReference(
      CPDF_IndirectObjectHolder* holder) const;

  RetainPtr<const CPDF_Object> GetDirect() const;    // Wraps virtual method.
  RetainPtr<CPDF_Object> GetMutableDirect();         // Wraps virtual method.
  RetainPtr<const CPDF_Dictionary> GetDict() const;  // Wraps virtual method.
  RetainPtr<CPDF_Dictionary> GetMutableDict();       // Wraps virtual method.

  // Const methods wrapping non-const virtual As*() methods.
  const CPDF_Array* AsArray() const;
  const CPDF_Boolean* AsBoolean() const;
  const CPDF_Dictionary* AsDictionary() const;
  const CPDF_Name* AsName() const;
  const CPDF_Null* AsNull() const;
  const CPDF_Number* AsNumber() const;
  const CPDF_Reference* AsReference() const;
  const CPDF_Stream* AsStream() const;
  const CPDF_String* AsString() const;

  // Type-testing methods merely wrap As*() methods.
  bool IsArray() const { return !!AsArray(); }
  bool IsBoolean() const { return !!AsBoolean(); }
  bool IsDictionary() const { return !!AsDictionary(); }
  bool IsName() const { return !!AsName(); }
  bool IsNull() const { return !!AsNull(); }
  bool IsNumber() const { return !!AsNumber(); }
  bool IsReference() const { return !!AsReference(); }
  bool IsStream() const { return !!AsStream(); }
  bool IsString() const { return !!AsString(); }

 protected:
  friend class CPDF_Dictionary;
  friend class CPDF_Reference;

  CPDF_Object() = default;
  CPDF_Object(const CPDF_Object& src) = delete;
  ~CPDF_Object() override;

  virtual const CPDF_Object* GetDirectInternal() const;
  virtual const CPDF_Dictionary* GetDictInternal() const;
  RetainPtr<CPDF_Object> CloneObjectNonCyclic(bool bDirect) const;

  uint32_t m_ObjNum = 0;
  uint32_t m_GenNum = 0;
};

template <typename T>
struct CanInternStrings {
  static constexpr bool value = std::is_same<T, CPDF_Array>::value ||
                                std::is_same<T, CPDF_Dictionary>::value ||
                                std::is_same<T, CPDF_Name>::value ||
                                std::is_same<T, CPDF_String>::value;
};

#endif  // CORE_FPDFAPI_PARSER_CPDF_OBJECT_H_
