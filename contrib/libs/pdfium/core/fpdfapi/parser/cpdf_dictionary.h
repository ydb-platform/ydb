// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_PARSER_CPDF_DICTIONARY_H_
#define CORE_FPDFAPI_PARSER_CPDF_DICTIONARY_H_

#include <functional>
#include <map>
#include <set>
#include <type_traits>
#include <utility>
#include <vector>

#include "core/fpdfapi/parser/cpdf_object.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/fx_coordinates.h"
#include "core/fxcrt/fx_string.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/string_pool_template.h"
#include "core/fxcrt/weak_ptr.h"

class CPDF_IndirectObjectHolder;

// Dictionaries never contain nullptr for valid keys, but some of the methods
// will return nullptr to indicate non-existent keys.
class CPDF_Dictionary final : public CPDF_Object {
 public:
  using DictMap = std::map<ByteString, RetainPtr<CPDF_Object>, std::less<>>;
  using const_iterator = DictMap::const_iterator;

  CONSTRUCT_VIA_MAKE_RETAIN;

  // CPDF_Object:
  Type GetType() const override;
  RetainPtr<CPDF_Object> Clone() const override;
  CPDF_Dictionary* AsMutableDictionary() override;
  bool WriteTo(IFX_ArchiveStream* archive,
               const CPDF_Encryptor* encryptor) const override;

  bool IsLocked() const { return !!m_LockCount; }

  size_t size() const { return m_Map.size(); }
  RetainPtr<const CPDF_Object> GetObjectFor(const ByteString& key) const;
  RetainPtr<CPDF_Object> GetMutableObjectFor(const ByteString& key);

  RetainPtr<const CPDF_Object> GetDirectObjectFor(const ByteString& key) const;
  RetainPtr<CPDF_Object> GetMutableDirectObjectFor(const ByteString& key);

  // These will return the string representation of the object specified by
  // |key|, for any object type that has a string representation.
  ByteString GetByteStringFor(const ByteString& key) const;
  ByteString GetByteStringFor(const ByteString& key,
                              const ByteString& default_str) const;
  WideString GetUnicodeTextFor(const ByteString& key) const;

  // This will only return the string representation of a name object specified
  // by |key|. Useful when the PDF spec requires the value to be an object of
  // type name. i.e. /Foo and not (Foo).
  ByteString GetNameFor(const ByteString& key) const;

  bool GetBooleanFor(const ByteString& key, bool bDefault) const;
  int GetIntegerFor(const ByteString& key) const;
  int GetIntegerFor(const ByteString& key, int default_int) const;
  int GetDirectIntegerFor(const ByteString& key) const;
  float GetFloatFor(const ByteString& key) const;
  RetainPtr<const CPDF_Dictionary> GetDictFor(const ByteString& key) const;
  RetainPtr<CPDF_Dictionary> GetMutableDictFor(const ByteString& key);
  RetainPtr<CPDF_Dictionary> GetOrCreateDictFor(const ByteString& key);
  RetainPtr<const CPDF_Array> GetArrayFor(const ByteString& key) const;
  RetainPtr<CPDF_Array> GetMutableArrayFor(const ByteString& key);
  RetainPtr<CPDF_Array> GetOrCreateArrayFor(const ByteString& key);
  RetainPtr<const CPDF_Stream> GetStreamFor(const ByteString& key) const;
  RetainPtr<CPDF_Stream> GetMutableStreamFor(const ByteString& key);
  RetainPtr<const CPDF_Number> GetNumberFor(const ByteString& key) const;
  RetainPtr<const CPDF_String> GetStringFor(const ByteString& key) const;
  CFX_FloatRect GetRectFor(const ByteString& key) const;
  CFX_Matrix GetMatrixFor(const ByteString& key) const;

  bool KeyExist(const ByteString& key) const;
  std::vector<ByteString> GetKeys() const;

  // Creates a new object owned by the dictionary and returns an unowned
  // pointer to it. Invalidates iterators for the element with the key |key|.
  // Prefer using these templates over calls to SetFor(), since by creating
  // a new object with no previous references, they ensure cycles can not be
  // introduced.
  template <typename T, typename... Args>
  typename std::enable_if<!CanInternStrings<T>::value, RetainPtr<T>>::type
  SetNewFor(const ByteString& key, Args&&... args) {
    static_assert(!std::is_same<T, CPDF_Stream>::value,
                  "Cannot set a CPDF_Stream directly. Add it indirectly as a "
                  "`CPDF_Reference` instead.");
    return pdfium::WrapRetain(static_cast<T*>(SetForInternal(
        key, pdfium::MakeRetain<T>(std::forward<Args>(args)...))));
  }
  template <typename T, typename... Args>
  typename std::enable_if<CanInternStrings<T>::value, RetainPtr<T>>::type
  SetNewFor(const ByteString& key, Args&&... args) {
    return pdfium::WrapRetain(static_cast<T*>(SetForInternal(
        key, pdfium::MakeRetain<T>(m_pPool, std::forward<Args>(args)...))));
  }

  // If `object` is null, then `key` is erased from the map. Otherwise, takes
  // ownership of `object` and stores in in the map. Invalidates iterators for
  // the element with the key `key`.
  void SetFor(const ByteString& key, RetainPtr<CPDF_Object> object);
  // A stream must be indirect and added as a `CPDF_Reference` instead.
  void SetFor(const ByteString& key, RetainPtr<CPDF_Stream> stream) = delete;

  // Convenience functions to convert native objects to array form.
  void SetRectFor(const ByteString& key, const CFX_FloatRect& rect);
  void SetMatrixFor(const ByteString& key, const CFX_Matrix& matrix);

  void ConvertToIndirectObjectFor(const ByteString& key,
                                  CPDF_IndirectObjectHolder* pHolder);

  // Invalidates iterators for the element with the key |key|.
  RetainPtr<CPDF_Object> RemoveFor(ByteStringView key);

  // Invalidates iterators for the element with the key |oldkey|.
  void ReplaceKey(const ByteString& oldkey, const ByteString& newkey);

  WeakPtr<ByteStringPool> GetByteStringPool() const { return m_pPool; }

 private:
  friend class CPDF_DictionaryLocker;

  CPDF_Dictionary();
  explicit CPDF_Dictionary(const WeakPtr<ByteStringPool>& pPool);
  ~CPDF_Dictionary() override;

  // No guarantees about result lifetime, use with caution.
  const CPDF_Object* GetObjectForInternal(const ByteString& key) const;
  const CPDF_Object* GetDirectObjectForInternal(const ByteString& key) const;
  const CPDF_Array* GetArrayForInternal(const ByteString& key) const;
  const CPDF_Dictionary* GetDictForInternal(const ByteString& key) const;
  const CPDF_Number* GetNumberForInternal(const ByteString& key) const;
  const CPDF_Stream* GetStreamForInternal(const ByteString& key) const;
  const CPDF_String* GetStringForInternal(const ByteString& key) const;
  CPDF_Object* SetForInternal(const ByteString& key,
                              RetainPtr<CPDF_Object> pObj);

  ByteString MaybeIntern(const ByteString& str);
  const CPDF_Dictionary* GetDictInternal() const override;
  RetainPtr<CPDF_Object> CloneNonCyclic(
      bool bDirect,
      std::set<const CPDF_Object*>* visited) const override;

  mutable uint32_t m_LockCount = 0;
  WeakPtr<ByteStringPool> m_pPool;
  DictMap m_Map;
};

class CPDF_DictionaryLocker {
 public:
  FX_STACK_ALLOCATED();
  using const_iterator = CPDF_Dictionary::const_iterator;

  explicit CPDF_DictionaryLocker(const CPDF_Dictionary* pDictionary);
  explicit CPDF_DictionaryLocker(RetainPtr<CPDF_Dictionary> pDictionary);
  explicit CPDF_DictionaryLocker(RetainPtr<const CPDF_Dictionary> pDictionary);
  ~CPDF_DictionaryLocker();

  const_iterator begin() const {
    CHECK(m_pDictionary->IsLocked());
    return m_pDictionary->m_Map.begin();
  }
  const_iterator end() const {
    CHECK(m_pDictionary->IsLocked());
    return m_pDictionary->m_Map.end();
  }

 private:
  RetainPtr<const CPDF_Dictionary> const m_pDictionary;
};

inline CPDF_Dictionary* ToDictionary(CPDF_Object* obj) {
  return obj ? obj->AsMutableDictionary() : nullptr;
}

inline const CPDF_Dictionary* ToDictionary(const CPDF_Object* obj) {
  return obj ? obj->AsDictionary() : nullptr;
}

inline RetainPtr<CPDF_Dictionary> ToDictionary(RetainPtr<CPDF_Object> obj) {
  return RetainPtr<CPDF_Dictionary>(ToDictionary(obj.Get()));
}

inline RetainPtr<const CPDF_Dictionary> ToDictionary(
    RetainPtr<const CPDF_Object> obj) {
  return RetainPtr<const CPDF_Dictionary>(ToDictionary(obj.Get()));
}

#endif  // CORE_FPDFAPI_PARSER_CPDF_DICTIONARY_H_
