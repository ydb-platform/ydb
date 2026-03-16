// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef CORE_FPDFAPI_PARSER_CPDF_OBJECT_WALKER_H_
#define CORE_FPDFAPI_PARSER_CPDF_OBJECT_WALKER_H_

#include <memory>
#include <stack>

#include "core/fxcrt/bytestring.h"
#include "core/fxcrt/retain_ptr.h"

class CPDF_Object;

// Walk on all non-null sub-objects in an object in depth, include itself,
// like in flat list.
class CPDF_ObjectWalker {
 public:
  class SubobjectIterator {
   public:
    virtual ~SubobjectIterator();
    virtual bool IsFinished() const = 0;
    bool IsStarted() const { return is_started_; }
    RetainPtr<const CPDF_Object> Increment();
    const CPDF_Object* object() const { return object_.Get(); }

   protected:
    explicit SubobjectIterator(RetainPtr<const CPDF_Object> object);

    virtual RetainPtr<const CPDF_Object> IncrementImpl() = 0;
    virtual void Start() = 0;

   private:
    RetainPtr<const CPDF_Object> object_;
    bool is_started_ = false;
  };

  explicit CPDF_ObjectWalker(RetainPtr<const CPDF_Object> root);
  ~CPDF_ObjectWalker();

  RetainPtr<const CPDF_Object> GetNext();
  void SkipWalkIntoCurrentObject();

  size_t current_depth() const { return current_depth_; }
  const CPDF_Object* GetParent() const { return parent_object_.Get(); }
  const ByteString& dictionary_key() const { return dict_key_; }

 private:
  static std::unique_ptr<SubobjectIterator> MakeIterator(
      RetainPtr<const CPDF_Object> object);

  RetainPtr<const CPDF_Object> next_object_;
  RetainPtr<const CPDF_Object> parent_object_;
  ByteString dict_key_;
  size_t current_depth_ = 0;
  std::stack<std::unique_ptr<SubobjectIterator>> stack_;
};

class CPDF_NonConstObjectWalker final : public CPDF_ObjectWalker {
 public:
  explicit CPDF_NonConstObjectWalker(RetainPtr<CPDF_Object> root);
  RetainPtr<CPDF_Object> GetNext();
};

#endif  // CORE_FPDFAPI_PARSER_CPDF_OBJECT_WALKER_H_
