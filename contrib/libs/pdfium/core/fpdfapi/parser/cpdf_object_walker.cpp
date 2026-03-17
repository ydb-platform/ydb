// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "core/fpdfapi/parser/cpdf_object_walker.h"

#include <utility>

#include "core/fpdfapi/parser/cpdf_array.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_stream.h"
#include "core/fxcrt/check.h"

namespace {

class StreamIterator final : public CPDF_ObjectWalker::SubobjectIterator {
 public:
  explicit StreamIterator(RetainPtr<const CPDF_Stream> stream)
      : SubobjectIterator(std::move(stream)) {}

  ~StreamIterator() override = default;

  bool IsFinished() const override { return IsStarted() && is_finished_; }

  RetainPtr<const CPDF_Object> IncrementImpl() override {
    DCHECK(IsStarted());
    DCHECK(!IsFinished());
    is_finished_ = true;
    return object()->GetDict();
  }

  void Start() override {}

 private:
  bool is_finished_ = false;
};

class DictionaryIterator final : public CPDF_ObjectWalker::SubobjectIterator {
 public:
  explicit DictionaryIterator(RetainPtr<const CPDF_Dictionary> dictionary)
      : SubobjectIterator(dictionary), locker_(dictionary) {}

  ~DictionaryIterator() override = default;

  bool IsFinished() const override {
    return IsStarted() && dict_iterator_ == locker_.end();
  }

  RetainPtr<const CPDF_Object> IncrementImpl() override {
    DCHECK(IsStarted());
    DCHECK(!IsFinished());
    RetainPtr<const CPDF_Object> result = dict_iterator_->second;
    dict_key_ = dict_iterator_->first;
    ++dict_iterator_;
    return result;
  }

  void Start() override {
    DCHECK(!IsStarted());
    dict_iterator_ = locker_.begin();
  }

  ByteString dict_key() const { return dict_key_; }

 private:
  CPDF_Dictionary::const_iterator dict_iterator_;
  CPDF_DictionaryLocker locker_;
  ByteString dict_key_;
};

class ArrayIterator final : public CPDF_ObjectWalker::SubobjectIterator {
 public:
  explicit ArrayIterator(RetainPtr<const CPDF_Array> array)
      : SubobjectIterator(array), locker_(array) {}

  ~ArrayIterator() override = default;

  bool IsFinished() const override {
    return IsStarted() && arr_iterator_ == locker_.end();
  }

  RetainPtr<const CPDF_Object> IncrementImpl() override {
    DCHECK(IsStarted());
    DCHECK(!IsFinished());
    RetainPtr<const CPDF_Object> result = *arr_iterator_;
    ++arr_iterator_;
    return result;
  }

  void Start() override { arr_iterator_ = locker_.begin(); }

 public:
  CPDF_Array::const_iterator arr_iterator_;
  CPDF_ArrayLocker locker_;
};

}  // namespace

CPDF_ObjectWalker::SubobjectIterator::~SubobjectIterator() = default;

RetainPtr<const CPDF_Object> CPDF_ObjectWalker::SubobjectIterator::Increment() {
  if (!IsStarted()) {
    Start();
    is_started_ = true;
  }
  while (!IsFinished()) {
    RetainPtr<const CPDF_Object> result = IncrementImpl();
    if (result)
      return result;
  }
  return nullptr;
}

CPDF_ObjectWalker::SubobjectIterator::SubobjectIterator(
    RetainPtr<const CPDF_Object> object)
    : object_(std::move(object)) {
  DCHECK(object_);
}

// static
std::unique_ptr<CPDF_ObjectWalker::SubobjectIterator>
CPDF_ObjectWalker::MakeIterator(RetainPtr<const CPDF_Object> object) {
  if (object->IsStream())
    return std::make_unique<StreamIterator>(ToStream(object));
  if (object->IsDictionary())
    return std::make_unique<DictionaryIterator>(ToDictionary(object));
  if (object->IsArray())
    return std::make_unique<ArrayIterator>(ToArray(object));
  return nullptr;
}

CPDF_ObjectWalker::CPDF_ObjectWalker(RetainPtr<const CPDF_Object> root)
    : next_object_(std::move(root)) {}

CPDF_ObjectWalker::~CPDF_ObjectWalker() = default;

RetainPtr<const CPDF_Object> CPDF_ObjectWalker::GetNext() {
  while (!stack_.empty() || next_object_) {
    if (next_object_) {
      auto new_iterator = MakeIterator(next_object_);
      if (new_iterator) {
        // Schedule walk within composite objects.
        stack_.push(std::move(new_iterator));
      }
      return std::move(next_object_);  // next_object_ is NULL after move.
    }

    SubobjectIterator* it = stack_.top().get();
    if (it->IsFinished()) {
      stack_.pop();
    } else {
      next_object_ = it->Increment();
      parent_object_.Reset(it->object());
      dict_key_ = parent_object_->IsDictionary()
                      ? static_cast<DictionaryIterator*>(it)->dict_key()
                      : ByteString();
      current_depth_ = stack_.size();
    }
  }
  dict_key_ = ByteString();
  current_depth_ = 0;
  return nullptr;
}

void CPDF_ObjectWalker::SkipWalkIntoCurrentObject() {
  if (stack_.empty() || stack_.top()->IsStarted())
    return;
  stack_.pop();
}

CPDF_NonConstObjectWalker::CPDF_NonConstObjectWalker(
    RetainPtr<CPDF_Object> root)
    : CPDF_ObjectWalker(std::move(root)) {}

RetainPtr<CPDF_Object> CPDF_NonConstObjectWalker::GetNext() {
  return pdfium::WrapRetain(
      const_cast<CPDF_Object*>(CPDF_ObjectWalker::GetNext().Get()));
}
