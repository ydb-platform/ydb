/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "orc/MemoryPool.hh"
#include "orc/Int128.hh"

#include "Adaptor.hh"

#include <string.h>
#include <cstdlib>
#include <iostream>

namespace orc {

  MemoryPool::~MemoryPool() {
    // PASS
  }

  class MemoryPoolImpl : public MemoryPool {
   public:
    virtual ~MemoryPoolImpl() override;

    char* malloc(uint64_t size) override;
    void free(char* p) override;
  };

  char* MemoryPoolImpl::malloc(uint64_t size) {
    return static_cast<char*>(std::malloc(size));
  }

  void MemoryPoolImpl::free(char* p) {
    std::free(p);
  }

  MemoryPoolImpl::~MemoryPoolImpl() {
    // PASS
  }

  template <class T>
  DataBuffer<T>::DataBuffer(MemoryPool& pool, uint64_t newSize)
      : memoryPool_(pool), buf_(nullptr), currentSize_(0), currentCapacity_(0) {
    reserve(newSize);
    currentSize_ = newSize;
  }

  template <class T>
  DataBuffer<T>::DataBuffer(DataBuffer<T>&& buffer) noexcept
      : memoryPool_(buffer.memoryPool_),
        buf_(buffer.buf_),
        currentSize_(buffer.currentSize_),
        currentCapacity_(buffer.currentCapacity_) {
    buffer.buf_ = nullptr;
    buffer.currentSize_ = 0;
    buffer.currentCapacity_ = 0;
  }

  template <class T>
  DataBuffer<T>::~DataBuffer() {
    for (uint64_t i = currentSize_; i > 0; --i) {
      (buf_ + i - 1)->~T();
    }
    if (buf_) {
      memoryPool_.free(reinterpret_cast<char*>(buf_));
    }
  }

  template <class T>
  void DataBuffer<T>::resize(uint64_t newSize) {
    reserve(newSize);
    if (currentSize_ > newSize) {
      for (uint64_t i = currentSize_; i > newSize; --i) {
        (buf_ + i - 1)->~T();
      }
    } else if (newSize > currentSize_) {
      for (uint64_t i = currentSize_; i < newSize; ++i) {
        new (buf_ + i) T();
      }
    }
    currentSize_ = newSize;
  }

  template <class T>
  void DataBuffer<T>::reserve(uint64_t newCapacity) {
    if (newCapacity > currentCapacity_ || !buf_) {
      if (buf_) {
        T* buf_old = buf_;
        buf_ = reinterpret_cast<T*>(memoryPool_.malloc(sizeof(T) * newCapacity));
        memcpy(buf_, buf_old, sizeof(T) * currentSize_);
        memoryPool_.free(reinterpret_cast<char*>(buf_old));
      } else {
        buf_ = reinterpret_cast<T*>(memoryPool_.malloc(sizeof(T) * newCapacity));
      }
      currentCapacity_ = newCapacity;
    }
  }

  template <class T>
  void DataBuffer<T>::zeroOut() {
    memset(buf_, 0, sizeof(T) * currentCapacity_);
  }

  // Specializations for Int128
  template <>
  void DataBuffer<Int128>::zeroOut() {
    for (uint64_t i = 0; i < currentCapacity_; ++i) {
      new (buf_ + i) Int128();
    }
  }

  // Specializations for char

  template <>
  DataBuffer<char>::~DataBuffer() {
    if (buf_) {
      memoryPool_.free(reinterpret_cast<char*>(buf_));
    }
  }

  template <>
  void DataBuffer<char>::resize(uint64_t newSize) {
    reserve(newSize);
    if (newSize > currentSize_) {
      memset(buf_ + currentSize_, 0, newSize - currentSize_);
    }
    currentSize_ = newSize;
  }

  // Specializations for char*

  template <>
  DataBuffer<char*>::~DataBuffer() {
    if (buf_) {
      memoryPool_.free(reinterpret_cast<char*>(buf_));
    }
  }

  template <>
  void DataBuffer<char*>::resize(uint64_t newSize) {
    reserve(newSize);
    if (newSize > currentSize_) {
      memset(buf_ + currentSize_, 0, (newSize - currentSize_) * sizeof(char*));
    }
    currentSize_ = newSize;
  }

  // Specializations for double

  template <>
  DataBuffer<double>::~DataBuffer() {
    if (buf_) {
      memoryPool_.free(reinterpret_cast<char*>(buf_));
    }
  }

  template <>
  void DataBuffer<double>::resize(uint64_t newSize) {
    reserve(newSize);
    if (newSize > currentSize_) {
      memset(buf_ + currentSize_, 0, (newSize - currentSize_) * sizeof(double));
    }
    currentSize_ = newSize;
  }

  // Specializations for float

  template <>
  DataBuffer<float>::~DataBuffer() {
    if (buf_) {
      memoryPool_.free(reinterpret_cast<char*>(buf_));
    }
  }

  template <>
  void DataBuffer<float>::resize(uint64_t newSize) {
    reserve(newSize);
    if (newSize > currentSize_) {
      memset(buf_ + currentSize_, 0, (newSize - currentSize_) * sizeof(float));
    }
    currentSize_ = newSize;
  }

  // Specializations for int64_t

  template <>
  DataBuffer<int64_t>::~DataBuffer() {
    if (buf_) {
      memoryPool_.free(reinterpret_cast<char*>(buf_));
    }
  }

  template <>
  void DataBuffer<int64_t>::resize(uint64_t newSize) {
    reserve(newSize);
    if (newSize > currentSize_) {
      memset(buf_ + currentSize_, 0, (newSize - currentSize_) * sizeof(int64_t));
    }
    currentSize_ = newSize;
  }

  // Specializations for int32_t

  template <>
  DataBuffer<int32_t>::~DataBuffer() {
    if (buf_) {
      memoryPool_.free(reinterpret_cast<char*>(buf_));
    }
  }

  template <>
  void DataBuffer<int32_t>::resize(uint64_t newSize) {
    reserve(newSize);
    if (newSize > currentSize_) {
      memset(buf_ + currentSize_, 0, (newSize - currentSize_) * sizeof(int32_t));
    }
    currentSize_ = newSize;
  }

  // Specializations for int16_t

  template <>
  DataBuffer<int16_t>::~DataBuffer() {
    if (buf_) {
      memoryPool_.free(reinterpret_cast<char*>(buf_));
    }
  }

  template <>
  void DataBuffer<int16_t>::resize(uint64_t newSize) {
    reserve(newSize);
    if (newSize > currentSize_) {
      memset(buf_ + currentSize_, 0, (newSize - currentSize_) * sizeof(int16_t));
    }
    currentSize_ = newSize;
  }

  // Specializations for int8_t

  template <>
  DataBuffer<int8_t>::~DataBuffer() {
    if (buf_) {
      memoryPool_.free(reinterpret_cast<char*>(buf_));
    }
  }

  template <>
  void DataBuffer<int8_t>::resize(uint64_t newSize) {
    reserve(newSize);
    if (newSize > currentSize_) {
      memset(buf_ + currentSize_, 0, (newSize - currentSize_) * sizeof(int8_t));
    }
    currentSize_ = newSize;
  }

  // Specializations for uint64_t

  template <>
  DataBuffer<uint64_t>::~DataBuffer() {
    if (buf_) {
      memoryPool_.free(reinterpret_cast<char*>(buf_));
    }
  }

  template <>
  void DataBuffer<uint64_t>::resize(uint64_t newSize) {
    reserve(newSize);
    if (newSize > currentSize_) {
      memset(buf_ + currentSize_, 0, (newSize - currentSize_) * sizeof(uint64_t));
    }
    currentSize_ = newSize;
  }

  // Specializations for unsigned char

  template <>
  DataBuffer<unsigned char>::~DataBuffer() {
    if (buf_) {
      memoryPool_.free(reinterpret_cast<char*>(buf_));
    }
  }

  template <>
  void DataBuffer<unsigned char>::resize(uint64_t newSize) {
    reserve(newSize);
    if (newSize > currentSize_) {
      memset(buf_ + currentSize_, 0, newSize - currentSize_);
    }
    currentSize_ = newSize;
  }

#ifdef __clang__
#pragma clang diagnostic ignored "-Wweak-template-vtables"
#endif

  template class DataBuffer<char>;
  template class DataBuffer<char*>;
  template class DataBuffer<double>;
  template class DataBuffer<float>;
  template class DataBuffer<Int128>;
  template class DataBuffer<int64_t>;
  template class DataBuffer<int32_t>;
  template class DataBuffer<int16_t>;
  template class DataBuffer<int8_t>;
  template class DataBuffer<uint64_t>;
  template class DataBuffer<unsigned char>;

#ifdef __clang__
#pragma clang diagnostic ignored "-Wexit-time-destructors"
#endif

  MemoryPool* getDefaultPool() {
    static MemoryPoolImpl internal;
    return &internal;
  }
}  // namespace orc
