#pragma clang system_header
// Copyright 2021 The TCMalloc Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef TCMALLOC_MOCK_STATIC_FORWARDER_H_
#define TCMALLOC_MOCK_STATIC_FORWARDER_H_

#include <cstddef>
#include <cstdint>
#include <map>
#include <new>

#include "gmock/gmock.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "absl/types/span.h"
#include "tcmalloc/pages.h"
#include "tcmalloc/span.h"

namespace tcmalloc {
namespace tcmalloc_internal {

class FakeStaticForwarder {
 public:
  FakeStaticForwarder() : class_size_(0), pages_() {}
  void Init(size_t class_size, size_t pages, size_t num_objects_to_move,
            bool use_large_spans) {
    class_size_ = class_size;
    pages_ = Length(pages);
    num_objects_to_move_ = num_objects_to_move;
    use_large_spans_ = use_large_spans;
    TC_ASSERT_LE(max_span_cache_size(), max_span_cache_array_size());
    clock_ = 1234;
  }
  uint64_t clock_now() const { return clock_; }
  double clock_frequency() const {
    return absl::ToDoubleNanoseconds(absl::Seconds(2));
  }
  void AdvanceClock(absl::Duration d) {
    clock_ += absl::ToDoubleSeconds(d) * clock_frequency();
  }

  size_t class_to_size(int size_class) const { return class_size_; }
  Length class_to_pages(int size_class) const { return pages_; }
  size_t num_objects_to_move() const { return num_objects_to_move_; }
  uint32_t max_span_cache_size() const {
    return use_large_spans_ ? Span::kLargeCacheSize : Span::kCacheSize;
  }
  uint32_t max_span_cache_array_size() const {
    return use_large_spans_ ? Span::kLargeCacheArraySize : Span::kCacheSize;
  }

  void MapObjectsToSpans(absl::Span<void*> batch, Span** spans,
                         int expected_size_class) {
    for (size_t i = 0; i < batch.size(); ++i) {
      spans[i] = MapObjectToSpan(batch[i]);
    }
  }

  [[nodiscard]] Span* MapObjectToSpan(const void* object) {
    const PageId page = PageIdContaining(object);

    absl::MutexLock l(&mu_);
    auto it = map_.lower_bound(page);
    if (it->first != page && it != map_.begin()) {
      --it;
    }

    if (it->first <= page && page <= it->second.span->last_page()) {
      return it->second.span;
    }

    return nullptr;
  }

  [[nodiscard]] Span* AllocateSpan(int, size_t objects_per_span,
                                   Length pages_per_span) {
    void* backing =
        ::operator new(pages_per_span.in_bytes(), std::align_val_t(kPageSize));
    PageId page = PageIdContaining(backing);

    void* span_buf =
        ::operator new(Span::CalcSizeOf(max_span_cache_array_size()),
                       Span::CalcAlignOf(max_span_cache_array_size()));
    TC_ASSERT_LE(max_span_cache_size(), max_span_cache_array_size());

    auto* span = new (span_buf) Span(Range(page, pages_per_span));

    absl::MutexLock l(&mu_);
    SpanInfo info;
    info.span = span;
    SpanAllocInfo span_alloc_info = {
        .objects_per_span = objects_per_span,
        .density = AccessDensityPrediction::kSparse};
    info.span_alloc_info = span_alloc_info;
    map_.emplace(page, info);
    return span;
  }

  void DeallocateSpans(size_t, absl::Span<Span*> free_spans) {
    {
      absl::MutexLock l(&mu_);
      for (Span* span : free_spans) {
        auto it = map_.find(span->first_page());
        EXPECT_NE(it, map_.end());
        map_.erase(it);
      }
    }

    const std::align_val_t span_alignment =
        Span::CalcAlignOf(max_span_cache_array_size());

    for (Span* span : free_spans) {
      ::operator delete(span->start_address(), std::align_val_t(kPageSize));

      span->~Span();
      ::operator delete(span, span_alignment);
    }
  }

 private:
  struct SpanInfo {
    Span* span;
    SpanAllocInfo span_alloc_info;
  };

  absl::Mutex mu_;
  std::map<PageId, SpanInfo> map_ ABSL_GUARDED_BY(mu_);
  size_t class_size_;
  Length pages_;
  size_t num_objects_to_move_;
  bool use_large_spans_;
  uint64_t clock_;
};

class RawMockStaticForwarder : public FakeStaticForwarder {
 public:
  RawMockStaticForwarder() {
    ON_CALL(*this, class_to_size).WillByDefault([this](int size_class) {
      return FakeStaticForwarder::class_to_size(size_class);
    });
    ON_CALL(*this, class_to_pages).WillByDefault([this](int size_class) {
      return FakeStaticForwarder::class_to_pages(size_class);
    });
    ON_CALL(*this, num_objects_to_move).WillByDefault([this]() {
      return FakeStaticForwarder::num_objects_to_move();
    });
    ON_CALL(*this, Init)
        .WillByDefault([this](size_t size_class, size_t pages,
                              size_t num_objects_to_move,
                              bool use_large_spans) {
          FakeStaticForwarder::Init(size_class, pages, num_objects_to_move,
                                    use_large_spans);
        });

    ON_CALL(*this, MapObjectsToSpans)
        .WillByDefault([this](absl::Span<void*> batch, Span** spans,
                              int expected_size_class) {
          return FakeStaticForwarder::MapObjectsToSpans(batch, spans,
                                                        expected_size_class);
        });
    ON_CALL(*this, AllocateSpan)
        .WillByDefault([this](int size_class, size_t objects_per_span,
                              Length pages_per_span) {
          return FakeStaticForwarder::AllocateSpan(size_class, objects_per_span,
                                                   pages_per_span);
        });
    ON_CALL(*this, DeallocateSpans)
        .WillByDefault([this](size_t objects_per_span,
                              absl::Span<Span*> free_spans) {
          FakeStaticForwarder::DeallocateSpans(objects_per_span, free_spans);
        });
  }

  MOCK_METHOD(size_t, class_to_size, (int size_class));
  MOCK_METHOD(Length, class_to_pages, (int size_class));
  MOCK_METHOD(size_t, num_objects_to_move, ());
  MOCK_METHOD(void, Init,
              (size_t class_size, size_t pages, size_t num_objects_to_move,
               bool use_large_spans));
  MOCK_METHOD(void, MapObjectsToSpans,
              (absl::Span<void*> batch, Span** spans, int expected_size_class));
  MOCK_METHOD(Span*, AllocateSpan,
              (int size_class, size_t objects_per_span, Length pages_per_span));
  MOCK_METHOD(void, DeallocateSpans,
              (size_t object_per_span, absl::Span<Span*> free_spans));
};

using MockStaticForwarder = testing::NiceMock<RawMockStaticForwarder>;

// Wires up a largely functional CentralFreeList + MockStaticForwarder.
//
// By default, it fills allocations and responds sensibly.  Because it backs
// onto malloc/free, it will detect leaks and memory misuse when run under
// sanitizers.
//
// Exposes the underlying mocks to allow for more whitebox tests.
template <typename CentralFreeListT>
class FakeCentralFreeListEnvironment {
 public:
  using CentralFreeList = CentralFreeListT;
  using Forwarder = typename CentralFreeListT::Forwarder;

  static constexpr int kSizeClass = 1;
  size_t objects_per_span() {
    return forwarder().class_to_pages(kSizeClass).in_bytes() /
           forwarder().class_to_size(kSizeClass);
  }
  size_t batch_size() { return forwarder().num_objects_to_move(); }

  explicit FakeCentralFreeListEnvironment(size_t class_size, size_t pages,
                                          size_t num_objects_to_move,
                                          bool use_large_spans) {
    forwarder().Init(class_size, pages, num_objects_to_move, use_large_spans);
    cache_.Init(kSizeClass);
  }

  ~FakeCentralFreeListEnvironment() { EXPECT_EQ(cache_.length(), 0); }

  CentralFreeList& central_freelist() { return cache_; }

  Forwarder& forwarder() { return cache_.forwarder(); }

 private:
  CentralFreeList cache_;
};

}  // namespace tcmalloc_internal
}  // namespace tcmalloc

#endif  // TCMALLOC_MOCK_STATIC_FORWARDER_H_
