#include <library/cpp/containers/ring_buffer/ring_buffer.h>

#include <util/system/yassert.h>

#include <cstddef>
#include <cstdint>
#include <deque>

namespace {

class TInput {
public:
  TInput(const ui8 *data, size_t size)
      : Data_(data), Size_(size)
  {}

  bool Empty() const { return Pos_ >= Size_; }

  ui8 Byte() { return Empty() ? 0 : Data_[Pos_++]; }

  ui32 Int() {
    ui32 value = 0;
    for (int i = 0; i < 4; ++i) {
      value = (value << 8) | Byte();
    }
    return value;
  }

private:
  const ui8 *Data_;
  size_t Size_;
  size_t Pos_ = 0;
};

void CheckRing(const TSimpleRingBuffer<ui32> &ring,
               const std::deque<ui32> &model, size_t firstIndex,
               size_t totalInserted) {
  Y_ABORT_UNLESS(ring.FirstIndex() == firstIndex);
  Y_ABORT_UNLESS(ring.AvailSize() == model.size());
  Y_ABORT_UNLESS(ring.TotalSize() == totalInserted);

  for (size_t i = 0; i < model.size(); ++i) {
    const size_t index = firstIndex + i;
    Y_ABORT_UNLESS(ring.IsAvail(index));
    Y_ABORT_UNLESS(ring[index] == model[i]);
  }

  if (firstIndex > 0) {
    Y_ABORT_UNLESS(!ring.IsAvail(firstIndex - 1));
  }
  Y_ABORT_UNLESS(!ring.IsAvail(totalInserted));
}

void Run(const ui8 *data, size_t size) {
  TInput input(data, size);
  const size_t capacity = 1 + input.Byte() % 64;
  TSimpleRingBuffer<ui32> ring(capacity);
  std::deque<ui32> model;
  size_t firstIndex = 0;
  size_t totalInserted = 0;

  for (size_t step = 0; step < 512 && !input.Empty(); ++step) {
    switch (input.Byte() % 5) {
    case 0: {
      const ui32 value = input.Int();
      ring.PushBack(value);
      if (model.size() == capacity) {
        model.pop_front();
        ++firstIndex;
      }
      model.push_back(value);
      ++totalInserted;
      break;
    }
    case 1:
      ring.Clear();
      model.clear();
      firstIndex = 0;
      totalInserted = 0;
      break;
    case 2: {
      TSimpleRingBuffer<ui32> copy(ring);
      CheckRing(copy, model, firstIndex, totalInserted);
      break;
    }
    case 3: {
      TSimpleRingBuffer<ui32> copy(ring);
      TSimpleRingBuffer<ui32> moved(std::move(copy));
      CheckRing(moved, model, firstIndex, totalInserted);
      break;
    }
    case 4:
      if (!model.empty()) {
        const size_t offset = input.Byte() % model.size();
        const size_t index = firstIndex + offset;
        ring[index] ^= input.Int();
        model[offset] = ring[index];
      }
      break;
    }

    CheckRing(ring, model, firstIndex, totalInserted);
  }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8 *data, size_t size) {
  Run(data, size);
  return 0;
}
