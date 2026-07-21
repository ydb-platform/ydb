#include <library/cpp/containers/paged_vector/paged_vector.h>

#include <util/system/yassert.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <numeric>
#include <vector>

namespace {

class TInput {
public:
  TInput(const ui8 *data, size_t size)
      : Data_(data), Size_(size)
  {}

  bool Empty() const { return Pos_ >= Size_; }

  ui8 Byte() { return Empty() ? 0 : Data_[Pos_++]; }

  size_t Size(size_t modulo) {
    if (!modulo) {
      return 0;
    }
    size_t value = 0;
    for (int i = 0; i < 4; ++i) {
      value = (value << 8) | Byte();
    }
    return value % modulo;
  }

  std::vector<ui32> Values(size_t maxLen) {
    const size_t len = Size(maxLen + 1);
    std::vector<ui32> result;
    result.reserve(len);
    for (size_t i = 0; i < len; ++i) {
      result.push_back(Size(1000003));
    }
    return result;
  }

private:
  const ui8 *Data_;
  size_t Size_;
  size_t Pos_ = 0;
};

using TVec = NPagedVector::TPagedVector<ui32, 16>;

void CheckVector(const TVec &vec, const std::vector<ui32> &model) {
  Y_ABORT_UNLESS(vec.size() == model.size());
  Y_ABORT_UNLESS(vec.empty() == model.empty());
  Y_ABORT_UNLESS(static_cast<bool>(vec) == !model.empty());
  Y_ABORT_UNLESS(vec.end() - vec.begin() ==
                 static_cast<ptrdiff_t>(model.size()));
  Y_ABORT_UNLESS(
      std::equal(vec.begin(), vec.end(), model.begin(), model.end()));

  std::vector<ui32> iterated(vec.begin(), vec.end());
  Y_ABORT_UNLESS(iterated == model);
  std::vector<ui32> reversed(vec.rbegin(), vec.rend());
  Y_ABORT_UNLESS(std::equal(reversed.begin(), reversed.end(), model.rbegin(),
                            model.rend()));

  for (size_t i = 0; i < model.size(); ++i) {
    Y_ABORT_UNLESS(vec[i] == model[i]);
    Y_ABORT_UNLESS(vec.at(i) == model[i]);
    Y_ABORT_UNLESS((vec.begin() + i) - vec.begin() ==
                   static_cast<ptrdiff_t>(i));
    Y_ABORT_UNLESS(*(vec.begin() + i) == model[i]);
  }
  if (!model.empty()) {
    Y_ABORT_UNLESS(vec.front() == model.front());
    Y_ABORT_UNLESS(vec.back() == model.back());
  }

  TVec copy(vec.begin(), vec.end());
  Y_ABORT_UNLESS(copy == vec);
}

void Run(const ui8 *data, size_t size) {
  TInput input(data, size);
  TVec vec;
  std::vector<ui32> model;
  constexpr size_t MaxSize = 512;

  for (size_t step = 0; step < 256 && !input.Empty(); ++step) {
    switch (input.Byte() % 9) {
    case 0:
      if (model.size() < MaxSize) {
        const ui32 value = input.Size(1000003);
        vec.push_back(value);
        model.push_back(value);
      }
      break;
    case 1:
      if (model.size() < MaxSize) {
        const ui32 value = input.Size(1000003);
        vec.emplace_back(value);
        model.emplace_back(value);
      }
      break;
    case 2:
      if (!model.empty()) {
        vec.pop_back();
        model.pop_back();
      }
      break;
    case 3: {
      const size_t newSize = input.Size(MaxSize + 1);
      vec.resize(newSize);
      model.resize(newSize);
      break;
    }
    case 4: {
      std::vector<ui32> chunk =
          input.Values(std::min<size_t>(64, MaxSize - model.size()));
      vec.append(chunk.begin(), chunk.end());
      model.insert(model.end(), chunk.begin(), chunk.end());
      break;
    }
    case 5: {
      TVec copy(vec);
      TVec assigned;
      assigned = copy;
      CheckVector(assigned, model);

      TVec moved(std::move(copy));
      CheckVector(moved, model);
      assigned = std::move(moved);
      CheckVector(assigned, model);
      break;
    }
    case 6:
      if (!model.empty()) {
        const size_t pos = input.Size(model.size());
        vec.erase(vec.begin() + pos);
        model.erase(model.begin() + pos);
      }
      break;
    case 7:
      if (!model.empty()) {
        const size_t begin = input.Size(model.size());
        const size_t end = begin + input.Size(model.size() - begin + 1);
        vec.erase(vec.begin() + begin, vec.begin() + end);
        model.erase(model.begin() + begin, model.begin() + end);
      }
      break;
    case 8: {
      TVec other(vec.begin(), vec.end());
      TVec empty;
      other.swap(empty);
      Y_ABORT_UNLESS(empty.size() == model.size());
      Y_ABORT_UNLESS(other.empty());
      break;
    }
    }

    CheckVector(vec, model);
  }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8 *data, size_t size) {
  Run(data, size);
  return 0;
}
