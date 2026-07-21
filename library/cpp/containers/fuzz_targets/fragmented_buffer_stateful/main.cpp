#include <ydb/core/util/fragmented_buffer.h>

#include <util/system/yassert.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
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

  std::string Bytes(size_t maxLen) {
    const size_t len = Size(maxLen + 1);
    std::string result(len, '\0');
    for (char &c : result) {
      c = static_cast<char>(Byte());
    }
    return result;
  }

private:
  const ui8 *Data_;
  size_t Size_;
  size_t Pos_ = 0;
};

TRope MakeRope(const std::string &data, TInput &input) {
  TRope rope;
  size_t offset = 0;
  while (offset < data.size()) {
    const size_t len = 1 + input.Size(data.size() - offset);
    rope.Insert(rope.End(), TRcBuf::Copy(data.data() + offset, len));
    offset += len;
  }
  return rope;
}

std::vector<std::pair<size_t, size_t>>
Intervals(const std::vector<bool> &written) {
  std::vector<std::pair<size_t, size_t>> result;
  for (size_t i = 0; i < written.size();) {
    if (!written[i]) {
      ++i;
      continue;
    }
    const size_t begin = i;
    while (i < written.size() && written[i]) {
      ++i;
    }
    result.emplace_back(begin, i);
  }
  return result;
}

void CheckIntervals(const NKikimr::TFragmentedBuffer &buffer,
                    const std::vector<bool> &written) {
  const auto expected = Intervals(written);
  const auto actual = buffer.GetIntervalSet();
  Y_ABORT_UNLESS(actual.Size() == expected.size());
  auto it = actual.begin();
  for (const auto &[begin, end] : expected) {
    Y_ABORT_UNLESS(it != actual.end());
    const auto actualInterval = *it;
    Y_ABORT_UNLESS(static_cast<size_t>(actualInterval.first) == begin);
    Y_ABORT_UNLESS(static_cast<size_t>(actualInterval.second) == end);
    ++it;
  }
  Y_ABORT_UNLESS(it == actual.end());
  Y_ABORT_UNLESS(static_cast<bool>(buffer) == !expected.empty());
  Y_ABORT_UNLESS(
      buffer.GetTotalSize() ==
      static_cast<size_t>(std::count(written.begin(), written.end(), true)));
  Y_ABORT_UNLESS(buffer.IsMonolith() ==
                 (expected.size() == 1 && expected.front().first == 0));
}

void Run(const ui8 *data, size_t size) {
  TInput input(data, size);
  NKikimr::TFragmentedBuffer buffer;
  constexpr size_t MaxSize = 512;
  std::string model(MaxSize, '\0');
  std::vector<bool> written(MaxSize, false);

  for (size_t step = 0; step < 256 && !input.Empty(); ++step) {
    switch (input.Byte() % 6) {
    case 0:
    case 1: {
      const size_t offset = input.Size(MaxSize);
      const size_t len = 1 + input.Size(std::min<size_t>(64, MaxSize - offset));
      std::string chunk = input.Bytes(len);
      chunk.resize(len, '\0');
      std::copy(chunk.begin(), chunk.end(), model.begin() + offset);
      std::fill(written.begin() + offset, written.begin() + offset + len, true);
      if (input.Byte() & 1) {
        buffer.Write(offset, chunk.data(), chunk.size());
      } else {
        buffer.Write(offset, MakeRope(chunk, input));
      }
      break;
    }
    case 2: {
      const auto intervals = Intervals(written);
      if (!intervals.empty()) {
        const auto [ibegin, iend] = intervals[input.Size(intervals.size())];
        const size_t begin = ibegin + input.Size(iend - ibegin);
        const size_t len = 1 + input.Size(iend - begin);
        std::string out(len, '\0');
        buffer.Read(begin, out.data(), out.size());
        Y_ABORT_UNLESS(out == model.substr(begin, len));
        Y_ABORT_UNLESS(buffer.Read(begin, len).ConvertToString() ==
                       model.substr(begin, len));
      }
      break;
    }
    case 3: {
      const auto intervals = Intervals(written);
      if (intervals.size() == 1 && intervals.front().first == 0) {
        TRope monolith = buffer.GetMonolith();
        Y_ABORT_UNLESS(monolith.ConvertToString() ==
                       model.substr(0, intervals.front().second));
      }
      break;
    }
    case 4: {
      std::string chunk = input.Bytes(128);
      if (!chunk.empty()) {
        buffer.SetMonolith(MakeRope(chunk, input));
        model.assign(MaxSize, '\0');
        written.assign(MaxSize, false);
        if (chunk.size() > MaxSize) {
          chunk.resize(MaxSize);
        }
        std::copy(chunk.begin(), chunk.end(), model.begin());
        std::fill(written.begin(), written.begin() + chunk.size(), true);
      }
      break;
    }
    case 5: {
      const auto intervals = Intervals(written);
      if (!intervals.empty()) {
        NKikimr::TFragmentedBuffer copy;
        NKikimr::TIntervalSet<i32> range;
        for (const auto &[begin, end] : intervals) {
          range.Add(begin, end);
        }
        copy.CopyFrom(buffer, range);
        CheckIntervals(copy, written);
        for (const auto &[begin, end] : intervals) {
          Y_ABORT_UNLESS(copy.Read(begin, end - begin).ConvertToString() ==
                         model.substr(begin, end - begin));
        }
      }
      break;
    }
    }

    CheckIntervals(buffer, written);
  }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8 *data, size_t size) {
  Run(data, size);
  return 0;
}
