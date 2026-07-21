#include <util/generic/buffer.h>
#include <util/generic/string.h>
#include <util/stream/buffer.h>
#include <util/system/yassert.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>

namespace {

class TInput {
public:
  TInput(const ui8 *data, size_t size)
      : Data_(data), Size_(size)
  {}

  bool Empty() const { return Pos_ >= Size_; }

  ui8 Byte() { return Empty() ? 0 : Data_[Pos_++]; }

  size_t Size(size_t modulo) {
    if (modulo == 0) {
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

void CheckBuffer(const TBuffer &buffer, const std::string &model) {
  Y_ABORT_UNLESS(buffer.Size() == model.size());
  Y_ABORT_UNLESS(buffer.size() == model.size());
  Y_ABORT_UNLESS(buffer.Empty() == model.empty());
  Y_ABORT_UNLESS(static_cast<bool>(buffer) == !model.empty());
  Y_ABORT_UNLESS(buffer.Capacity() >= buffer.Size());
  Y_ABORT_UNLESS(buffer.Avail() == buffer.Capacity() - buffer.Size());
  Y_ABORT_UNLESS(buffer.End() - buffer.Begin() ==
                 static_cast<ptrdiff_t>(model.size()));
  if (!model.empty()) {
    Y_ABORT_UNLESS(std::memcmp(buffer.Data(), model.data(), model.size()) == 0);
    Y_ABORT_UNLESS(std::equal(buffer.Begin(), buffer.End(), model.begin()));
  }

  TBuffer copy(buffer);
  Y_ABORT_UNLESS(copy == buffer);
  if (!model.empty()) {
    Y_ABORT_UNLESS(std::memcmp(copy.Data(), model.data(), model.size()) == 0);
  }
}

void Run(const ui8 *data, size_t size) {
  TInput input(data, size);
  TBuffer buffer;
  std::string model;

  constexpr size_t MaxModel = 4096;
  for (size_t step = 0; step < 256 && !input.Empty(); ++step) {
    switch (input.Byte() % 14) {
    case 0: {
      const std::string chunk = input.Bytes(128);
      const size_t len =
          std::min(chunk.size(), MaxModel - std::min(model.size(), MaxModel));
      buffer.Append(chunk.data(), len);
      model.append(chunk.data(), len);
      break;
    }
    case 1: {
      if (model.size() < MaxModel) {
        const char ch = static_cast<char>(input.Byte());
        buffer.Append(ch);
        model.push_back(ch);
      }
      break;
    }
    case 2: {
      const size_t len = std::min(input.Size(128),
                                  MaxModel - std::min(model.size(), MaxModel));
      const char ch = static_cast<char>(input.Byte());
      buffer.Fill(ch, len);
      model.append(len, ch);
      break;
    }
    case 3: {
      const size_t newSize = input.Size(MaxModel + 1);
      const size_t oldSize = model.size();
      const char fill = static_cast<char>(input.Byte());
      buffer.Resize(newSize);
      if (newSize > oldSize) {
        std::memset(buffer.Data() + oldSize, fill, newSize - oldSize);
      }
      model.resize(newSize, fill);
      break;
    }
    case 4: {
      const size_t newSize = input.Size(MaxModel + 1);
      const size_t oldSize = model.size();
      const char fill = static_cast<char>(input.Byte());
      buffer.ResizeExactNeverCallMeInSaneCode(newSize);
      if (newSize > oldSize) {
        std::memset(buffer.Data() + oldSize, fill, newSize - oldSize);
      }
      model.resize(newSize, fill);
      break;
    }
    case 5: {
      if (!model.empty()) {
        const size_t count = input.Size(model.size() + 1);
        buffer.EraseBack(count);
        model.resize(model.size() - count);
      }
      break;
    }
    case 6: {
      if (!model.empty()) {
        const size_t pos = input.Size(model.size() + 1);
        const size_t count = input.Size(model.size() - pos + 1);
        buffer.Chop(pos, count);
        model.erase(pos, count);
      }
      break;
    }
    case 7: {
      if (!model.empty()) {
        const size_t count = input.Size(model.size() + 1);
        buffer.ChopHead(count);
        model.erase(0, count);
      }
      break;
    }
    case 8:
      buffer.Clear();
      model.clear();
      break;
    case 9:
      buffer.Reset();
      model.clear();
      break;
    case 10:
      buffer.Reserve(input.Size(MaxModel * 2 + 1));
      break;
    case 11:
      buffer.ShrinkToFit();
      break;
    case 12: {
      const size_t align = size_t{1} << input.Size(5);
      const char fill = static_cast<char>(input.Byte());
      const size_t target = (model.size() + align - 1) / align * align;
      if (target <= MaxModel) {
        buffer.AlignUp(align, fill);
        model.append(target - model.size(), fill);
      }
      break;
    }
    case 13: {
      TBuffer roundTrip;
      {
        TBufferOutput out(roundTrip);
        out.Write(model.data(), model.size());
      }
      Y_ABORT_UNLESS(roundTrip.Size() == model.size());
      if (!model.empty()) {
        Y_ABORT_UNLESS(
            std::memcmp(roundTrip.Data(), model.data(), model.size()) == 0);
      }

      TBuffer moved(std::move(roundTrip));
      Y_ABORT_UNLESS(moved.Size() == model.size());
      TBuffer swapped;
      swapped.Swap(moved);
      Y_ABORT_UNLESS(swapped.Size() == model.size());
      break;
    }
    }

    CheckBuffer(buffer, model);
  }

  TString asString;
  TBuffer copy(buffer);
  copy.AsString(asString);
  Y_ABORT_UNLESS(asString.size() == model.size());
  if (!model.empty()) {
    Y_ABORT_UNLESS(std::memcmp(asString.data(), model.data(), model.size()) ==
                   0);
  }
  Y_ABORT_UNLESS(copy.Empty());
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8 *data, size_t size) {
  Run(data, size);
  return 0;
}
