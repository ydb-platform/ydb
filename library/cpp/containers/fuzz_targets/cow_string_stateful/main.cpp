#include <library/cpp/containers/cow_string/cow_string.h>
#include <library/cpp/containers/cow_string/ysaveload.h>

#include <util/generic/buffer.h>
#include <util/stream/buffer.h>
#include <util/system/yassert.h>
#include <util/ysaveload.h>

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

void CheckString(const TCowString &value, const std::string &model) {
  Y_ABORT_UNLESS(value.size() == model.size());
  Y_ABORT_UNLESS(value.length() == model.size());
  Y_ABORT_UNLESS(value.empty() == model.empty());
  Y_ABORT_UNLESS(value.end() - value.begin() ==
                 static_cast<ptrdiff_t>(model.size()));
  if (!model.empty()) {
    Y_ABORT_UNLESS(std::memcmp(value.data(), model.data(), model.size()) == 0);
    Y_ABORT_UNLESS(value.front() == model.front());
    Y_ABORT_UNLESS(value.back() == model.back());
  }
  for (size_t i = 0; i < model.size(); ++i) {
    Y_ABORT_UNLESS(value[i] == model[i]);
  }

  TCowString copy(value);
  Y_ABORT_UNLESS(copy == value);
  if (!model.empty()) {
    const char old = copy[0];
    copy[0] = static_cast<char>(old ^ 0x5a);
    Y_ABORT_UNLESS(value[0] == model[0]);
    Y_ABORT_UNLESS(copy[0] == static_cast<char>(old ^ 0x5a));
  }
}

TCowString SerializeThereAndBack(const TCowString &value) {
  TBuffer buffer;
  {
    TBufferOutput out(buffer);
    Save(&out, value);
  }
  TBufferInput in(buffer);
  TCowString result;
  Load(&in, result);
  return result;
}

void Run(const ui8 *data, size_t size) {
  TInput input(data, size);
  TCowString value;
  std::string model;
  constexpr size_t MaxSize = 4096;

  for (size_t step = 0; step < 256 && !input.Empty(); ++step) {
    switch (input.Byte() % 15) {
    case 0: {
      const std::string chunk = input.Bytes(128);
      const size_t len =
          std::min(chunk.size(), MaxSize - std::min(model.size(), MaxSize));
      value.append(chunk.data(), len);
      model.append(chunk.data(), len);
      break;
    }
    case 1:
      if (model.size() < MaxSize) {
        const char ch = static_cast<char>(input.Byte());
        value.push_back(ch);
        model.push_back(ch);
      }
      break;
    case 2: {
      const std::string chunk = input.Bytes(128);
      value.assign(chunk.data(), chunk.size());
      model.assign(chunk.data(), chunk.size());
      break;
    }
    case 3: {
      const size_t newSize = input.Size(MaxSize + 1);
      const char fill = static_cast<char>(input.Byte());
      value.resize(newSize, fill);
      model.resize(newSize, fill);
      break;
    }
    case 4:
      value.clear();
      model.clear();
      break;
    case 5:
      value.reserve(input.Size(MaxSize * 2 + 1));
      break;
    case 6:
      if (!model.empty()) {
        value.pop_back();
        model.pop_back();
      }
      break;
    case 7:
      if (!model.empty()) {
        const size_t pos = input.Size(model.size());
        const size_t count = input.Size(model.size() - pos + 1);
        value.erase(pos, count);
        model.erase(pos, count);
      }
      break;
    case 8:
      if (model.size() < MaxSize) {
        const size_t pos = input.Size(model.size() + 1);
        const std::string chunk =
            input.Bytes(std::min<size_t>(64, MaxSize - model.size()));
        value.insert(pos, chunk.data(), chunk.size());
        model.insert(pos, chunk.data(), chunk.size());
      }
      break;
    case 9:
      if (!model.empty()) {
        const size_t pos = input.Size(model.size());
        const size_t count = input.Size(model.size() - pos + 1);
        const std::string chunk =
            input.Bytes(std::min<size_t>(64, MaxSize - (model.size() - count)));
        value.replace(pos, count, chunk.data(), chunk.size());
        model.replace(pos, count, chunk.data(), chunk.size());
      }
      break;
    case 10: {
      const std::string chunk =
          input.Bytes(std::min<size_t>(64, MaxSize - model.size()));
      value.prepend(TStringBuf(chunk.data(), chunk.size()));
      model.insert(0, chunk.data(), chunk.size());
      break;
    }
    case 11:
      if (!model.empty()) {
        const size_t pos = input.Size(model.size());
        const char ch = static_cast<char>(input.Byte());
        value[pos] = ch;
        model[pos] = ch;
      }
      break;
    case 12: {
      TCowString copy = value;
      copy += value;
      std::string doubled = model + model;
      if (doubled.size() <= MaxSize * 2) {
        CheckString(copy, doubled);
      }
      break;
    }
    case 13: {
      TCowString loaded = SerializeThereAndBack(value);
      CheckString(loaded, model);
      break;
    }
    case 14: {
      TCowString joined =
          TCowString::Join(TStringBuf(model.data(), model.size()), 'x');
      std::string expected = model + 'x';
      CheckString(joined, expected);
      break;
    }
    }

    CheckString(value, model);
  }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8 *data, size_t size) {
  Run(data, size);
  return 0;
}
