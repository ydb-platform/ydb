#include <yql/essentials/utils/chunked_buffer.h>

#include <util/stream/str.h>
#include <util/system/yassert.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
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

  TString Bytes(size_t maxLen) {
    const size_t len = Size(maxLen + 1);
    TString result(len, '\0');
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

std::string ToString(const NYql::TChunkedBuffer &buffer,
                     size_t limit = std::numeric_limits<size_t>::max()) {
  TStringStream out;
  buffer.CopyTo(out, limit);
  const TString result = out.Str();
  return std::string(result.data(), result.size());
}

void CheckBuffer(const NYql::TChunkedBuffer &buffer, const std::string &model) {
  Y_ABORT_UNLESS(buffer.Size() == model.size());
  Y_ABORT_UNLESS(buffer.Empty() == model.empty());
  Y_ABORT_UNLESS(ToString(buffer) == model);
  for (size_t limit = 0; limit <= std::min<size_t>(model.size(), 8); ++limit) {
    Y_ABORT_UNLESS(ToString(buffer, limit) == model.substr(0, limit));
  }
  if (!model.empty()) {
    Y_ABORT_UNLESS(buffer.ContigousSize() == buffer.Front().Buf.size());
    Y_ABORT_UNLESS(buffer.ContigousSize() > 0);
    Y_ABORT_UNLESS(model.compare(0, buffer.Front().Buf.size(),
                                 buffer.Front().Buf.data(),
                                 buffer.Front().Buf.size()) == 0);
  } else {
    Y_ABORT_UNLESS(buffer.ContigousSize() == 0);
  }

  NYql::TChunkedBuffer copied = NYql::CopyData(buffer);
  Y_ABORT_UNLESS(ToString(copied) == model);
}

void Run(const ui8 *data, size_t size) {
  TInput input(data, size);
  NYql::TChunkedBuffer buffer;
  std::string model;
  constexpr size_t MaxSize = 4096;

  for (size_t step = 0; step < 256 && !input.Empty(); ++step) {
    switch (input.Byte() % 8) {
    case 0: {
      TString chunk = input.Bytes(
          std::min<size_t>(128, MaxSize - std::min(model.size(), MaxSize)));
      model.append(chunk.data(), chunk.size());
      buffer.Append(std::move(chunk));
      break;
    }
    case 1: {
      TString chunk = input.Bytes(
          std::min<size_t>(128, MaxSize - std::min(model.size(), MaxSize)));
      auto owner = std::make_shared<TString>(std::move(chunk));
      TStringBuf view(*owner);
      model.append(view.data(), view.size());
      buffer.Append(view, owner);
      break;
    }
    case 2: {
      NYql::TChunkedBuffer other;
      std::string otherModel;
      const size_t chunks = input.Size(4);
      for (size_t i = 0;
           i < chunks && model.size() + otherModel.size() < MaxSize; ++i) {
        TString chunk = input.Bytes(
            std::min<size_t>(64, MaxSize - model.size() - otherModel.size()));
        otherModel.append(chunk.data(), chunk.size());
        other.Append(std::move(chunk));
      }
      buffer.Append(std::move(other));
      model += otherModel;
      Y_ABORT_UNLESS(other.Empty());
      break;
    }
    case 3: {
      const size_t erase = input.Size(model.size() + 1);
      buffer.Erase(erase);
      model.erase(0, erase);
      break;
    }
    case 4:
      buffer.Clear();
      model.clear();
      break;
    case 5: {
      NYql::TChunkedBuffer copy = NYql::CopyData(buffer);
      NYql::TChunkedBuffer moved = NYql::CopyData(std::move(copy));
      Y_ABORT_UNLESS(copy.Empty());
      Y_ABORT_UNLESS(ToString(moved) == model);
      break;
    }
    case 6: {
      NYql::TChunkedBuffer constructed(TString(model.data(), model.size()));
      Y_ABORT_UNLESS(ToString(constructed) == model);
      break;
    }
    case 7: {
      NYql::TChunkedBuffer outputBuffer;
      {
        NYql::TChunkedBufferOutput out(outputBuffer);
        out.Write(model.data(), model.size());
      }
      Y_ABORT_UNLESS(ToString(outputBuffer) == model);
      break;
    }
    }

    CheckBuffer(buffer, model);
  }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8 *data, size_t size) {
  Run(data, size);
  return 0;
}
