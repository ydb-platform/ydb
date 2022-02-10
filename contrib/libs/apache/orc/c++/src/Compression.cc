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

#include "Adaptor.hh"
#include "Compression.hh"
#include "orc/Exceptions.hh"
#include "LzoDecompressor.hh"
#include "lz4.h"

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <sstream>

#include "zlib.h"
#include "zstd.h"

#include "wrap/snappy-wrapper.h"

#ifndef ZSTD_CLEVEL_DEFAULT
#define ZSTD_CLEVEL_DEFAULT 3
#endif

namespace orc {

  class CompressionStreamBase: public BufferedOutputStream {
  public:
    CompressionStreamBase(OutputStream * outStream,
                          int compressionLevel,
                          uint64_t capacity,
                          uint64_t blockSize,
                          MemoryPool& pool);

    virtual bool Next(void** data, int*size) override = 0;
    virtual void BackUp(int count) override;

    virtual std::string getName() const override = 0;
    virtual uint64_t flush() override;

    virtual bool isCompressed() const override { return true; }
    virtual uint64_t getSize() const override;

  protected:
    void writeHeader(char * buffer, size_t compressedSize, bool original) {
      buffer[0] = static_cast<char>((compressedSize << 1) + (original ? 1 : 0));
      buffer[1] = static_cast<char>(compressedSize >> 7);
      buffer[2] = static_cast<char>(compressedSize >> 15);
    }

    // ensure enough room for compression block header
    void ensureHeader();

    // Buffer to hold uncompressed data until user calls Next()
    DataBuffer<unsigned char> rawInputBuffer;

    // Compress level
    int level;

    // Compressed data output buffer
    char * outputBuffer;

    // Size for compressionBuffer
    int bufferSize;

    // Compress output position
    int outputPosition;

    // Compress output buffer size
    int outputSize;
  };

  CompressionStreamBase::CompressionStreamBase(OutputStream * outStream,
                                               int compressionLevel,
                                               uint64_t capacity,
                                               uint64_t blockSize,
                                               MemoryPool& pool) :
                                                BufferedOutputStream(pool,
                                                                     outStream,
                                                                     capacity,
                                                                     blockSize),
                                                rawInputBuffer(pool, blockSize),
                                                level(compressionLevel),
                                                outputBuffer(nullptr),
                                                bufferSize(0),
                                                outputPosition(0),
                                                outputSize(0) {
    // PASS
  }

  void CompressionStreamBase::BackUp(int count) {
    if (count > bufferSize) {
      throw std::logic_error("Can't backup that much!");
    }
    bufferSize -= count;
  }

  uint64_t CompressionStreamBase::flush() {
    void * data;
    int size;
    if (!Next(&data, &size)) {
      throw std::runtime_error("Failed to flush compression buffer.");
    }
    BufferedOutputStream::BackUp(outputSize - outputPosition);
    bufferSize = outputSize = outputPosition = 0;
    return BufferedOutputStream::flush();
  }

  uint64_t CompressionStreamBase::getSize() const {
    return BufferedOutputStream::getSize() -
           static_cast<uint64_t>(outputSize - outputPosition);
  }

  void CompressionStreamBase::ensureHeader() {
    // adjust 3 bytes for the compression header
    if (outputPosition + 3 >= outputSize) {
      int newPosition = outputPosition + 3 - outputSize;
      if (!BufferedOutputStream::Next(
        reinterpret_cast<void **>(&outputBuffer),
        &outputSize)) {
        throw std::runtime_error(
          "Failed to get next output buffer from output stream.");
      }
      outputPosition = newPosition;
    } else {
      outputPosition += 3;
    }
  }

  /**
   * Streaming compression base class
   */
  class CompressionStream: public CompressionStreamBase {
  public:
    CompressionStream(OutputStream * outStream,
                          int compressionLevel,
                          uint64_t capacity,
                          uint64_t blockSize,
                          MemoryPool& pool);

    virtual bool Next(void** data, int*size) override;
    virtual std::string getName() const override = 0;

  protected:
    // return total compressed size
    virtual uint64_t doStreamingCompression() = 0;
  };

  CompressionStream::CompressionStream(OutputStream * outStream,
                                       int compressionLevel,
                                       uint64_t capacity,
                                       uint64_t blockSize,
                                       MemoryPool& pool) :
                                         CompressionStreamBase(outStream,
                                                               compressionLevel,
                                                               capacity,
                                                               blockSize,
                                                               pool) {
    // PASS
  }

  bool CompressionStream::Next(void** data, int*size) {
    if (bufferSize != 0) {
      ensureHeader();

      uint64_t totalCompressedSize = doStreamingCompression();

      char * header = outputBuffer + outputPosition - totalCompressedSize - 3;
      if (totalCompressedSize >= static_cast<unsigned long>(bufferSize)) {
        writeHeader(header, static_cast<size_t>(bufferSize), true);
        memcpy(
          header + 3,
          rawInputBuffer.data(),
          static_cast<size_t>(bufferSize));

        int backup = static_cast<int>(totalCompressedSize) - bufferSize;
        BufferedOutputStream::BackUp(backup);
        outputPosition -= backup;
        outputSize -= backup;
      } else {
        writeHeader(header, totalCompressedSize, false);
      }
    }

    *data = rawInputBuffer.data();
    *size = static_cast<int>(rawInputBuffer.size());
    bufferSize = *size;

    return true;
  }

  class ZlibCompressionStream: public CompressionStream {
  public:
    ZlibCompressionStream(OutputStream * outStream,
                          int compressionLevel,
                          uint64_t capacity,
                          uint64_t blockSize,
                          MemoryPool& pool);

    virtual ~ZlibCompressionStream() override {
      end();
    }

    virtual std::string getName() const override;

  protected:
    virtual uint64_t doStreamingCompression() override;

  private:
    void init();
    void end();
    z_stream strm;
  };

  ZlibCompressionStream::ZlibCompressionStream(
                        OutputStream * outStream,
                        int compressionLevel,
                        uint64_t capacity,
                        uint64_t blockSize,
                        MemoryPool& pool)
                        : CompressionStream(outStream,
                                            compressionLevel,
                                            capacity,
                                            blockSize,
                                            pool) {
    init();
  }

  uint64_t ZlibCompressionStream::doStreamingCompression() {
    if (deflateReset(&strm) != Z_OK) {
      throw std::runtime_error("Failed to reset inflate.");
    }

    strm.avail_in = static_cast<unsigned int>(bufferSize);
    strm.next_in = rawInputBuffer.data();

    do {
      if (outputPosition >= outputSize) {
        if (!BufferedOutputStream::Next(
          reinterpret_cast<void **>(&outputBuffer),
          &outputSize)) {
          throw std::runtime_error(
            "Failed to get next output buffer from output stream.");
        }
        outputPosition = 0;
      }
      strm.next_out = reinterpret_cast<unsigned char *>
      (outputBuffer + outputPosition);
      strm.avail_out = static_cast<unsigned int>
      (outputSize - outputPosition);

      int ret = deflate(&strm, Z_FINISH);
      outputPosition = outputSize - static_cast<int>(strm.avail_out);

      if (ret == Z_STREAM_END) {
        break;
      } else if (ret == Z_OK) {
        // needs more buffer so will continue the loop
      } else {
        throw std::runtime_error("Failed to deflate input data.");
      }
    } while (strm.avail_out == 0);

    return strm.total_out;
  }

  std::string ZlibCompressionStream::getName() const {
    return "ZlibCompressionStream";
  }

DIAGNOSTIC_PUSH

#if defined(__GNUC__) || defined(__clang__)
  DIAGNOSTIC_IGNORE("-Wold-style-cast")
#endif

  void ZlibCompressionStream::init() {
    strm.zalloc = nullptr;
    strm.zfree = nullptr;
    strm.opaque = nullptr;
    strm.next_in = nullptr; 

    if (deflateInit2(&strm, level, Z_DEFLATED, -15, 8, Z_DEFAULT_STRATEGY)
        != Z_OK) {
      throw std::runtime_error("Error while calling deflateInit2() for zlib.");
    }
  }

  void ZlibCompressionStream::end() {
    (void)deflateEnd(&strm);
  }

DIAGNOSTIC_PUSH

  enum DecompressState { DECOMPRESS_HEADER,
                         DECOMPRESS_START,
                         DECOMPRESS_CONTINUE,
                         DECOMPRESS_ORIGINAL,
                         DECOMPRESS_EOF};

  class ZlibDecompressionStream: public SeekableInputStream {
  public:
    ZlibDecompressionStream(std::unique_ptr<SeekableInputStream> inStream,
                            size_t blockSize,
                            MemoryPool& pool);
    virtual ~ZlibDecompressionStream() override;
    virtual bool Next(const void** data, int*size) override;
    virtual void BackUp(int count) override;
    virtual bool Skip(int count) override;
    virtual int64_t ByteCount() const override;
    virtual void seek(PositionProvider& position) override;
    virtual std::string getName() const override;

  private:
    void readBuffer(bool failOnEof) {
      int length;
      if (!input->Next(reinterpret_cast<const void**>(&inputBuffer),
                       &length)) {
        if (failOnEof) {
          throw ParseError("Read past EOF in "
                           "ZlibDecompressionStream::readBuffer");
        }
        state = DECOMPRESS_EOF;
        inputBuffer = nullptr;
        inputBufferEnd = nullptr;
      } else {
        inputBufferEnd = inputBuffer + length;
      }
    }

    uint32_t readByte(bool failOnEof) {
      if (inputBuffer == inputBufferEnd) {
        readBuffer(failOnEof);
        if (state == DECOMPRESS_EOF) {
          return 0;
        }
      }
      return static_cast<unsigned char>(*(inputBuffer++));
    }

    void readHeader() {
      uint32_t header = readByte(false);
      if (state != DECOMPRESS_EOF) {
        header |= readByte(true) << 8;
        header |= readByte(true) << 16;
        if (header & 1) {
          state = DECOMPRESS_ORIGINAL;
        } else {
          state = DECOMPRESS_START;
        }
        remainingLength = header >> 1;
      } else {
        remainingLength = 0;
      }
    }

    MemoryPool& pool;
    const size_t blockSize;
    std::unique_ptr<SeekableInputStream> input;
    z_stream zstream;
    DataBuffer<char> buffer;

    // the current state
    DecompressState state;

    // the start of the current buffer
    // This pointer is not owned by us. It is either owned by zstream or
    // the underlying stream.
    const char* outputBuffer;
    // the size of the current buffer
    size_t outputBufferLength;
    // the size of the current chunk
    size_t remainingLength;

    // the last buffer returned from the input
    const char *inputBuffer;
    const char *inputBufferEnd;

    // roughly the number of bytes returned
    off_t bytesReturned;
  };

DIAGNOSTIC_PUSH

#if defined(__GNUC__) || defined(__clang__)
  DIAGNOSTIC_IGNORE("-Wold-style-cast")
#endif

  ZlibDecompressionStream::ZlibDecompressionStream
                   (std::unique_ptr<SeekableInputStream> inStream,
                    size_t _blockSize,
                    MemoryPool& _pool
                    ): pool(_pool),
                       blockSize(_blockSize),
                       buffer(pool, _blockSize) {
    input.reset(inStream.release());
    zstream.next_in = nullptr;
    zstream.avail_in = 0;
    zstream.zalloc = nullptr;
    zstream.zfree = nullptr;
    zstream.opaque = nullptr;
    zstream.next_out = reinterpret_cast<Bytef*>(buffer.data());
    zstream.avail_out = static_cast<uInt>(blockSize);
    int64_t result = inflateInit2(&zstream, -15);
    switch (result) {
    case Z_OK:
      break;
    case Z_MEM_ERROR:
      throw std::logic_error("Memory error from inflateInit2");
    case Z_VERSION_ERROR:
      throw std::logic_error("Version error from inflateInit2");
    case Z_STREAM_ERROR:
      throw std::logic_error("Stream error from inflateInit2");
    default:
      throw std::logic_error("Unknown error from inflateInit2");
    }
    outputBuffer = nullptr;
    outputBufferLength = 0;
    remainingLength = 0;
    state = DECOMPRESS_HEADER;
    inputBuffer = nullptr;
    inputBufferEnd = nullptr;
    bytesReturned = 0;
  }

DIAGNOSTIC_POP

  ZlibDecompressionStream::~ZlibDecompressionStream() {
    int64_t result = inflateEnd(&zstream);
    if (result != Z_OK) {
      // really can't throw in destructors
      std::cout << "Error in ~ZlibDecompressionStream() " << result << "\n";
    }
  }

  bool ZlibDecompressionStream::Next(const void** data, int*size) {
    // if the user pushed back, return them the partial buffer
    if (outputBufferLength) {
      *data = outputBuffer;
      *size = static_cast<int>(outputBufferLength);
      outputBuffer += outputBufferLength;
      outputBufferLength = 0;
      return true;
    }
    if (state == DECOMPRESS_HEADER || remainingLength == 0) {
      readHeader();
    }
    if (state == DECOMPRESS_EOF) {
      return false;
    }
    if (inputBuffer == inputBufferEnd) {
      readBuffer(true);
    }
    size_t availSize =
      std::min(static_cast<size_t>(inputBufferEnd - inputBuffer),
               remainingLength);
    if (state == DECOMPRESS_ORIGINAL) {
      *data = inputBuffer;
      *size = static_cast<int>(availSize);
      outputBuffer = inputBuffer + availSize;
      outputBufferLength = 0;
    } else if (state == DECOMPRESS_START) {
      zstream.next_in =
        reinterpret_cast<Bytef*>(const_cast<char*>(inputBuffer));
      zstream.avail_in = static_cast<uInt>(availSize);
      outputBuffer = buffer.data();
      zstream.next_out =
        reinterpret_cast<Bytef*>(const_cast<char*>(outputBuffer));
      zstream.avail_out = static_cast<uInt>(blockSize);
      if (inflateReset(&zstream) != Z_OK) {
        throw std::logic_error("Bad inflateReset in "
                               "ZlibDecompressionStream::Next");
      }
      int64_t result;
      do {
        result = inflate(&zstream, availSize == remainingLength ? Z_FINISH :
                         Z_SYNC_FLUSH);
        switch (result) {
        case Z_OK:
          remainingLength -= availSize;
          inputBuffer += availSize;
          readBuffer(true);
          availSize =
            std::min(static_cast<size_t>(inputBufferEnd - inputBuffer),
                     remainingLength);
          zstream.next_in =
            reinterpret_cast<Bytef*>(const_cast<char*>(inputBuffer));
          zstream.avail_in = static_cast<uInt>(availSize);
          break;
        case Z_STREAM_END:
          break;
        case Z_BUF_ERROR:
          throw std::logic_error("Buffer error in "
                                 "ZlibDecompressionStream::Next");
        case Z_DATA_ERROR:
          throw std::logic_error("Data error in "
                                 "ZlibDecompressionStream::Next");
        case Z_STREAM_ERROR:
          throw std::logic_error("Stream error in "
                                 "ZlibDecompressionStream::Next");
        default:
          throw std::logic_error("Unknown error in "
                                 "ZlibDecompressionStream::Next");
        }
      } while (result != Z_STREAM_END);
      *size = static_cast<int>(blockSize - zstream.avail_out);
      *data = outputBuffer;
      outputBufferLength = 0;
      outputBuffer += *size;
    } else {
      throw std::logic_error("Unknown compression state in "
                             "ZlibDecompressionStream::Next");
    }
    inputBuffer += availSize;
    remainingLength -= availSize;
    bytesReturned += *size;
    return true;
  }

  void ZlibDecompressionStream::BackUp(int count) {
    if (outputBuffer == nullptr || outputBufferLength != 0) {
      throw std::logic_error("Backup without previous Next in "
                             "ZlibDecompressionStream");
    }
    outputBuffer -= static_cast<size_t>(count);
    outputBufferLength = static_cast<size_t>(count);
    bytesReturned -= count;
  }

  bool ZlibDecompressionStream::Skip(int count) {
    bytesReturned += count;
    // this is a stupid implementation for now.
    // should skip entire blocks without decompressing
    while (count > 0) {
      const void *ptr;
      int len;
      if (!Next(&ptr, &len)) {
        return false;
      }
      if (len > count) {
        BackUp(len - count);
        count = 0;
      } else {
        count -= len;
      }
    }
    return true;
  }

  int64_t ZlibDecompressionStream::ByteCount() const {
    return bytesReturned;
  }

  void ZlibDecompressionStream::seek(PositionProvider& position) {
    // clear state to force seek to read from the right position
    state = DECOMPRESS_HEADER;
    outputBuffer = nullptr;
    outputBufferLength = 0;
    remainingLength = 0;
    inputBuffer = nullptr;
    inputBufferEnd = nullptr;

    input->seek(position);
    bytesReturned = static_cast<off_t>(input->ByteCount());
    if (!Skip(static_cast<int>(position.next()))) {
      throw ParseError("Bad skip in ZlibDecompressionStream::seek");
    }
  }

  std::string ZlibDecompressionStream::getName() const {
    std::ostringstream result;
    result << "zlib(" << input->getName() << ")";
    return result.str();
  }

  class BlockDecompressionStream: public SeekableInputStream {
  public:
    BlockDecompressionStream(std::unique_ptr<SeekableInputStream> inStream,
                             size_t blockSize,
                             MemoryPool& pool);

    virtual ~BlockDecompressionStream() override {}
    virtual bool Next(const void** data, int*size) override;
    virtual void BackUp(int count) override;
    virtual bool Skip(int count) override;
    virtual int64_t ByteCount() const override;
    virtual void seek(PositionProvider& position) override;
    virtual std::string getName() const override = 0;

  protected:
    virtual uint64_t decompress(const char *input, uint64_t length,
                                char *output, size_t maxOutputLength) = 0;

    std::string getStreamName() const {
      return input->getName();
    }

  private:
    void readBuffer(bool failOnEof) {
      int length;
      if (!input->Next(reinterpret_cast<const void**>(&inputBufferPtr),
                       &length)) {
        if (failOnEof) {
          throw ParseError(getName() + "read past EOF");
        }
        state = DECOMPRESS_EOF;
        inputBufferPtr = nullptr;
        inputBufferPtrEnd = nullptr;
      } else {
        inputBufferPtrEnd = inputBufferPtr + length;
      }
    }

    uint32_t readByte(bool failOnEof) {
      if (inputBufferPtr == inputBufferPtrEnd) {
        readBuffer(failOnEof);
        if (state == DECOMPRESS_EOF) {
          return 0;
        }
      }
      return static_cast<unsigned char>(*(inputBufferPtr++));
    }

    void readHeader() {
      uint32_t header = readByte(false);
      if (state != DECOMPRESS_EOF) {
        header |= readByte(true) << 8;
        header |= readByte(true) << 16;
        if (header & 1) {
          state = DECOMPRESS_ORIGINAL;
        } else {
          state = DECOMPRESS_START;
        }
        remainingLength = header >> 1;
      } else {
        remainingLength = 0;
      }
    }

    std::unique_ptr<SeekableInputStream> input;
    MemoryPool& pool;

    // may need to stitch together multiple input buffers;
    // to give snappy a contiguous block
    DataBuffer<char> inputBuffer;

    // uncompressed output
    DataBuffer<char> outputBuffer;

    // the current state
    DecompressState state;

    // the start of the current output buffer
    const char* outputBufferPtr;
    // the size of the current output buffer
    size_t outputBufferLength;

    // the size of the current chunk
    size_t remainingLength;

    // the last buffer returned from the input
    const char *inputBufferPtr;
    const char *inputBufferPtrEnd;

    // bytes returned by this stream
    off_t bytesReturned;
  };

  BlockDecompressionStream::BlockDecompressionStream
                   (std::unique_ptr<SeekableInputStream> inStream,
                    size_t bufferSize,
                    MemoryPool& _pool
                    ) : pool(_pool),
                        inputBuffer(pool, bufferSize),
                        outputBuffer(pool, bufferSize),
                        state(DECOMPRESS_HEADER),
                        outputBufferPtr(nullptr),
                        outputBufferLength(0),
                        remainingLength(0),
                        inputBufferPtr(nullptr),
                        inputBufferPtrEnd(nullptr),
                        bytesReturned(0) {
    input.reset(inStream.release());
  }

  bool BlockDecompressionStream::Next(const void** data, int*size) {
    // if the user pushed back, return them the partial buffer
    if (outputBufferLength) {
      *data = outputBufferPtr;
      *size = static_cast<int>(outputBufferLength);
      outputBufferPtr += outputBufferLength;
      bytesReturned += static_cast<off_t>(outputBufferLength);
      outputBufferLength = 0;
      return true;
    }
    if (state == DECOMPRESS_HEADER || remainingLength == 0) {
      readHeader();
    }
    if (state == DECOMPRESS_EOF) {
      return false;
    }
    if (inputBufferPtr == inputBufferPtrEnd) {
      readBuffer(true);
    }

    size_t availSize =
      std::min(static_cast<size_t>(inputBufferPtrEnd - inputBufferPtr),
               remainingLength);
    if (state == DECOMPRESS_ORIGINAL) {
      *data = inputBufferPtr;
      *size = static_cast<int>(availSize);
      outputBufferPtr = inputBufferPtr + availSize;
      outputBufferLength = 0;
      inputBufferPtr += availSize;
      remainingLength -= availSize;
    } else if (state == DECOMPRESS_START) {
      // Get contiguous bytes of compressed block.
      const char *compressed = inputBufferPtr;
      if (remainingLength == availSize) {
          inputBufferPtr += availSize;
      } else {
        // Did not read enough from input.
        if (inputBuffer.capacity() < remainingLength) {
          inputBuffer.resize(remainingLength);
        }
        ::memcpy(inputBuffer.data(), inputBufferPtr, availSize);
        inputBufferPtr += availSize;
        compressed = inputBuffer.data();

        for (size_t pos = availSize; pos < remainingLength; ) {
          readBuffer(true);
          size_t avail =
              std::min(static_cast<size_t>(inputBufferPtrEnd -
                                           inputBufferPtr),
                       remainingLength - pos);
          ::memcpy(inputBuffer.data() + pos, inputBufferPtr, avail);
          pos += avail;
          inputBufferPtr += avail;
        }
      }

      outputBufferLength = decompress(compressed, remainingLength,
                                      outputBuffer.data(),
                                      outputBuffer.capacity());

      remainingLength = 0;
      state = DECOMPRESS_HEADER;
      *data = outputBuffer.data();
      *size = static_cast<int>(outputBufferLength);
      outputBufferPtr = outputBuffer.data() + outputBufferLength;
      outputBufferLength = 0;
    }

    bytesReturned += *size;
    return true;
  }

  void BlockDecompressionStream::BackUp(int count) {
    if (outputBufferPtr == nullptr || outputBufferLength != 0) {
      throw std::logic_error("Backup without previous Next in "+getName());
    }
    outputBufferPtr -= static_cast<size_t>(count);
    outputBufferLength = static_cast<size_t>(count);
    bytesReturned -= count;
  }

  bool BlockDecompressionStream::Skip(int count) {
    bytesReturned += count;
    // this is a stupid implementation for now.
    // should skip entire blocks without decompressing
    while (count > 0) {
      const void *ptr;
      int len;
      if (!Next(&ptr, &len)) {
        return false;
      }
      if (len > count) {
        BackUp(len - count);
        count = 0;
      } else {
        count -= len;
      }
    }
    return true;
  }

  int64_t BlockDecompressionStream::ByteCount() const {
    return bytesReturned;
  }

  void BlockDecompressionStream::seek(PositionProvider& position) {
    // clear state to force seek to read from the right position
    state = DECOMPRESS_HEADER;
    outputBufferPtr = nullptr;
    outputBufferLength = 0;
    remainingLength = 0;
    inputBufferPtr = nullptr;
    inputBufferPtrEnd = nullptr;

    input->seek(position);
    if (!Skip(static_cast<int>(position.next()))) {
      throw ParseError("Bad skip in " + getName());
    }
  }

  class SnappyDecompressionStream: public BlockDecompressionStream {
  public:
    SnappyDecompressionStream(std::unique_ptr<SeekableInputStream> inStream,
                              size_t blockSize,
                              MemoryPool& pool
                              ): BlockDecompressionStream
                                 (std::move(inStream),
                                  blockSize,
                                  pool) {
      // PASS
    }

    std::string getName() const override {
      std::ostringstream result;
      result << "snappy(" << getStreamName() << ")";
      return result.str();
    }

  protected:
    virtual uint64_t decompress(const char *input, uint64_t length,
                                char *output, size_t maxOutputLength
                                ) override;
  };

  uint64_t SnappyDecompressionStream::decompress(const char *input,
                                                 uint64_t length,
                                                 char *output,
                                                 size_t maxOutputLength) {
    size_t outLength;
    if (!snappy::GetUncompressedLength(input, length, &outLength)) {
      throw ParseError("SnappyDecompressionStream choked on corrupt input");
    }

    if (outLength > maxOutputLength) {
      throw std::logic_error("Snappy length exceeds block size");
    }

    if (!snappy::RawUncompress(input, length, output)) {
      throw ParseError("SnappyDecompressionStream choked on corrupt input");
    }
    return outLength;
  }

  class LzoDecompressionStream: public BlockDecompressionStream {
  public:
    LzoDecompressionStream(std::unique_ptr<SeekableInputStream> inStream,
                           size_t blockSize,
                           MemoryPool& pool
                           ): BlockDecompressionStream
                              (std::move(inStream),
                               blockSize,
                               pool) {
      // PASS
    }

    std::string getName() const override {
      std::ostringstream result;
      result << "lzo(" << getStreamName() << ")";
      return result.str();
    }

  protected:
    virtual uint64_t decompress(const char *input, uint64_t length,
                                char *output, size_t maxOutputLength
                                ) override;
  };

  uint64_t LzoDecompressionStream::decompress(const char *input,
                                              uint64_t length,
                                              char *output,
                                              size_t maxOutputLength) {
    return lzoDecompress(input, input + length, output,
                         output + maxOutputLength);
  }

  class Lz4DecompressionStream: public BlockDecompressionStream {
  public:
    Lz4DecompressionStream(std::unique_ptr<SeekableInputStream> inStream,
                           size_t blockSize,
                           MemoryPool& pool
                           ): BlockDecompressionStream
                              (std::move(inStream),
                               blockSize,
                               pool) {
      // PASS
    }

    std::string getName() const override {
      std::ostringstream result;
      result << "lz4(" << getStreamName() << ")";
      return result.str();
    }

  protected:
    virtual uint64_t decompress(const char *input, uint64_t length,
                                char *output, size_t maxOutputLength
                                ) override;
  };

  uint64_t Lz4DecompressionStream::decompress(const char *input,
                                              uint64_t length,
                                              char *output,
                                              size_t maxOutputLength) {
    int result = LZ4_decompress_safe(input, output, static_cast<int>(length),
                                     static_cast<int>(maxOutputLength));
    if (result < 0) {
      throw ParseError(getName() + " - failed to decompress");
    }
    return static_cast<uint64_t>(result);
  }

  /**
   * Block compression base class
   */
  class BlockCompressionStream: public CompressionStreamBase {
  public:
    BlockCompressionStream(OutputStream * outStream,
                           int compressionLevel,
                           uint64_t capacity,
                           uint64_t blockSize,
                           MemoryPool& pool)
                           : CompressionStreamBase(outStream,
                                                   compressionLevel,
                                                   capacity,
                                                   blockSize,
                                                   pool)
                           , compressorBuffer(pool) {
      // PASS
    }

    virtual bool Next(void** data, int*size) override;
    virtual std::string getName() const override = 0;

  protected:
    // compresses a block and returns the compressed size
    virtual uint64_t doBlockCompression() = 0;

    // return maximum possible compression size for allocating space for
    // compressorBuffer below
    virtual uint64_t estimateMaxCompressionSize() = 0;

    // should allocate max possible compressed size
    DataBuffer<unsigned char> compressorBuffer;
  };

  bool BlockCompressionStream::Next(void** data, int*size) {
    if (bufferSize != 0) {
      ensureHeader();

      // perform compression
      size_t totalCompressedSize = doBlockCompression();

      const unsigned char * dataToWrite = nullptr;
      int totalSizeToWrite = 0;
      char * header = outputBuffer + outputPosition - 3;

      if (totalCompressedSize >= static_cast<size_t>(bufferSize)) {
        writeHeader(header, static_cast<size_t>(bufferSize), true);
        dataToWrite = rawInputBuffer.data();
        totalSizeToWrite = bufferSize;
      } else {
        writeHeader(header, totalCompressedSize, false);
        dataToWrite = compressorBuffer.data();
        totalSizeToWrite = static_cast<int>(totalCompressedSize);
      }

      char * dst = header + 3;
      while (totalSizeToWrite > 0) {
        if (outputPosition == outputSize) {
          if (!BufferedOutputStream::Next(reinterpret_cast<void **>(&outputBuffer),
                                          &outputSize)) {
            throw std::logic_error(
              "Failed to get next output buffer from output stream.");
          }
          outputPosition = 0;
          dst = outputBuffer;
        } else if (outputPosition > outputSize) {
          // this will unlikely happen, but we have seen a few on zstd v1.1.0
          throw std::logic_error("Write to an out-of-bound place!");
        }

        int sizeToWrite = std::min(totalSizeToWrite, outputSize - outputPosition);
        std::memcpy(dst, dataToWrite, static_cast<size_t>(sizeToWrite));

        outputPosition += sizeToWrite;
        dataToWrite += sizeToWrite;
        totalSizeToWrite -= sizeToWrite;
        dst += sizeToWrite;
      }
    }

    *data = rawInputBuffer.data();
    *size = static_cast<int>(rawInputBuffer.size());
    bufferSize = *size;
    compressorBuffer.resize(estimateMaxCompressionSize());

    return true;
  }

  /**
   * ZSTD block compression
   */
  class ZSTDCompressionStream: public BlockCompressionStream {
  public:
    ZSTDCompressionStream(OutputStream * outStream,
                          int compressionLevel,
                          uint64_t capacity,
                          uint64_t blockSize,
                          MemoryPool& pool)
                          : BlockCompressionStream(outStream,
                                                   compressionLevel,
                                                   capacity,
                                                   blockSize,
                                                   pool) {
      this->init(); 
    }

    virtual std::string getName() const override {
      return "ZstdCompressionStream";
    }
     
    virtual ~ZSTDCompressionStream() override { 
      this->end(); 
    } 

  protected:
    virtual uint64_t doBlockCompression() override;

    virtual uint64_t estimateMaxCompressionSize() override {
      return ZSTD_compressBound(static_cast<size_t>(bufferSize));
    }
     
  private: 
    void init(); 
    void end(); 
    ZSTD_CCtx *cctx; 
  };

  uint64_t ZSTDCompressionStream::doBlockCompression() {
    return ZSTD_compressCCtx(cctx, 
                             compressorBuffer.data(), 
                             compressorBuffer.size(), 
                             rawInputBuffer.data(), 
                             static_cast<size_t>(bufferSize), 
                             level); 
  }
   
DIAGNOSTIC_PUSH 

#if defined(__GNUC__) || defined(__clang__) 
  DIAGNOSTIC_IGNORE("-Wold-style-cast") 
#endif 
 
  void ZSTDCompressionStream::init() { 
 
    cctx = ZSTD_createCCtx(); 
    if (!cctx) { 
      throw std::runtime_error("Error while calling ZSTD_createCCtx() for zstd."); 
    } 
  } 
 
 
  void ZSTDCompressionStream::end() { 
    (void)ZSTD_freeCCtx(cctx); 
    cctx = nullptr; 
  } 
 
DIAGNOSTIC_PUSH 
 
  /**
   * ZSTD block decompression
   */
  class ZSTDDecompressionStream: public BlockDecompressionStream {
  public:
    ZSTDDecompressionStream(std::unique_ptr<SeekableInputStream> inStream,
                            size_t blockSize,
                            MemoryPool& pool)
                            : BlockDecompressionStream(std::move(inStream),
                                                       blockSize,
                                                       pool) {
      this->init(); 
    }

    virtual ~ZSTDDecompressionStream() override { 
      this->end(); 
    } 
 
    std::string getName() const override {
      std::ostringstream result;
      result << "zstd(" << getStreamName() << ")";
      return result.str();
    }

  protected:
    virtual uint64_t decompress(const char *input,
                                uint64_t length,
                                char *output,
                                size_t maxOutputLength) override;
 
  private: 
    void init(); 
    void end(); 
    ZSTD_DCtx *dctx; 
  };

  uint64_t ZSTDDecompressionStream::decompress(const char *input,
                                               uint64_t length,
                                               char *output,
                                               size_t maxOutputLength) {
    return static_cast<uint64_t>(ZSTD_decompressDCtx(dctx, 
                                                     output, 
                                                     maxOutputLength, 
                                                     input, 
                                                     length)); 
  }

DIAGNOSTIC_PUSH 
 
#if defined(__GNUC__) || defined(__clang__) 
  DIAGNOSTIC_IGNORE("-Wold-style-cast") 
#endif 
 
  void ZSTDDecompressionStream::init() { 
 
    dctx = ZSTD_createDCtx(); 
    if (!dctx) { 
      throw std::runtime_error("Error while calling ZSTD_createDCtx() for zstd."); 
    } 
  } 
 
 
  void ZSTDDecompressionStream::end() { 
    (void)ZSTD_freeDCtx(dctx); 
    dctx = nullptr; 
  } 
 
DIAGNOSTIC_PUSH 
 
  std::unique_ptr<BufferedOutputStream>
     createCompressor(
                      CompressionKind kind,
                      OutputStream * outStream,
                      CompressionStrategy strategy,
                      uint64_t bufferCapacity,
                      uint64_t compressionBlockSize,
                      MemoryPool& pool) {
    switch (static_cast<int64_t>(kind)) {
    case CompressionKind_NONE: {
      return std::unique_ptr<BufferedOutputStream>
        (new BufferedOutputStream(
                pool, outStream, bufferCapacity, compressionBlockSize));
    }
    case CompressionKind_ZLIB: {
      int level = (strategy == CompressionStrategy_SPEED) ?
              Z_BEST_SPEED + 1 : Z_DEFAULT_COMPRESSION;
      return std::unique_ptr<BufferedOutputStream>
        (new ZlibCompressionStream(
                outStream, level, bufferCapacity, compressionBlockSize, pool));
    }
    case CompressionKind_ZSTD: {
      int level = (strategy == CompressionStrategy_SPEED) ?
              1 : ZSTD_CLEVEL_DEFAULT;
      return std::unique_ptr<BufferedOutputStream>
        (new ZSTDCompressionStream(
          outStream, level, bufferCapacity, compressionBlockSize, pool));
    }
    case CompressionKind_SNAPPY:
    case CompressionKind_LZO:
    case CompressionKind_LZ4:
    default:
      throw NotImplementedYet("compression codec");
    }
  }

  std::unique_ptr<SeekableInputStream>
     createDecompressor(CompressionKind kind,
                        std::unique_ptr<SeekableInputStream> input,
                        uint64_t blockSize,
                        MemoryPool& pool) {
    switch (static_cast<int64_t>(kind)) {
    case CompressionKind_NONE:
      return REDUNDANT_MOVE(input);
    case CompressionKind_ZLIB:
      return std::unique_ptr<SeekableInputStream>
        (new ZlibDecompressionStream(std::move(input), blockSize, pool));
    case CompressionKind_SNAPPY:
      return std::unique_ptr<SeekableInputStream>
        (new SnappyDecompressionStream(std::move(input), blockSize, pool));
    case CompressionKind_LZO:
      return std::unique_ptr<SeekableInputStream>
        (new LzoDecompressionStream(std::move(input), blockSize, pool));
    case CompressionKind_LZ4:
      return std::unique_ptr<SeekableInputStream>
        (new Lz4DecompressionStream(std::move(input), blockSize, pool));
    case CompressionKind_ZSTD:
      return std::unique_ptr<SeekableInputStream>
        (new ZSTDDecompressionStream(std::move(input), blockSize, pool));
    default: {
      std::ostringstream buffer;
      buffer << "Unknown compression codec " << kind;
      throw NotImplementedYet(buffer.str());
    }
    }
  }

}
