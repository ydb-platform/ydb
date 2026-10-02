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

#include "BloomFilter.hh"
#include "Murmur3.hh"

namespace orc {

  constexpr uint64_t BITS_OF_LONG = 64;
  constexpr uint8_t SHIFT_6_BITS = 6;
  constexpr uint8_t SHIFT_3_BITS = 3;

  static bool isLittleEndian() {
    static union {
      uint32_t i;
      char c[4];
    } num = {0x01020304};
    return num.c[0] == 4;
  }

  /**
   * Implementation of BitSet
   */
  BitSet::BitSet(uint64_t numBits) {
    data_.resize(static_cast<size_t>(ceil(static_cast<double>(numBits) / BITS_OF_LONG)), 0);
  }

  BitSet::BitSet(const uint64_t* bits, uint64_t numBits) {
    // caller should make sure numBits is multiple of 64
    data_.resize(numBits >> SHIFT_6_BITS, 0);
    memcpy(data_.data(), bits, numBits >> SHIFT_3_BITS);
  }

  void BitSet::set(uint64_t index) {
    data_[index >> SHIFT_6_BITS] |= (1ULL << (index % BITS_OF_LONG));
  }

  bool BitSet::get(uint64_t index) {
    return (data_[index >> SHIFT_6_BITS] & (1ULL << (index % BITS_OF_LONG))) != 0;
  }

  uint64_t BitSet::bitSize() {
    return data_.size() << SHIFT_6_BITS;
  }

  void BitSet::merge(const BitSet& other) {
    if (data_.size() != other.data_.size()) {
      std::stringstream ss;
      ss << "BitSet must be of equal length (" << data_.size() << " != " << other.data_.size()
         << ")";
      throw std::logic_error(ss.str());
    }

    for (size_t i = 0; i != data_.size(); i++) {
      data_[i] |= other.data_[i];
    }
  }

  void BitSet::clear() {
    memset(data_.data(), 0, sizeof(uint64_t) * data_.size());
  }

  const uint64_t* BitSet::getData() const {
    return data_.data();
  }

  bool BitSet::operator==(const BitSet& other) const {
    return data_ == other.data_;
  }

  /**
   * Helper functions
   */
  void checkArgument(bool expression, const std::string& message) {
    if (!expression) {
      throw std::logic_error(message);
    }
  }

  int32_t optimalNumOfHashFunctions(uint64_t expectedEntries, uint64_t numBits) {
    double n = static_cast<double>(expectedEntries);
    return std::max<int32_t>(
        1, static_cast<int32_t>(std::round(static_cast<double>(numBits) / n * std::log(2.0))));
  }

  int32_t optimalNumOfBits(uint64_t expectedEntries, double fpp) {
    double n = static_cast<double>(expectedEntries);
    return static_cast<int32_t>(-n * std::log(fpp) / (std::log(2.0) * std::log(2.0)));
  }

  // We use the trick mentioned in "Less Hashing, Same Performance:
  // Building a Better Bloom Filter" by Kirsch et.al. From abstract
  // 'only two hash functions are necessary to effectively implement
  // a Bloom filter without any loss in the asymptotic false positive
  // probability'
  // Lets split up 64-bit hashcode into two 32-bit hash codes and employ
  // the technique mentioned in the above paper
  inline uint64_t getBytesHash(const char* data, int64_t length) {
    if (data == nullptr) {
      return Murmur3::NULL_HASHCODE;
    }

    return Murmur3::hash64(reinterpret_cast<const uint8_t*>(data), static_cast<uint32_t>(length));
  }

  /**
   * Implementation of BloomFilter
   */
  BloomFilterImpl::BloomFilterImpl(uint64_t expectedEntries, double fpp) {
    checkArgument(expectedEntries > 0, "expectedEntries should be > 0");
    checkArgument(fpp > 0.0 && fpp < 1.0, "False positive probability should be > 0.0 & < 1.0");

    uint64_t nb = static_cast<uint64_t>(optimalNumOfBits(expectedEntries, fpp));
    // make 'mNumBits' multiple of 64
    numBits_ = nb + (BITS_OF_LONG - (nb % BITS_OF_LONG));
    numHashFunctions_ = optimalNumOfHashFunctions(expectedEntries, numBits_);
    bitSet_.reset(new BitSet(numBits_));
  }

  void BloomFilterImpl::addBytes(const char* data, int64_t length) {
    uint64_t hash64 = getBytesHash(data, length);
    addHash(static_cast<int64_t>(hash64));
  }

  void BloomFilterImpl::addLong(int64_t data) {
    addHash(getLongHash(data));
  }

  bool BloomFilterImpl::testBytes(const char* data, int64_t length) const {
    uint64_t hash64 = getBytesHash(data, length);
    return testHash(static_cast<int64_t>(hash64));
  }

  bool BloomFilterImpl::testLong(int64_t data) const {
    return testHash(getLongHash(data));
  }

  uint64_t BloomFilterImpl::sizeInBytes() const {
    return getBitSize() >> SHIFT_3_BITS;
  }

  uint64_t BloomFilterImpl::getBitSize() const {
    return bitSet_->bitSize();
  }

  int32_t BloomFilterImpl::getNumHashFunctions() const {
    return numHashFunctions_;
  }

  DIAGNOSTIC_PUSH

#if defined(__clang__)
  DIAGNOSTIC_IGNORE("-Wundefined-reinterpret-cast")
#endif

#if defined(__GNUC__)
  DIAGNOSTIC_IGNORE("-Wstrict-aliasing")
#endif

  // caller should make sure input proto::BloomFilter is valid since
  // no check will be performed in the following constructor
  BloomFilterImpl::BloomFilterImpl(const proto::BloomFilter& bloomFilter) {
    numHashFunctions_ = static_cast<int32_t>(bloomFilter.num_hash_functions());

    const std::string& bitsetStr = bloomFilter.utf8bitset();
    numBits_ = bitsetStr.size() << SHIFT_3_BITS;
    checkArgument(numBits_ % BITS_OF_LONG == 0, "numBits should be multiple of 64!");

    const uint64_t* bitset = reinterpret_cast<const uint64_t*>(bitsetStr.data());
    if (isLittleEndian()) {
      bitSet_.reset(new BitSet(bitset, numBits_));
    } else {
      std::vector<uint64_t> longs(numBits_ >> SHIFT_6_BITS);
      for (size_t i = 0; i != longs.size(); ++i) {
        // convert little-endian to big-endian
        const uint64_t src = bitset[i];
        uint64_t& dst = longs[i];
        for (size_t bit = 0; bit != 64; bit += 8) {
          dst |= (((src & (0xFFu << bit)) >> bit) << (56 - bit));
        }
      }

      bitSet_.reset(new BitSet(longs.data(), numBits_));
    }
  }

  void BloomFilterImpl::addDouble(double data) {
    addLong(reinterpret_cast<int64_t&>(data));
  }

  bool BloomFilterImpl::testDouble(double data) const {
    return testLong(reinterpret_cast<int64_t&>(data));
  }

  DIAGNOSTIC_POP
  NO_SANITIZE_ATTR
  void BloomFilterImpl::addHash(int64_t hash64) {
    int32_t hash1 = static_cast<int32_t>(hash64 & 0xffffffff);
    // In Java codes, we use "hash64 >>> 32" which is an unsigned shift op.
    // So we cast hash64 to uint64_t here for an unsigned right shift.
    int32_t hash2 = static_cast<int32_t>(static_cast<uint64_t>(hash64) >> 32);

    for (int32_t i = 1; i <= numHashFunctions_; ++i) {
      int32_t combinedHash = hash1 + i * hash2;
      // hashcode should be positive, flip all the bits if it's negative
      if (combinedHash < 0) {
        combinedHash = ~combinedHash;
      }
      uint64_t pos = static_cast<uint64_t>(combinedHash) % numBits_;
      bitSet_->set(pos);
    }
  }

  NO_SANITIZE_ATTR
  bool BloomFilterImpl::testHash(int64_t hash64) const {
    int32_t hash1 = static_cast<int32_t>(hash64 & 0xffffffff);
    // In Java codes, we use "hash64 >>> 32" which is an unsigned shift op.
    // So we cast hash64 to uint64_t here for an unsigned right shift.
    int32_t hash2 = static_cast<int32_t>(static_cast<uint64_t>(hash64) >> 32);

    for (int32_t i = 1; i <= numHashFunctions_; ++i) {
      int32_t combinedHash = hash1 + i * hash2;
      // hashcode should be positive, flip all the bits if it's negative
      if (combinedHash < 0) {
        combinedHash = ~combinedHash;
      }
      uint64_t pos = static_cast<uint64_t>(combinedHash) % numBits_;
      if (!bitSet_->get(pos)) {
        return false;
      }
    }
    return true;
  }

  void BloomFilterImpl::merge(const BloomFilterImpl& other) {
    if (numBits_ != other.numBits_ || numHashFunctions_ != other.numHashFunctions_) {
      std::stringstream ss;
      ss << "BloomFilters are not compatible for merging: "
         << "this: numBits:" << numBits_ << ",numHashFunctions:" << numHashFunctions_
         << ", that: numBits:" << other.numBits_ << ",numHashFunctions:" << other.numHashFunctions_;
      throw std::logic_error(ss.str());
    }

    bitSet_->merge(*other.bitSet_);
  }

  void BloomFilterImpl::reset() {
    bitSet_->clear();
  }

  void BloomFilterImpl::serialize(proto::BloomFilter& bloomFilter) const {
    bloomFilter.set_num_hash_functions(static_cast<uint32_t>(numHashFunctions_));

    // According to ORC standard, the encoding is a sequence of bytes with
    // a little endian encoding in the utf8bitset field.
    if (isLittleEndian()) {
      // bytes are already organized in little endian; thus no conversion needed
      const char* bitset = reinterpret_cast<const char*>(bitSet_->getData());
      bloomFilter.set_utf8bitset(bitset, sizeInBytes());
    } else {
      std::vector<uint64_t> bitset(sizeInBytes() / sizeof(uint64_t), 0);
      const uint64_t* longs = bitSet_->getData();
      for (size_t i = 0; i != bitset.size(); ++i) {
        uint64_t& dst = bitset[i];
        const uint64_t src = longs[i];
        // convert big-endian to little-endian
        for (size_t bit = 0; bit != 64; bit += 8) {
          dst |= (((src & (0xFFu << bit)) >> bit) << (56 - bit));
        }
      }
      bloomFilter.set_utf8bitset(bitset.data(), sizeInBytes());
    }
  }

  bool BloomFilterImpl::operator==(const BloomFilterImpl& other) const {
    return numBits_ == other.numBits_ && numHashFunctions_ == other.numHashFunctions_ &&
           *bitSet_ == *other.bitSet_;
  }

  BloomFilter::~BloomFilter() {
    // PASS
  }

  std::unique_ptr<BloomFilter> BloomFilterUTF8Utils::deserialize(
      const proto::Stream_Kind& streamKind, const proto::ColumnEncoding& encoding,
      const proto::BloomFilter& bloomFilter) {
    // only BLOOM_FILTER_UTF8 is supported
    if (streamKind != proto::Stream_Kind_BLOOM_FILTER_UTF8) {
      return nullptr;
    }

    // make sure we don't use unknown encodings or original timestamp encodings
    if (!encoding.has_bloom_encoding() || encoding.bloom_encoding() != 1) {
      return nullptr;
    }

    // make sure all required fields exist
    if (!bloomFilter.has_num_hash_functions() || !bloomFilter.has_utf8bitset()) {
      return nullptr;
    }

    return std::make_unique<BloomFilterImpl>(bloomFilter);
  }

}  // namespace orc
