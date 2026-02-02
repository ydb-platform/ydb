#include <cstdint>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace NKikimr::NKqp {

class TMCMCMT19937 {
public:
    using result_type = uint32_t;

    // Constructor
    explicit TMCMCMT19937(uint64_t seed) : Seed_(seed), Counter_(0) {
        Engine_.seed(static_cast<uint32_t>(seed));
    }

    // Standard RNG interface
    result_type operator()() {
        ++Counter_;
        return Engine_();
    }

    static constexpr result_type min() { return std::mt19937::min(); }
    static constexpr result_type max() { return std::mt19937::max(); }

    // Seed management
    void seed(uint64_t s) {
        Seed_ = s;
        Counter_ = 0;
        Markers_.clear();
        SnapshotMarkers_.clear();
        Engine_.seed(static_cast<uint32_t>(s));
    }

    // Discard operations
    void discard(uint64_t z) {
        Counter_ += z;
        Engine_.discard(z);
    }

    // Hierarchical markers
    void Mark(size_t depth) {
        // If in replay mode and we have a snapshot for this depth, jump to it
        if (!SnapshotMarkers_.empty() && depth < SnapshotMarkers_.size()) {
            uint64_t target = SnapshotMarkers_[depth];
            JumpTo(target);

            // After replaying this marker, clear it so we go back to normal mode
            SnapshotMarkers_[depth] = 0;

            // If all snapshot markers have been used, exit replay mode completely
            bool allUsed = true;
            for (uint64_t marker : SnapshotMarkers_) {
                if (marker != 0) {
                    allUsed = false;
                    break;
                }
            }
            if (allUsed) {
                SnapshotMarkers_.clear();
            }
        }

        // Record current position
        if (depth >= Markers_.size()) {
            Markers_.resize(depth + 1, 0);
        }
        Markers_[depth] = Counter_;
    }

    void Rewind(size_t depth) {
        if (depth >= Markers_.size()) {
            throw std::out_of_range("Rewind depth not marked");
        }
        JumpTo(Markers_[depth]);
    }

    // Serialization
    std::string SerializeToHex() const {
        std::vector<uint8_t> binary = SerializeToBinary();
        return BinaryToHex(binary);
    }

    void RestoreFromHex(const std::string& hex) {
        std::vector<uint8_t> binary = HexToBinary(hex);
        RestoreFromBinary(binary);
    }

    // Accessors
    uint64_t GetCounter() const { return Counter_; }
    uint64_t GetSeed() const { return Seed_; }
    bool IsReplayMode() const { return !SnapshotMarkers_.empty(); }

private:
    std::mt19937 Engine_;
    uint64_t Seed_;
    uint64_t Counter_;
    std::vector<uint64_t> Markers_;
    std::vector<uint64_t> SnapshotMarkers_;

    void JumpTo(uint64_t target) {
        // Always reset and discard - don't optimize
        Engine_.seed(static_cast<uint32_t>(Seed_));
        Counter_ = 0;

        if (target > 0) {
            Engine_.discard(target);
            Counter_ = target;
        }
    }

    // VarInt encoding/decoding (LEB128)
    static void EncodeVarInt(uint64_t value, std::vector<uint8_t>& out) {
        do {
            uint8_t byte = value & 0x7F;
            value >>= 7;
            if (value != 0) {
                byte |= 0x80;
            }
            out.push_back(byte);
        } while (value != 0);
    }

    static uint64_t DecodeVarInt(const std::vector<uint8_t>& data, size_t& pos) {
        if (pos >= data.size()) {
            throw std::runtime_error("VarInt decode: unexpected end of data");
        }

        uint64_t result = 0;
        int shift = 0;

        while (pos < data.size()) {
            uint8_t byte = data[pos++];
            result |= static_cast<uint64_t>(byte & 0x7F) << shift;

            if ((byte & 0x80) == 0) {
                break;
            }

            shift += 7;
            if (shift >= 64) {
                throw std::runtime_error("VarInt decode: overflow");
            }
        }

        return result;
    }

    // Binary serialization
    std::vector<uint8_t> SerializeToBinary() const {
        std::vector<uint8_t> result;

        EncodeVarInt(Seed_, result);
        EncodeVarInt(Markers_.size(), result);

        uint64_t prev = 0;
        for (uint64_t marker : Markers_) {
            uint64_t delta = marker - prev;
            EncodeVarInt(delta, result);
            prev = marker;
        }

        uint64_t deltaHead = Counter_ - (Markers_.empty() ? 0 : Markers_.back());
        EncodeVarInt(deltaHead, result);

        return result;
    }

    void RestoreFromBinary(const std::vector<uint8_t>& data) {
        size_t pos = 0;

        Seed_ = DecodeVarInt(data, pos);

        size_t numMarkers = static_cast<size_t>(DecodeVarInt(data, pos));

        SnapshotMarkers_.clear();
        SnapshotMarkers_.reserve(numMarkers);
        uint64_t prev = 0;
        for (size_t i = 0; i < numMarkers; ++i) {
            uint64_t delta = DecodeVarInt(data, pos);
            uint64_t marker = prev + delta;
            SnapshotMarkers_.push_back(marker);
            prev = marker;
        }

        uint64_t deltaHead = DecodeVarInt(data, pos);
        Counter_ = (SnapshotMarkers_.empty() ? 0 : SnapshotMarkers_.back()) + deltaHead;

        Markers_.clear();
        Engine_.seed(static_cast<uint32_t>(Seed_));

        JumpTo(Counter_);
    }

    static std::string BinaryToHex(const std::vector<uint8_t>& data) {
        std::ostringstream oss;
        oss << std::hex;
        for (uint8_t byte : data) {
            oss << "0123456789abcdef"[byte >> 4];
            oss << "0123456789abcdef"[byte & 0x0F];
        }
        return oss.str();
    }

    static std::vector<uint8_t> HexToBinary(const std::string& hex) {
        if (hex.length() % 2 != 0) {
            throw std::invalid_argument("Hex string must have even length");
        }

        std::vector<uint8_t> result;
        result.reserve(hex.length() / 2);

        for (size_t i = 0; i < hex.length(); i += 2) {
            int high = HexCharToInt(hex[i]);
            int low = HexCharToInt(hex[i + 1]);
            if (high < 0 || low < 0) {
                throw std::invalid_argument("Invalid hex character");
            }
            result.push_back(static_cast<uint8_t>((high << 4) | low));
        }

        return result;
    }

    static int HexCharToInt(char c) {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'a' && c <= 'f') return c - 'a' + 10;
        if (c >= 'A' && c <= 'F') return c - 'A' + 10;
        return -1;
    }
};

}
