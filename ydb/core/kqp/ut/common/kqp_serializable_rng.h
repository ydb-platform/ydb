#pragma once

#include <cstdint>
#include <random>
#include <cassert>


namespace NKikimr::NKqp {


// wrapper around std::mt19937 that tracks usage and simplifies serialization
class TSerializableMT19937 {
public: // compatibility with std::mt19937
    using result_type = std::mt19937::result_type;
    static constexpr auto default_seed = std::mt19937::default_seed;

public:
    TSerializableMT19937()
        : TSerializableMT19937(default_seed)
    {
    }
    
    TSerializableMT19937(uint32_t seed) 
        : Engine_(seed)
        , Seed_(seed)
        , Counter_(0)
    {
    }

    uint64_t Serialize() const {
        return (static_cast<uint64_t>(Seed_) << 32ULL) | static_cast<uint64_t>(Counter_);
    }

    void Restore(uint64_t state) {
        Seed_ = static_cast<uint32_t>(state >> 32);
        Counter_ = static_cast<uint32_t>(state & 0xFFFFFFFF);
        
        Engine_.seed(Seed_);
        Engine_.discard(Counter_);
    }

    void Forward(uint32_t counter) {
        assert(counter >= GetCounter());
        ui32 difference = counter - GetCounter();
        discard(difference);
    }

    uint32_t GetCounter() const {
        return Counter_;
    }

    uint32_t GetSeed() const {
        return Seed_;
    }
    
    static TSerializableMT19937 Deserialize(uint64_t key) {
        TSerializableMT19937 mt;
        mt.Restore(key);

        return mt;
    }

public: // compatibility with std::mt19937
    static constexpr auto min() { return std::mt19937::min(); }
    static constexpr auto max() { return std::mt19937::max(); }

    auto operator()() {
        assert(Counter_ != UINT32_MAX);
        ++ Counter_;
        return Engine_();
    }

    void seed(uint32_t seed) {
        Seed_ = seed;
        Counter_ = 0;

        Engine_.seed(seed);
    }
    
    void discard(uint64_t n) {
        assert(n <= static_cast<unsigned long long>(UINT32_MAX - Counter_));

        Counter_ += static_cast<uint32_t>(n);
        Engine_.discard(n);
    }
    
    void reset() {
        Counter_ = 0;
        Engine_.seed(Seed_);
    }
    
    bool operator==(const TSerializableMT19937& other) const {
        return Seed_ == other.Seed_ && Counter_ == other.Counter_;
    }
    
    bool operator!=(const TSerializableMT19937& other) const {
        return !(*this == other);
    }

private:
    std::mt19937 Engine_;
    uint32_t Seed_;
    uint32_t Counter_;

};

}

