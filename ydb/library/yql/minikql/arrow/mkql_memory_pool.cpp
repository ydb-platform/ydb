#include "mkql_memory_pool.h" 
 
namespace NKikimr::NMiniKQL { 
 
namespace { 
class TArrowMemoryPool : public arrow::MemoryPool { 
public: 
    explicit TArrowMemoryPool(TAllocState& allocState) 
        : AllocState(allocState) { 
    } 
 
    arrow::Status Allocate(int64_t size, uint8_t** out) override { 
        *out = static_cast<uint8_t*>(MKQLAllocWithSize(size)); 
        return arrow::Status(); 
    } 
 
    arrow::Status Reallocate(int64_t oldSize, int64_t newSize, uint8_t** ptr) override { 
        void* newPtr = MKQLAllocWithSize(newSize); 
        ::memcpy(newPtr, *ptr, std::min(oldSize, newSize)); 
        MKQLFreeWithSize(*ptr, oldSize); 
        *ptr = static_cast<uint8_t*>(newPtr); 
        return arrow::Status(); 
    } 
 
    void Free(uint8_t* buffer, int64_t size) override { 
        MKQLFreeWithSize(buffer, size); 
    } 
 
    int64_t bytes_allocated() const override { 
        return AllocState.GetUsed(); 
    } 
 
    int64_t max_memory() const override { 
        return -1; 
    } 
 
    std::string backend_name() const override { 
        return "MKQL"; 
    } 
 
private: 
    TAllocState& AllocState; 
}; 
} 
 
std::unique_ptr <arrow::MemoryPool> MakeArrowMemoryPool(TAllocState& allocState) { 
    return std::make_unique<TArrowMemoryPool>(allocState); 
} 
 
} 
