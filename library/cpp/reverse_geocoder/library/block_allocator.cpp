#include "block_allocator.h"

using namespace NReverseGeocoder;

static size_t const MEMORY_IS_USED_FLAG = ~0ull;
static size_t const SIZEOF_SIZE = AlignMemory(sizeof(size_t));

void* NReverseGeocoder::TBlockAllocator::Allocate(size_t number) {
    number = AlignMemory(number);
    if (BytesAllocated_ + number + SIZEOF_SIZE > BytesLimit_)
        ythrow yexception() << "Unable allocate memory";
    char* begin = ((char*)Data_) + BytesAllocated_;
    char* end = begin + number;
    *((size_t*)end) = MEMORY_IS_USED_FLAG;
    BytesAllocated_ += number + SIZEOF_SIZE;
    return begin;
}

size_t NReverseGeocoder::TBlockAllocator::AllocateSize(size_t number) {
    return AlignMemory(number) + SIZEOF_SIZE;
}

static void RelaxBlock(char* begin, size_t* number) {
    while (*number > 0) {
        char* ptr = begin + *number - SIZEOF_SIZE;
        if (*((size_t*)ptr) == MEMORY_IS_USED_FLAG)
            return;
        *number -= *((size_t*)ptr) + SIZEOF_SIZE;
    }
}

void NReverseGeocoder::TBlockAllocator::Deallocate(void* ptr, size_t number) {
    number = AlignMemory(number);
    char* begin = (char*)ptr;
    char* end = begin + number;
    if (*((size_t*)end) != MEMORY_IS_USED_FLAG)
        ythrow yexception() << "Trying to deallocate not allocated pointer " << ptr;
    *((size_t*)end) = number;
    RelaxBlock((char*)Data_, &BytesAllocated_);
}
