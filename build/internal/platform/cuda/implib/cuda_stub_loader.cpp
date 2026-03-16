extern "C" void* __cuda_sanitizers_dlopen_stub(const char*) {
    // If you are reading this text, it means that you are trying to run a binary with CUDA libraries and sanitizers.
    // To prevent a large number of false positive errors, we disable the use of CUDA libraries with sanitizers.
    return nullptr;
}
