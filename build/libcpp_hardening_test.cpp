// Compile-time verification that _LIBCPP_HARDENING_MODE_FAST is defined
// when using internal STL
#ifndef _LIBCPP_HARDENING_MODE_FAST
    #error "_LIBCPP_HARDENING_MODE_FAST is not defined - hardening mode not enabled!"
#endif

#ifndef _YDB_LIBCPP_HARDENING_ENABLED
    #error "_YDB_LIBCPP_HARDENING_ENABLED is not defined - YDB hardening flag not set!"
#endif

#include <vector>
#include <iostream>
#include <cstdlib>

// Test program to verify _LIBCPP_HARDENING_MODE_FAST is working
// This program intentionally triggers undefined behavior that should be caught
// by libc++ hardening mode
int main() {
    std::vector<int> vec = {1, 2, 3};
    
    std::cout << "=== YDB libc++ Hardening Test ===" << std::endl;
    std::cout << "Hardening mode is ENABLED (_LIBCPP_HARDENING_MODE_FAST is defined)" << std::endl;
    std::cout << "Vector size: " << vec.size() << std::endl;
    std::cout << "Accessing valid element vec[1]: " << vec[1] << std::endl;
    
    // This should trigger bounds checking with _LIBCPP_HARDENING_MODE_FAST
    // and cause the program to abort/terminate
    std::cout << "About to access out-of-bounds element vec[10]..." << std::endl;
    std::cout << "Value at vec[10]: " << vec[10] << std::endl;  // Out of bounds!
    
    std::cout << "ERROR: This line should not be reached with hardening enabled!" << std::endl;
    return 1; // Return error code if we reach here
}