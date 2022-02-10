#include "rand.h" 
 
#include <cstdlib> 
#include <util/random/random.h> 
#include <util/system/types.h> 
 
int utilRandom() { 
    return (int)RandomNumber((ui32)RAND_MAX); 
} 
 
