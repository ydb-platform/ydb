cdef extern from "edlib.h" nogil:

     ctypedef enum EdlibAlignMode: EDLIB_MODE_NW, EDLIB_MODE_SHW, EDLIB_MODE_HW
     ctypedef enum EdlibAlignTask: EDLIB_TASK_DISTANCE, EDLIB_TASK_LOC, EDLIB_TASK_PATH
     ctypedef enum EdlibCigarFormat: EDLIB_CIGAR_STANDARD, EDLIB_CIGAR_EXTENDED

     ctypedef struct EdlibEqualityPair:
         char first
         char second

     ctypedef struct EdlibAlignConfig:
         int k
         EdlibAlignMode mode
         EdlibAlignTask task
         const EdlibEqualityPair* additionalEqualities
         int additionalEqualitiesLength

     EdlibAlignConfig edlibNewAlignConfig(int k, EdlibAlignMode mode, EdlibAlignTask task,
                                          const EdlibEqualityPair* additionalEqualities,
                                          int additionalEqualitiesLength)
     EdlibAlignConfig edlibDefaultAlignConfig()

     ctypedef struct EdlibAlignResult:
         int status
         int editDistance
         int* endLocations
         int* startLocations
         int numLocations
         unsigned char* alignment
         int alignmentLength
         int alphabetLength

     void edlibFreeAlignResult(EdlibAlignResult result)

     EdlibAlignResult edlibAlign(const char* query, int queryLength,
                                 const char* target, int targetLength,
                                 const EdlibAlignConfig config)

     char* edlibAlignmentToCigar(const unsigned char* alignment, int alignmentLength, EdlibCigarFormat cigarFormat)
