#pragma once

#if USE_CUDNN == 91200
    #include "private/9.12.0/cudnn.h"
#elif USE_CUDNN == 91002
    #include "private/9.10.2/cudnn.h"
#elif USE_CUDNN == 90300
    #include "private/9.3.0/cudnn.h"
#elif USE_CUDNN == 90000
    #include "private/9.0.0/cudnn.h"
#elif USE_CUDNN == 80907
    #include "private/8.9.7/cudnn.h"
#elif USE_CUDNN == 80905
    #include "private/8.9.5/cudnn.h"
#elif USE_CUDNN == 80600
    #include "private/8.6.0/cudnn.h"
#elif USE_CUDNN == 80202
    #include "private/8.2.2/cudnn.h"
#elif USE_CUDNN == 80201
    #include "private/8.2.1/cudnn.h"
#elif USE_CUDNN == 80005
    #include "private/8.0.5/cudnn.h"
#elif USE_CUDNN == 70605
    #include "private/cudnn_7.6.5.32.h"
#else
    #error "cuDNN version is unknown"
#endif
