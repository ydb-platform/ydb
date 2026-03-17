// OpenCV CPU baseline features

#define CV_CPU_COMPILE_VSX 1
#define CV_CPU_BASELINE_COMPILE_VSX 1

#define CV_CPU_BASELINE_FEATURES 0 \
    , CV_CPU_VSX \


// OpenCV supported CPU dispatched features

#define CV_CPU_DISPATCH_COMPILE_VSX3 1


#define CV_CPU_DISPATCH_FEATURES 0 \
    , CV_CPU_VSX3 \

