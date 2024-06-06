#define COMPRESSION_LEVEL  1
#define STREAMING_MODE  0
#include "y.c"
#define COMPRESSION_LEVEL  1
#define STREAMING_MODE  100000
#include "y.c"
#define COMPRESSION_LEVEL  1
#define STREAMING_MODE  1000000
#include "y.c"
#define COMPRESSION_LEVEL  2
#define STREAMING_MODE  0
#include "y.c"
#define COMPRESSION_LEVEL  2
#define STREAMING_MODE  100000
#include "y.c"
#define COMPRESSION_LEVEL  2
#define STREAMING_MODE  1000000
#include "y.c"
#define COMPRESSION_LEVEL  3
#define STREAMING_MODE  0
#include "y.c"
#define COMPRESSION_LEVEL  3
#define STREAMING_MODE  100000
#include "y.c"
#define COMPRESSION_LEVEL  3
#define STREAMING_MODE  1000000
#include "y.c"
static struct TQuickLZMethods* methods_qlz151[3][3] = {
{&yqlz_1_51_1_0_table, &yqlz_1_51_1_100000_table, &yqlz_1_51_1_1000000_table, },
{&yqlz_1_51_2_0_table, &yqlz_1_51_2_100000_table, &yqlz_1_51_2_1000000_table, },
{&yqlz_1_51_3_0_table, &yqlz_1_51_3_100000_table, &yqlz_1_51_3_1000000_table, },
};

