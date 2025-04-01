#include <util/system/types.h>


#include "histogram.h"

int main() {

    auto hist1 = Histogram<Bucket>{
        .buckets = std::vector<Bucket>{Bucket(2, 16, 4), Bucket(16, 20, 4), Bucket(20, 30, 4)}
    };
    auto hist2 = Histogram<Bucket>{
        .buckets = std::vector<Bucket>{Bucket(0, 5, 5), Bucket(13, 22, 5), Bucket(25, 35, 5)}
    };
    auto hist3 = Histogram<Bucket>{
        .buckets = std::vector<Bucket>{Bucket(10, 18, 6), Bucket(23, 24, 6)}
    };

    auto merged = multiMerge(std::vector<Histogram<Bucket>>{hist1, hist2, hist3});

    std::cout << merged << "\n";

    BinArray fit = multifit(merged, 4);

    std::cout << "partitions distribution: " << fit << "\n";

    return 0;
}
