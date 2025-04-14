#pragma once

#include <string>
#include <vector>

using std::vector;
using std::string;

void report_time(std::chrono::high_resolution_clock::time_point begin_time) {
    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> duration = end_time - begin_time;
    double time_ms = duration.count();
    std::cout << "Single Thread Sorting Time: " << time_ms << "[ms]" << std::endl;
}

void partition_cb(const vector<char*>& si, int s, vector<char*>& si_sample, size_t str_len) {
    int j = 0;
    int c = 0;

    size_t size = si.size() * str_len;
    int w = size / (s + 1);

    for (int k = 0; k < s;) {
        while (c < (k + 1) * w) {
            c += str_len;
            j += 1;
        }

        while ((k + 1) * w <= c && k < s) {
            si_sample[k] = si[j - 1];
            k += 1;
        }
    }
}