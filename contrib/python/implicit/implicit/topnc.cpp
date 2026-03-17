#include <vector>
#include <limits>
#include <algorithm>
#include <iostream>

#include "topnc.h"

namespace {
struct target {
    int index;
    float value;
};

bool targets_compare(target t_i, target t_j) { return (t_i.value > t_j.value); }
}

void fargsort_c(float A[], int n_row, int m_row, int m_cols, int ktop, int B[]) {
    std::vector<target> targets;
    for ( int j = 0; j < m_cols; j++ ) {
        target c;
        c.index = j;
        c.value = A[(n_row*m_cols) + j];
        targets.push_back(c);
    }
    std::partial_sort(targets.begin(), targets.begin() + ktop, targets.end(), targets_compare);
    std::sort(targets.begin(), targets.begin() + ktop, targets_compare);
    for (int j = 0; j < ktop; j++) {
        B[(m_row*ktop) + j] = targets[j].index;
    }
}
