#pragma once

std::pair<int, bool> binary_search(const vector<char *>& v, char* splitter) {
    int l = 0;
    int r = v.size() - 1;

    while (l <= r) {
        int mid = (l + r) / 2;

        if (strcmp(v[mid], splitter) == 0) {
            return { mid, true };
        }

        if (strcmp(v[mid], splitter) < 0) {
            l = mid + 1;
        } else if (strcmp(v[mid], splitter) > 0) {
            r = mid - 1;
        }
    }

    return { l, false };
}