#pragma once

#include <iostream>
#include <vector>
#include <string>
#include <sstream>
#include <iomanip>
#include <mutex>
#include <algorithm>


namespace NKikimr::NKqp {

class StreamAligner {
public:
    StreamAligner(size_t gapSize = 4, std::string delim = "&")
        : Delimiter_(std::move(delim))
        , Gap_(gapSize, ' ')
    {
    }

    std::string Align(const std::string& line) {
        // Find all the parts (delimited by Delimiter_) to be aligned
        std::vector<std::string> parts;
        size_t start = 0;
        size_t end = line.find(Delimiter_);

        while (end != std::string::npos) {
            parts.push_back(Trim_(line.substr(start, end - start)));
            start = end + Delimiter_.length();
            end = line.find(Delimiter_, start);
        }
        parts.push_back(Trim_(line.substr(start)));

        std::vector<size_t> snapshotWidths;
        // Update formatting anchors for later alignments + read current widths
        {
            std::lock_guard<std::mutex> lock(Mutex_);

            if (parts.size() > ColWidths_.size()) {
                ColWidths_.resize(parts.size(), 0);
            }

            // Go through all parts except the last one to allow for such behaviour
            // COLUMN COLUMN LAAAAAAAAAAAAAAAARGE
            // COLUMN COLUMN COLUMN COLUMN
            //                      ^ if there is no "&" in the previous row that's
            //                        further than this column, then don't move it

            for (size_t i = 1; i < parts.size(); ++i) {
                size_t partIdx = i - 1;

                size_t len = parts[partIdx].length();
                if (len > ColWidths_[partIdx]) {
                    ColWidths_[partIdx] = len;
                }
            }

            // Take a snapshot of current widths to release lock immediately
            snapshotWidths = ColWidths_;
        }

        // Align string appropriately
        std::ostringstream oss;
        for (size_t i = 0; i < parts.size(); ++i) {
            oss << std::left << std::setw(snapshotWidths[i]) << parts[i];
            if (i < parts.size() - 1) {
                oss << Gap_;
            }
        }

        return oss.str();
    }

private:
    std::vector<size_t> ColWidths_;
    std::string Delimiter_;
    std::string Gap_;
    mutable std::mutex Mutex_;

    std::string Trim_(const std::string& str) const {
        size_t first = str.find_first_not_of(" \t");
        if (std::string::npos == first) {
            return "";
        }
        size_t last = str.find_last_not_of(" \t");
        return str.substr(first, (last - first + 1));
    }
};

} // namespace NKikimr::NKqp
