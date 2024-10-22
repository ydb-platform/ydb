#include "line_split.h"

TLineSplitter::TLineSplitter(IInputStream& stream)
    : Stream_(stream)
{
}

size_t TLineSplitter::Next(TString& st) {
    st.clear();
    char c;
    size_t ret = 0;
    if (HasPendingLineChar_) {
        st.push_back(PendingLineChar_);
        HasPendingLineChar_ = false;
        ++ret;
    }

    while (Stream_.ReadChar(c)) {
        ++ret;
        if (c == '\n') {
            break;
        } else if (c == '\r') {
            if (Stream_.ReadChar(c)) {
                ++ret;
                if (c != '\n') {
                    --ret;
                    PendingLineChar_ = c;
                    HasPendingLineChar_ = true;
                }
            }

            break;
        } else {
            st.push_back(c);
        }
    }

    return ret;
}
