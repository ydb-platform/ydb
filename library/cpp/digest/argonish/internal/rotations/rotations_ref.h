#pragma once

namespace NArgonish {
    static inline ui64 Rotr(const ui64 w, const unsigned c) {
        return (w >> c) | (w << (64 - c));
    }
}
