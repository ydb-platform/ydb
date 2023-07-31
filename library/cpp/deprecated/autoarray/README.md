Pre-C++11 vector-like container.

Just use std::vector. If you need to fill your vector with custom-constructed data, use reserve+emplace_back (but make sure that your elements are movable).
