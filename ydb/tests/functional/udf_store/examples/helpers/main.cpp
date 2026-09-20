//! Intermediate WASM library used by the with_helpers UDF example.
//! Uploaded into modules as type=LIBRARY name "helpers" (see with_helpers/manifest.json).

extern "C" {
    __attribute__((visibility("default"))) long long helpers_scale(long long value)
    {
        return value * 3;
    }
}
