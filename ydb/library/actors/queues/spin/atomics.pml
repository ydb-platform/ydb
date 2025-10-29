#ifndef BASE_PML
#define BASE_PML

inline atomic_compare_exchange(location, expected, desired, success) {
    atomic {
        if
        :: location == expected ->
            location = desired
            success = true
        :: else ->
            success = false
            expected = location
        fi
    }
}

inline blind_atomic_compare_exchange(location, expected, desired) {
    atomic {
        if
        :: location == expected ->
            location = desired
        :: else ->
            expected = location
        fi
    }
}

inline atomic_load(location, value) {
    atomic {
        value = location
    }
}

inline atomic_store(location, value) {
    atomic {
        location = value
    }
}

inline atomic_increment(location) {
    atomic {
        location = location + 1
    }
}

inline atomic_decrement(location) {
    atomic {
        location = location - 1
    }
}

#endif // BASE_PML
