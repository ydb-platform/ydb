set -xue

replace_has_keyword_in()
{
    sed 's/#if Y_ABSL_HAVE_'${1}'('${2}')/#if Y_ABSL_HAVE_'${1}'('${2}') \&\& !defined(__CUDACC__)/g' -i ${3}
}

# cuda 10 support
replace_has_keyword_in ATTRIBUTE enable_if y_absl/types/compare.h

# clang20 + cuda 12 support
replace_has_keyword_in BUILTIN __builtin_clzg y_absl/numeric/internal/bits.h
replace_has_keyword_in BUILTIN __builtin_ctzg y_absl/numeric/internal/bits.h
