set -xue

replace_has_keyword_in()
{
    sed 's/#if ABSL_HAVE_'${1}'('${2}')/#if ABSL_HAVE_'${1}'('${2}') \&\& !defined(__CUDACC__)/g' -i ${3}
}

# clang20 + cuda 12 support
replace_has_keyword_in BUILTIN __builtin_clzg absl/numeric/internal/bits.h
replace_has_keyword_in BUILTIN __builtin_ctzg absl/numeric/internal/bits.h
