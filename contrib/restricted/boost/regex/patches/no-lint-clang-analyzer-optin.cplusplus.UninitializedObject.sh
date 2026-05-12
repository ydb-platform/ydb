set -xue

add_nolint_to()
{
	search_text=$(echo "$1" | sed 's/[\/&]/\\&/g')
	pattern='/'${search_text}'/s/$/ \/* NOLINT(clang-analyzer-optin.cplusplus.UninitializedObject) *\//'
    sed -i "$pattern" "$2"
}

add_nolint_to "construct_init(e, f);" include/boost/regex/v5/perl_matcher.hpp
