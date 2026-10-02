set -xue

add_nolint_to()
{
	search_text=$(echo "$1" | sed 's/[\/&]/\\&/g')
	pattern='/'${search_text}'/s/$/ \/* NOLINT(clang-analyzer-optin.cplusplus.UninitializedObject) *\//'
    sed -i "$pattern" "$2"
}

add_nolint_to "callbacks(callbacks), encoding(encoding), src(encoding)" include/boost/property_tree/json_parser/detail/parser.hpp
