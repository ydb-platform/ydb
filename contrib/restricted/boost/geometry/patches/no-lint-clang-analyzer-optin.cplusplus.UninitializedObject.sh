set -xue

add_nolint_to()
{
	search_text=$(echo "$1" | sed 's/[\/&]/\\&/g')
	pattern='/'${search_text}'/s/$/ \/* NOLINT(clang-analyzer-optin.cplusplus.UninitializedObject) *\//'
    sed -i "$pattern" "$2"
}

add_nolint_to ", count_in_original(0)" include/boost/geometry/algorithms/detail/buffer/buffer_policies.hpp
add_nolint_to "        piece()" include/boost/geometry/algorithms/detail/buffer/buffered_piece_collection.hpp
add_nolint_to "valid = geometry::point_on_border(this->point, ring_or_box);" include/boost/geometry/algorithms/detail/overlay/ring_properties.hpp
add_nolint_to "inline turn_info()" include/boost/geometry/algorithms/detail/overlay/turn_info.hpp
