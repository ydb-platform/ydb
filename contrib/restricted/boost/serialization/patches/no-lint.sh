set -xue

add_nolint_to()
{
    expr='s|'${1}'|'${1}' /* NOLINT */|g'
    sed "${expr}" -i ${2}
}

add_nolint_to "BOOST_STATIC_CONSTANT(int, value = version::type::value);" include/boost/serialization/version.hpp
