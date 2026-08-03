GTEST(unittester-library-mpl)

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    concepts_ut.cpp
    tag_invoke_cpo_ut.cpp
    tag_invoke_impl_ut.cpp
    type_traits_ut.cpp
    wrapper_traits_ut.cpp
)

PEERDIR(
    library/cpp/yt/mpl

    library/cpp/testing/gtest
)

END()
