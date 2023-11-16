GO_LIBRARY()

LICENSE(BSD-3-Clause)

# https://st.yandex-team.ru/DEVTOOLS-5045
#GO_TEST_SRCS(floats_test.go)

SRCS(
    doc.go
    floats.go
)

END()

RECURSE(scalar)
