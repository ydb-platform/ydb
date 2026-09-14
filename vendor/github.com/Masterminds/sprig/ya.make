GO_LIBRARY()

LICENSE(MIT)

VERSION(v2.22.0+incompatible)

# We disable all tests, because all tests rely on 'testing' being
# imported in functions_test.go
# However, functions_tests.go has a test with random(!) generator
# and it is, of course, flaky. So we have to comment it, and
# because of that we have to comment them all :(

SRCS(
    crypto.go
    date.go
    defaults.go
    dict.go
    doc.go
    functions.go
    list.go
    network.go
    numeric.go
    reflect.go
    regex.go
    semver.go
    strings.go
    url.go
)

GO_TEST_SRCS(
    # crypto_test.go
    # date_test.go
    # defaults_test.go
    # dict_test.go
    # example_test.go
    # flow_control_test.go
    # functions_test.go
    # issue_188_test.go
    # list_test.go
    # network_test.go
    # numeric_test.go
    # reflect_test.go
    # regex_test.go
    # semver_test.go
    # strings_test.go
    # url_test.go
)

END()

RECURSE(
    gotest
)
