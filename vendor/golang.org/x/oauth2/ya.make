GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    deviceauth.go
    oauth2.go
    pkce.go
    token.go
    transport.go
)

GO_TEST_SRCS(
    deviceauth_test.go
    oauth2_test.go
    token_test.go
    transport_test.go
)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
    amazon
    authhandler
    bitbucket
    cern
    clientcredentials
    endpoints
    facebook
    fitbit
    foursquare
    github
    gitlab
    google
    gotest
    heroku
    hipchat
    instagram
    internal
    jira
    jws
    jwt
    kakao
    linkedin
    mailchimp
    mailru
    mediamath
    microsoft
    nokiahealth
    odnoklassniki
    paypal
    slack
    spotify
    stackoverflow
    twitch
    uber
    vk
    yahoo
    yandex
)
