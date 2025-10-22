#include "test_token_exchange_server.h"

void TTestTokenExchangeServer::Run(const std::function<void()>& f, bool checkExpectations) {
    Check.Reset();
    try {
        f();
    } catch (const std::exception& ex) {
        Check.Error = ex.what();
    }

    if (checkExpectations) {
        CheckExpectations();
    }
}

void TTestTokenExchangeServer::Run(const NYdb::TOauth2TokenExchangeParams& params, const std::string& expectedToken, bool checkExpectations) {
    std::string token;
    Run([&]() {
        auto factory = CreateOauth2TokenExchangeCredentialsProviderFactory(params);
        if (!expectedToken.empty()) {
            token = factory->CreateProvider()->GetAuthInfo();
        }
    },
    checkExpectations);

    if (!expectedToken.empty()) {
        UNIT_ASSERT_VALUES_EQUAL(expectedToken, token);
    }
}

void TTestTokenExchangeServer::RunFromConfig(const std::string& fileName, const std::string& expectedToken, bool checkExpectations, const std::string& explicitTokenEndpoint) {
    std::string token;
    Run([&]() {
        auto factory = NYdb::CreateOauth2TokenExchangeFileCredentialsProviderFactory(fileName, explicitTokenEndpoint);
        if (!expectedToken.empty()) {
            token = factory->CreateProvider()->GetAuthInfo();
        }
    },
    checkExpectations);

    if (!expectedToken.empty()) {
        UNIT_ASSERT_VALUES_EQUAL(expectedToken, token);
    }
}

void TTestTokenExchangeServer::TCheck::Check() {
    UNIT_ASSERT_C(InputParams || !ExpectRequest, "Request error: " << Error);
    if (InputParams) {
        if (SubjectJwtCheck || ActorJwtCheck) {
            TCgiParameters inputParamsCopy = *InputParams;
            if (SubjectJwtCheck) {
                std::string subjectJwt;
                UNIT_ASSERT(inputParamsCopy.Has("subject_token"));
                UNIT_ASSERT(inputParamsCopy.Has("subject_token_type", "urn:ietf:params:oauth:token-type:jwt"));
                subjectJwt = inputParamsCopy.Get("subject_token");
                inputParamsCopy.Erase("subject_token");
                inputParamsCopy.Erase("subject_token_type");
                SubjectJwtCheck->Check(subjectJwt);
            }
            if (ActorJwtCheck) {
                std::string actorJwt;
                UNIT_ASSERT(inputParamsCopy.Has("actor_token"));
                UNIT_ASSERT(inputParamsCopy.Has("actor_token_type", "urn:ietf:params:oauth:token-type:jwt"));
                actorJwt = inputParamsCopy.Get("actor_token");
                inputParamsCopy.Erase("actor_token");
                inputParamsCopy.Erase("actor_token_type");
                ActorJwtCheck->Check(actorJwt);
            }
            UNIT_ASSERT_VALUES_EQUAL(ExpectedInputParams.Print(), inputParamsCopy.Print());
        } else {
            UNIT_ASSERT_VALUES_EQUAL(ExpectedInputParams.Print(), InputParams->Print());
        }
    }

    if (!ExpectedErrorPart.empty()) {
        UNIT_ASSERT_STRING_CONTAINS(Error, ExpectedErrorPart);
    } else {
        UNIT_ASSERT(Error.empty());
    }
}

bool TTestTokenExchangeServer::TRequest::DoReply(const TReplyParams& params) {
    with_lock (Server->Lock) {
        const TParsedHttpFull parsed(params.Input.FirstLine());
        UNIT_ASSERT_VALUES_EQUAL(parsed.Path, "/exchange/token");
        const std::string bodyStr = params.Input.ReadAll();

        Server->Check.InputParams.emplace(bodyStr);
        THttpResponse resp(Server->Check.StatusCode);
        resp.SetContent(TString{Server->Check.Response});
        resp.OutTo(params.Output);
        return true;
    }
}
