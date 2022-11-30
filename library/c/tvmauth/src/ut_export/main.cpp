// DO_NOT_STYLE
#include <stdio.h>

#include <deprecated_wrapper.h>
#include <high_lvl_wrapper.h>
#include <tvmauth_wrapper.h>

void foo(int lvl, const char* msg) {
    (void)lvl;
    (void)msg;
    fprintf(stderr, "%s\n", msg);
}

int main() {
    try {
    const char tvmKeys[] = "";
    const char clientSecret[] = "";
    const uint32_t tvmId = 123;
    const char srvTicket[] = "";
    const char usrTicket[] = "";

    NTvmAuthWrapper::TScopes sco = std::vector<std::string>();
    (void)sco;
    NTvmAuthWrapper::TUids ui = std::vector<uint64_t>();
    (void)ui;

    NTvmAuthWrapper::TTvmId id = 17;
    (void)id;

    const char* i = NTvmAuthWrapper::NBlackboxTvmId::Prod;
    i = NTvmAuthWrapper::NBlackboxTvmId::Test;
    i = NTvmAuthWrapper::NBlackboxTvmId::ProdYateam;
    i = NTvmAuthWrapper::NBlackboxTvmId::TestYateam;
    i = NTvmAuthWrapper::NBlackboxTvmId::Stress;
    i = NTvmAuthWrapper::NBlackboxTvmId::Mimino;

    const char* ts = "100500";
    const char* dst = "456";
    const char* scopes = "";

    /* Service context */
    NTvmAuthWrapper::TServiceContext srvCtx(tvmId, clientSecret, tvmKeys);

    srvCtx = NTvmAuthWrapper::TServiceContext::CheckingFactory(tvmId, tvmKeys);
    srvCtx = NTvmAuthWrapper::TServiceContext::SigningFactory(clientSecret, tvmId);
    srvCtx = NTvmAuthWrapper::TServiceContext::SigningFactory(clientSecret);

    NTvmAuthWrapper::TCheckedServiceTicket st = srvCtx.Check(srvTicket);
    bool r = bool(st);

    TA_EErrorCode status = st.GetStatus();
    uint32_t src = st.GetSrc();
    (void)src;

    std::string debugInfo = st.DebugInfo();
    uint64_t issuer = st.GetIssuerUid();
    (void)issuer;

    std::string sub = NTvmAuthWrapper::RemoveTicketSignature(srvTicket);

    std::string sign = srvCtx.SignCgiParamsForTvm(ts, dst, scopes);
    (void)sign;

    /* User context */
    NTvmAuthWrapper::TUserContext usrCtx(TA_BE_TEST, tvmKeys);

    NTvmAuthWrapper::TCheckedUserTicket ut = usrCtx.Check(usrTicket);
    r = bool(ut);
    status = ut.GetStatus();

    NTvmAuthWrapper::TUids uids = ut.GetUids();
    (void)uids;

    NTvmAuthWrapper::TUid uid = ut.GetDefaultUid();
    (void)uid;

    NTvmAuthWrapper::TScopes sc = ut.GetScopes();
    bool hasScope = ut.HasScope(scopes);

    debugInfo = ut.DebugInfo();

    /* Etc */
    std::string ver = NTvmAuthWrapper::LibVersion();
    (void)ver;

    printf("OK");

    {
    NTvmAuthWrapper::TTvmApiClientSettings settings;

    NTvmAuthWrapper::TTvmClient client(settings, foo);
    NTvmAuthWrapper::TClientStatus code = client.GetStatus();
    NTvmAuthWrapper::ThrowIfFatal(TA_EC_OK);
    st = client.CheckServiceTicket("");
    ut = client.CheckUserTicket("");
    ut = client.CheckUserTicket("", TA_BE_TEST);

    std::string t = client.GetServiceTicketFor("asd");
    t = client.GetServiceTicketFor(100500);

    settings.SetSelfTvmId(100500);
    settings.EnableServiceTicketChecking();
    settings.EnableUserTicketChecking(TA_BE_TEST);
    settings.EnableServiceTicketsFetchOptions(
        "aaaaaaaaaaaaaaaa",
        NTvmAuthWrapper::TTvmApiClientSettings::TDstVector({19, 234}));
    settings.EnableServiceTicketsFetchOptions(
        "aaaaaaaaaaaaaaaa",
        NTvmAuthWrapper::TTvmApiClientSettings::TDstMap({
            {"foo", 19},
            {"bar", 234},
        }));
    settings.SetDiskCacheDir("");
    }

    {
    NTvmAuthWrapper::TTvmToolClientSettings settings("me");
    settings.SetPort(1);
    settings.SetHostname("localhost");
    settings.SetAuthtoken("qwe");
    settings.OverrideBlackboxEnv(TA_BE_TEST);
    NTvmAuthWrapper::TTvmClient(settings, foo);
    }

    NTvmAuthWrapper::TTvmApiClientSettings::TDst d(17);
    (void)d;
    NTvmAuthWrapper::TTvmApiClientSettings::TAlias t = std::string("123");
    (void)t;

    } catch (const NTvmAuthWrapper::TContextException&) {
    } catch (const NTvmAuthWrapper::TEmptyTvmKeysException&) {
    } catch (const NTvmAuthWrapper::TMalformedTvmKeysException&) {
    } catch (const NTvmAuthWrapper::TMalformedTvmSecretException&) {
    } catch (const NTvmAuthWrapper::TNotAllowedException&) {
    } catch (const NTvmAuthWrapper::TTvmException&) {
    } catch (...) {
    }

    return 0;
}
