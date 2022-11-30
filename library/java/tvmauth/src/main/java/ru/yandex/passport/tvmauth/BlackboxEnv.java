package ru.yandex.passport.tvmauth;

/**
 * BlackboxEnv describes environment of Passport:
 * https://wiki.yandex-team.ru/passport/tvm2/user-ticket/#0-opredeljaemsjasokruzhenijami
 */
public enum BlackboxEnv {
    PROD,
    TEST,
    PROD_YATEAM,
    TEST_YATEAM,
    STRESS,
}
