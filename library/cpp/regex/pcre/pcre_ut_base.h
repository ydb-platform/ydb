#define CHECK_MATCHES(EXPECTED, PATTERN, STR) \
    UNIT_ASSERT(EXPECTED == NPcre::TPcre<CHAR_TYPE>(STRING(PATTERN), OPTIMIZE).Matches(STRING(STR))); \
    UNIT_ASSERT(EXPECTED == NPcre::TPcre<CHAR_TYPE>(STRING(PATTERN), OPTIMIZE).Matches(STRING(STR), 0, 10));

#define CHECK(A, B) UNIT_ASSERT_STRINGS_EQUAL(ToString(STRING(A)), ToString(B))

#define CHECK_GROUPS(EXPECTED, PATTERN, STR) \
    CHECK(EXPECTED, NPcre::TPcre<CHAR_TYPE>(STRING(PATTERN), OPTIMIZE).Find(STRING(STR))); \
    CHECK(EXPECTED, NPcre::TPcre<CHAR_TYPE>(STRING(PATTERN), OPTIMIZE).Find(STRING(STR), 0, 10));

Y_UNIT_TEST_SUITE(TEST_NAME(TestRegExp)) {
    Y_UNIT_TEST(TestMatches) {
        CHECK_MATCHES(true,     "ю",                "bюd");
        CHECK_MATCHES(false,    "c",                "bюd");
        CHECK_MATCHES(true,     "(ю)(?:(b)c|bd)",   "zюbda");
        CHECK_MATCHES(false,    "(ю)(?:(b)c|bd)",   "bюd");
        CHECK_MATCHES(true,     "(abc|def)=\\g1",   "abc=abc");
        CHECK_MATCHES(true,     "(abc|def)=\\g1",   "def=def");
        CHECK_MATCHES(false,    "(abc|def)=\\g1",   "abc=def");
    }

    Y_UNIT_TEST(TestGroups) {
        CHECK_GROUPS("{1,2}",           "a",                "bad");
        CHECK_GROUPS("(empty maybe)",   "c",                "bad");
        CHECK_GROUPS("{1,4}",           "(a)(?:(b)c|bd)",   "zabda");
        CHECK_GROUPS("(empty maybe)",   "(a)(?:(b)c|bd)",   "bad");
        CHECK_GROUPS("{1,8}",           "(abc|def)=\\g1",   "aabc=abca");
        CHECK_GROUPS("(empty maybe)",   "(abc|def)=\\g1",   "abc=def");
    }

    Y_UNIT_TEST(TestCapture) {
        CHECK("[{1,2}]",NPcre::TPcre<CHAR_TYPE>(STRING("a"), OPTIMIZE).Capture(STRING("bad"), 0, 1));
        CHECK("[]",NPcre::TPcre<CHAR_TYPE>(STRING("c"), OPTIMIZE).Capture(STRING("bad"), 0, 1));
        CHECK("[{1,4},{1,2},{-1,-1},{3,4}]",NPcre::TPcre<CHAR_TYPE>(STRING("(a)(?:(b)c|b(d))"), OPTIMIZE).Capture(STRING("zabda"), 0, 1));
        CHECK("[]",NPcre::TPcre<CHAR_TYPE>(STRING("(a)(?:(b)c|bd)"), OPTIMIZE).Capture(STRING("bad"), 0, 1));
    }
}

