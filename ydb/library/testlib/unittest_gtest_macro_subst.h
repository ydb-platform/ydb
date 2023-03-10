// Macro substitutions for tests converted from Unittest to GTest

inline void DoUnitAssertValEqual(const char* s1, const char* s2) {
    ASSERT_STREQ(s1, s2);
}

template <typename T1, typename T2>
inline void DoUnitAssertValEqual(const T1& s1, const T2& s2) {
    ASSERT_EQ(s1, s2);
}

inline void DoUnitAssertValUnEqual(const char* s1, const char* s2) {
    ASSERT_STRNE(s1, s2);
}

template <typename T1, typename T2>
inline void DoUnitAssertValUnEqual(const T1& s1, const T2& s2) {
    ASSERT_NE(s1, s2);
}

#define UNIT_ASSERT( condition )                     ASSERT_TRUE( condition )
#define UNIT_ASSERT_C( condition, comment )          ASSERT_TRUE( condition ) << comment
#define UNIT_ASSERT_STRINGS_EQUAL( p1, p2 )          do { DoUnitAssertValEqual(p1, p2); } while (0);
#define UNIT_ASSERT_VALUES_EQUAL( p1, p2)            do { DoUnitAssertValEqual(p1, p2); } while (0);
#define UNIT_ASSERT_STRINGS_UNEQUAL( p1, p2 )        do { DoUnitAssertValUnEqual(p1, p2); } while (0);
#define UNIT_ASSERT_VALUES_UNEQUAL( p1, p2)          do { DoUnitAssertValUnEqual(p1, p2); } while (0);
#define UNIT_ASSERT_EQUAL( p1, p2 )                  ASSERT_EQ( p1, p2 )
#define UNIT_ASSERT_EQUAL_C( p1, p2, comment )       ASSERT_EQ( p1, p2 ) << comment
#define UNIT_ASSERT_UNEQUAL( p1, p2 )                ASSERT_NE( p1, p2 )
#define UNIT_ASSERT_UNEQUAL_C( p1, p2, comment )     ASSERT_NE( p1, p2 ) << comment
