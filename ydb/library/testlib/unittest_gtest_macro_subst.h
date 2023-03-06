// Macro substitutions for tests converted from Unittest to GTest

#define UNIT_ASSERT( condition ) ASSERT_TRUE( condition )
#define UNIT_ASSERT_C( condition, comment ) ASSERT_TRUE( condition ) << comment
#define UNIT_ASSERT_STRINGS_EQUAL( p1, p2 ) ASSERT_EQ( p1, p2 )
#define UNIT_ASSERT_EQUAL( p1, p2 ) ASSERT_EQ( p1, p2 )
#define UNIT_ASSERT_EQUAL_C( p1, p2, comment ) ASSERT_EQ( p1, p2 ) << comment
#define UNIT_ASSERT_UNEQUAL( p1, p2 ) ASSERT_NE( p1, p2 )
#define UNIT_ASSERT_UNEQUAL_C( p1, p2, comment ) ASSERT_NE( p1, p2 ) << comment