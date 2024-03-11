struct TArgs_float4_to_char_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
struct TArgs_float8_to_char_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
struct TArgs_ginarrayconsistent_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 8> IsFixedArg = {true,true,false,true,true,true,true,true};
};
struct TArgs_ginarrayextract_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,true};
};
struct TArgs_ginarrayextract_2args_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
struct TArgs_ginarraytriconsistent_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 7> IsFixedArg = {true,true,false,true,true,true,true};
};
struct TArgs_ginqueryarrayextract_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 7> IsFixedArg = {false,true,true,true,true,true,true};
};
struct TArgs_int4_to_char_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
struct TArgs_int8_to_char_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
struct TArgs_interval_to_char_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_numeric_to_char_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_numeric_to_number_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_thesaurus_init_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_thesaurus_lexize_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {true,true,true,true};
};
struct TArgs_timestamp_to_char_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
struct TArgs_timestamptz_to_char_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
struct TArgs_to_date_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_to_timestamp_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_tsquery_rewrite_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
struct TArgs_tsquery_rewrite_query_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
