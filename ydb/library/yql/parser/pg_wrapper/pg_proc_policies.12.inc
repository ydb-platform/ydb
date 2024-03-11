struct TArgs_ascii_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
struct TArgs_be_lo_close_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_be_lo_creat_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_be_lo_create_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_be_lo_export_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
struct TArgs_be_lo_from_bytea_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
struct TArgs_be_lo_get_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_be_lo_get_fragment_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
struct TArgs_be_lo_import_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
struct TArgs_be_lo_import_with_oid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
struct TArgs_be_lo_lseek_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
struct TArgs_be_lo_lseek64_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
struct TArgs_be_lo_open_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
struct TArgs_be_lo_put_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,false};
};
struct TArgs_be_lo_tell_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_be_lo_tell64_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_be_lo_truncate_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
struct TArgs_be_lo_truncate64_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
struct TArgs_be_lo_unlink_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_be_loread_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
struct TArgs_be_lowrite_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
struct TArgs_btrim_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_btrim1_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
struct TArgs_bytealike_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_bytealtrim_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_byteanlike_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_byteartrim_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_byteatrim_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_chr_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_gtsvector_compress_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_gtsvector_consistent_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,false,true,true,true};
};
struct TArgs_gtsvector_consistent_oldsig_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {true,false,true,true,true};
};
struct TArgs_gtsvector_decompress_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_gtsvector_options_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_gtsvector_penalty_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,true,true};
};
struct TArgs_gtsvector_picksplit_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
struct TArgs_gtsvector_same_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,true};
};
struct TArgs_gtsvector_union_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
struct TArgs_gtsvectorin_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
struct TArgs_gtsvectorout_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
struct TArgs_initcap_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
struct TArgs_json_array_element_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
struct TArgs_json_array_element_text_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
struct TArgs_json_array_length_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
struct TArgs_json_object_field_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_json_object_field_text_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_json_populate_record_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,true};
};
struct TArgs_json_strip_nulls_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
struct TArgs_json_to_record_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
struct TArgs_jsonb_array_element_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
struct TArgs_jsonb_array_element_text_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
struct TArgs_jsonb_array_length_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
struct TArgs_jsonb_concat_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_jsonb_delete_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_jsonb_delete_idx_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
struct TArgs_jsonb_delete_path_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_jsonb_insert_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,false,false,true};
};
struct TArgs_jsonb_object_field_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_jsonb_object_field_text_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_jsonb_populate_record_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,false};
};
struct TArgs_jsonb_pretty_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
struct TArgs_jsonb_set_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,false,false,true};
};
struct TArgs_jsonb_set_lax_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 5> IsFixedArg = {false,false,false,true,false};
};
struct TArgs_jsonb_strip_nulls_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
struct TArgs_jsonb_to_record_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
struct TArgs_like_escape_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_like_escape_bytea_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_lower_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
struct TArgs_lpad_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,false};
};
struct TArgs_ltrim_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_ltrim1_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
struct TArgs_nameiclike_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_nameicnlike_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_nameicregexeq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_nameicregexne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_namelike_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_namenlike_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_nameregexeq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_nameregexne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_pg_ndistinct_in_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
struct TArgs_pg_ndistinct_out_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
struct TArgs_pg_ndistinct_recv_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_ndistinct_send_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
struct TArgs_pg_nextoid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {true,false,true};
};
struct TArgs_pg_replication_origin_advance_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
struct TArgs_pg_replication_origin_create_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
struct TArgs_pg_replication_origin_drop_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
struct TArgs_pg_replication_origin_oid_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
struct TArgs_pg_replication_origin_progress_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
struct TArgs_pg_replication_origin_session_is_setup_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
struct TArgs_pg_replication_origin_session_progress_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {true};
};
struct TArgs_pg_replication_origin_session_reset_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
struct TArgs_pg_replication_origin_session_setup_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
struct TArgs_pg_replication_origin_xact_reset_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};
struct TArgs_pg_replication_origin_xact_setup_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {true,true};
};
struct TArgs_regexp_match_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
struct TArgs_regexp_match_no_flags_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_regexp_split_to_array_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
struct TArgs_regexp_split_to_array_no_flags_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_repeat_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,true};
};
struct TArgs_rpad_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,true,false};
};
struct TArgs_rtrim_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_rtrim1_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
struct TArgs_similar_escape_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_similar_to_escape_1_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
struct TArgs_similar_to_escape_2_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_texticlike_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_texticnlike_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_texticregexeq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_texticregexne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_textlike_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_textnlike_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_textregexeq_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_textregexne_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_textregexreplace_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 4> IsFixedArg = {false,false,false,false};
};
struct TArgs_textregexreplace_noopt_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
struct TArgs_textregexsubstr_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 2> IsFixedArg = {false,false};
};
struct TArgs_translate_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 3> IsFixedArg = {false,false,false};
};
struct TArgs_upper_Policy {
    static constexpr bool VarArgs = false;
    static constexpr std::array<bool, 1> IsFixedArg = {false};
};
