SECTIONS {
    /DISCARD/ : {
        contrib/*(.debug_info .debug_info.*)
        contrib/*(.debug_loc .debug_loc.* .debug_loclists .debug_loclists.*)
        contrib/*(.debug_ranges .debug_rnglists .debug_rnglists.*)
        contrib/*(.debug_pubnames .debug_pubtypes)
        contrib/*(.debug_names .debug_names.*)
        contrib/*(.debug_macinfo .debug_macro .debug_macro.*)
    }
}
INSERT AFTER .comment;
