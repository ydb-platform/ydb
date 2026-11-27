OVERWRITE_SECTIONS {
    /DISCARD/ : {
        *(.debug_gnu_pubnames)
        *(.debug_gnu_pubtypes)
    }
}
