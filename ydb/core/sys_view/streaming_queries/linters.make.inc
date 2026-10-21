IF (MODULE_LANG == CPP)
    SET(MODULE_COMMON_CONFIGS_DIR .github/config/cpp_format)
    STYLE_CPP(CONFIG_TYPE .clang-format)
ENDIF()
