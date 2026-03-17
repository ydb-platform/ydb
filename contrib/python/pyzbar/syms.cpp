#include <library/python/symbols/registry/syms.h>
#include <contrib/libs/zbar/include/zbar.h>

using namespace zbar;

BEGIN_SYMS("zbar")

SYM(zbar_image_create)
SYM(zbar_image_destroy)
SYM(zbar_image_first_symbol)
SYM(zbar_image_scanner_create)
SYM(zbar_image_scanner_destroy)
SYM(zbar_image_scanner_set_config)
SYM(zbar_image_set_data)
SYM(zbar_image_set_format)
SYM(zbar_image_set_size)
SYM(zbar_scan_image)
SYM(zbar_symbol_get_data_length)
SYM(zbar_symbol_get_data)
SYM(zbar_symbol_get_loc_size)
SYM(zbar_symbol_get_loc_x)
SYM(zbar_symbol_get_loc_y)
SYM(zbar_symbol_next)
SYM(zbar_symbol_get_quality)
SYM(zbar_parse_config)
SYM(zbar_version)
SYM(zbar_set_verbosity)

END_SYMS()
