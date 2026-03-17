# Copyright (c) 2008 Simplistix Ltd
#
# This Software is released under the MIT License:
# http://www.opensource.org/licenses/mit-license.html
# See license.txt for more details.

import xlrd

def quoted_sheet_name(sheet_name, encoding='ascii'):
    if "'" in sheet_name:
        qsn = "'" + sheet_name.replace("'", "''") + "'"
    elif " " in sheet_name:
        qsn = "'" + sheet_name + "'"
    else:
        qsn = sheet_name
    return qsn.encode(encoding, 'replace')
   
def cell_display(cell, datemode=0, encoding='ascii'):
    cty = cell.ctype
    if cty == xlrd.XL_CELL_EMPTY:
        return 'undefined'
    if cty == xlrd.XL_CELL_BLANK:
        return 'blank'
    if cty == xlrd.XL_CELL_NUMBER:
        return 'number (%.4f)' % cell.value
    if cty == xlrd.XL_CELL_DATE:
        try:
            return "date (%04d-%02d-%02d %02d:%02d:%02d)" \
                % xlrd.xldate_as_tuple(cell.value, datemode)
        except xlrd.xldate.XLDateError:
            return "date? (%.6f)" % cell.value
    if cty == xlrd.XL_CELL_TEXT:
        return "text (%s)" % cell.value.encode(encoding, 'replace')
    if cty == xlrd.XL_CELL_ERROR:
        if cell.value in xlrd.error_text_from_code:
            return "error (%s)" % xlrd.error_text_from_code[cell.value]
        return "unknown error code (%r)" % cell.value
    if cty == xlrd.XL_CELL_BOOLEAN:
        return "logical (%s)" % ['FALSE', 'TRUE'][cell.value]
    raise Exception("Unknown Cell.ctype: %r" % cty)
