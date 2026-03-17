# -*- coding: ascii -*-
from __future__ import print_function

import sys, glob, string

from .compat import xrange, unicode

try:
    from xlrd import open_workbook, XL_CELL_EMPTY, XL_CELL_BLANK, XL_CELL_TEXT, XL_CELL_NUMBER, cellname
    null_cell_types = (XL_CELL_EMPTY, XL_CELL_BLANK)
except ImportError:
    # older version
    from xlrd import open_workbook, XL_CELL_EMPTY, XL_CELL_TEXT, XL_CELL_NUMBER
    null_cell_types = (XL_CELL_EMPTY, )

def cells_all_junk(cells, is_rubbish=None):
    """\
    Return True if all cells in the sequence are junk.
    What qualifies as junk:
    -- empty cell
    -- blank cell
    -- zero-length text
    -- text is all whitespace
    -- number cell and is 0.0
    -- text cell and is_rubbish(cell.value) returns True.
    """
    for cell in cells:
        if cell.ctype in null_cell_types:
            continue
        if cell.ctype == XL_CELL_TEXT:
            if not cell.value:
                continue
            if cell.value.isspace():
                continue
        if cell.ctype == XL_CELL_NUMBER:
            if not cell.value:
                continue
        if is_rubbish is not None and is_rubbish(cell):
            continue
        return False
    return True

def ispunc(c, s=set(unicode(string.punctuation))):
    """Return True if c is a single punctuation character"""
    return c in s

def number_of_good_rows(sheet, checker=None, nrows=None, ncols=None):
    """Return 1 + the index of the last row with meaningful data in it."""
    if nrows is None: nrows = sheet.nrows
    if ncols is None: ncols = sheet.ncols
    for rowx in xrange(nrows - 1, -1, -1):
        if not cells_all_junk(sheet.row_slice(rowx, 0, ncols), checker):
            return rowx + 1
    return 0

def number_of_good_cols(sheet, checker=None, nrows=None, ncols=None):
    """Return 1 + the index of the last column with meaningful data in it."""
    if nrows is None: nrows = sheet.nrows
    if ncols is None: ncols = sheet.ncols
    for colx in xrange(ncols - 1, -1, -1):
        if not cells_all_junk(sheet.col_slice(colx, 0, nrows), checker):
            return colx+1
    return 0

def safe_encode(ustr, encoding):
    try:
        return ustr.encode(encoding)
    except (UnicodeEncodeError, UnicodeError):
        return repr(ustr)

def check_file(fname, verbose, do_punc=False, fmt_info=0, encoding='ascii', onesheet=''):
    print()
    print(fname)
    if do_punc:
        checker = ispunc
    else:
        checker = None
    try:
        book = open_workbook(fname, formatting_info=fmt_info, on_demand=True)
    except TypeError:
        try:
            book = open_workbook(fname, formatting_info=fmt_info)
        except TypeError:
            # this is becoming ridiculous
            book = open_workbook(fname)
    totold = totnew = totnotnull = 0
    if onesheet is None or onesheet == "":
        shxrange = range(book.nsheets)
    else:
        try:
            shxrange = [int(onesheet)]
        except ValueError:
            shxrange = [book.sheet_names().index(onesheet)]
    for shx in shxrange:
        sheet = book.sheet_by_index(shx)
        ngoodrows = number_of_good_rows(sheet, checker)
        ngoodcols = number_of_good_cols(sheet, checker, nrows=ngoodrows)
        oldncells = sheet.nrows * sheet.ncols
        newncells = ngoodrows * ngoodcols
        totold += oldncells
        totnew += newncells
        nnotnull = 0
        sheet_density_pct_s = ''
        if verbose >= 2:
            colxrange = range(ngoodcols)
            for rowx in xrange(ngoodrows):
                rowtypes = sheet.row_types(rowx)
                for colx in colxrange:
                    if rowtypes[colx] not in null_cell_types:
                        nnotnull += 1
            totnotnull += nnotnull
            sheet_density_pct = (nnotnull * 100.0) / max(1, newncells)
            sheet_density_pct_s = "; den = %5.1f%%" % sheet_density_pct
        if verbose >= 3:
            # which rows have non_empty cells in the right-most column?
            lastcolx = sheet.ncols - 1
            for rowx in xrange(sheet.nrows):
                cell = sheet.cell(rowx, lastcolx)
                if cell.ctype != XL_CELL_EMPTY:
                    print("%s (%d, %d): type %d, value %r" % (
                        cellname(rowx, lastcolx),
                        rowx, lastcolx, cell.ctype, cell.value
                    ))
        if (verbose
            or ngoodrows != sheet.nrows
            or ngoodcols != sheet.ncols
            or (verbose >= 2 and sheet_density_pct < 90.0)
            ):
            if oldncells:
                pctwaste = (1.0 - float(newncells) / oldncells) * 100.0
            else:
                pctwaste = 0.0
            shname_enc = safe_encode(sheet.name, encoding)
            print(
                "sheet #%2d: RxC %5d x %3d => %5d x %3d; %4.1f%% waste%s (%s)"
                % (shx, sheet.nrows, sheet.ncols,
                   ngoodrows, ngoodcols, pctwaste,
                   sheet_density_pct_s, shname_enc))
        if hasattr(book, 'unload_sheet'):
            book.unload_sheet(shx)
    if totold:
        pctwaste = (1.0 - float(totnew) / totold) * 100.0
    else:
        pctwaste = 0.0
    print("%d cells => %d cells; %4.1f%% waste" % (totold, totnew, pctwaste))
                
def main():
    import optparse
    usage = "%prog [options] input-file-patterns"
    oparser = optparse.OptionParser(usage)
    oparser.add_option(
        "-v", "--verbosity",
        type="int", default=0,
        help="level of information and diagnostics provided")
    oparser.add_option(
        "-p", "--punc",
        action="store_true", default=False,
        help="treat text cells containing only 1 punctuation char as rubbish")
    oparser.add_option(
        "-e", "--encoding",
        default='',
        help="encoding for text output")
    oparser.add_option(
        "-f", "--formatting",
        action="store_true", default=False,
        help="parse formatting information in the input files")
    oparser.add_option(
        "-s", "--onesheet",
        default="",
        help="restrict output to this sheet (name or index)")
    options, args = oparser.parse_args(sys.argv[1:])
    if len(args) < 1:
        oparser.error("Expected at least 1 arg, found %d" % len(args))
    encoding = options.encoding
    if not encoding:
        encoding = sys.stdout.encoding
    if not encoding:
        encoding = sys.getdefaultencoding()
    for pattern in args:
        for fname in glob.glob(pattern):
            try:
                check_file(fname,
                    options.verbosity, options.punc,
                    options.formatting, encoding, options.onesheet)
            except:
                e1, e2 = sys.exc_info()[:2]
                print("*** File %s => %s:%s" % (fname, e1.__name__, e2))
    
if __name__ == "__main__":
    main()
