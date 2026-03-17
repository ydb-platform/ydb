# Copyright (c) 2008-2009 Simplistix Ltd
#
# This Software is released under the MIT License:
# http://www.opensource.org/licenses/mit-license.html
# See license.txt for more details.
from __future__ import print_function

import logging
import os
import xlrd,xlwt

from functools import partial
from glob import glob
from shutil import rmtree
from tempfile import mkdtemp
from xlutils.display import quoted_sheet_name,cell_display
from xlutils.margins import cells_all_junk
from xlwt.Style import default_style
from .compat import xrange


logger = logging.getLogger('xlutils.filter')

class BaseReader:
    "A base reader good for subclassing."

    def get_filepaths(self):
        """
        This is the most common method to implement. It must return an
        interable sequence of paths to excel files.
        """
        raise NotImplementedError

    def get_workbooks(self):
        """
        If the data to be processed is not stored in files or if
        special parameters need to be passed to :func:`xlrd.open_workbook`
        then this method must be overriden.
        Any implementation must return an iterable sequence of tuples.
        The first element of which must be an :class:`xlrd.Book` object and the
        second must be the filename of the file from which the book
        object came.
        """
        for path in self.get_filepaths():
            yield (
                xlrd.open_workbook(
                    path,
                    formatting_info=1,
                    on_demand=True,
                    ragged_rows=True),
                os.path.split(path)[1]
                )

    def __call__(self, filter):
        """
        Once instantiated, a reader will be called and have the first
        filter in the chain passed to its :meth:`__call__` method.
        The implementation of this method
        should call the appropriate methods on the filter based on the
        cells found in the :class:`~xlrd.Book` objects returned from the
        :meth:`get_workbooks` method.
        """
        filter.start()
        for workbook,filename in self.get_workbooks():
            filter.workbook(workbook,filename)
            for sheet_x in range(workbook.nsheets):
                sheet = workbook.sheet_by_index(sheet_x)
                filter.sheet(sheet,sheet.name)
                for row_x in xrange(sheet.nrows):
                    filter.row(row_x,row_x)
                    for col_x in xrange(sheet.row_len(row_x)):
                        filter.cell(row_x,col_x,row_x,col_x)
                if workbook.on_demand:
                    workbook.unload_sheet(sheet_x)
        filter.finish()
    
class BaseFilterInterface:
    """
    This is the filter interface that shows the correct way to call the 
    next filter in the chain. 
    The `next` attribute is set up by the :func:`process` function.
    It can make a good base class for a new filter, but subclassing
    :class:`BaseFilter` is often a better idea!
    """

    def start(self):
        """
        This method is called before processing of a batch of input.
        This allows the filter to initialise any required data
        structures and dispose of any existing state from previous
        batches. 

        It is called once before the processing of any workbooks by
        the included reader implementations.

        This method can be called at any time. One common use is to
        reset all the filters in a chain in the event of an error
        during the processing of a `rdbook`.

        Implementations of this method should be extremely robust and
        must ensure that they call the :meth:`start` method of the next filter
        in the chain regardless of any work they do.
        """
        self.next.start()
        
    def workbook(self,rdbook,wtbook_name):
        """
        This method is called every time processing of a new
        workbook starts.

        :param rdbook: the :class:`~xlrd.Book` object from which the new workbook
                 should be created.

        :param wtbook_name: the name of the workbook into which content
                      should be written.
        """
        self.next.workbook(rdbook,wtbook_name)
   
    def sheet(self,rdsheet,wtsheet_name):
        """
        This method is called every time processing of a new
        sheet in the current workbook starts.

        :param rdsheet: the :class:`~xlrd.sheet.Sheet` object from which the new
                  sheet should be created.

        :param wtsheet_name: the name of the sheet into which content
                       should be written.
        """
        self.next.sheet(rdsheet,wtsheet_name)
       
    def set_rdsheet(self,rdsheet):
        """
        This is only ever called by a filter that
        wishes to change the source of cells mid-way through writing a
        sheet.

        :param rdsheet: the :class:`~xlrd.sheet.Sheet` object from which cells from
                  this point forward should be read from.

        """
        self.next.set_rdsheet(rdsheet)
       
    def row(self,rdrowx,wtrowx):
        """
        This is called every time processing of a new
        row in the current sheet starts.
        It is primarily for copying row-based formatting from the
        source row to the target row.

        :param rdrowx: the index of the row in the current sheet from which
                 information for the row to be written should be
                 copied.

        :param wtrowx: the index of the row in sheet to be written to which
                 information should be written for the row being read.
        """
        self.next.row(rdrowx,wtrowx)

    def cell(self,rdrowx,rdcolx,wtrowx,wtcolx):
        """
        This is called for every cell in the sheet being processed.
        This is the most common method in which filtering and queuing
        of onward calls to the next filter takes place.

        :param rdrowx: the index of the row to be read from in the current sheet. 
        :param rdcolx: the index of the column to be read from in the current sheet. 
        :param wtrowx: the index of the row to be written to in the current output sheet. 
        :param wtcolx: the index of the column to be written to in the current output sheet. 
        """
        self.next.cell(rdrowx,rdcolx,wtrowx,wtcolx)

    def finish(self):
        """
        This method is called once processing of all workbooks has
        been completed.

        A filter should call this method on the next filter in the
        chain as an indication that no further calls will be made to
        any methods or that, if they are, any new calls should be
        treated as new batch of workbooks with no information retained
        from the previous batch.
        """
        self.next.finish()

class BaseFilter:
    """
    A concrete filter that implements pass-through behaviour
    for :class:`~xlutils.filter.BaseFilterInterface`.

    This often makes a great base class for your own filters.
    """

    all_methods = (
        'start',
        'workbook',
        'sheet',
        'set_rdsheet',
        'row',
        'cell',
        'finish',
        )

    def sheet(self,rdsheet,wtsheet_name):
        self.rdsheet = rdsheet
        self.next.sheet(rdsheet,wtsheet_name)
       
    def set_rdsheet(self,rdsheet):
        self.rdsheet = rdsheet
        self.next.set_rdsheet(rdsheet)

    def __getattr__(self,name):
        if name not in self.all_methods:
            raise AttributeError(name)
        actual = getattr(self.next,name)
        setattr(self,name,actual)
        return actual

class BaseWriter:
    """
    This is the base writer that copies all data and formatting from
    the specified sources.
    It is designed for sequential use so when, for example, writing
    two workbooks, the calls must be ordered as follows:
    
    - :meth:`workbook` call for first workbook
    - :meth:`sheet` call for first sheet
    - :meth:`row` call for first row
    - :meth:`cell` call for left-most cell of first row
    - :meth:`cell` call for second-left-most cell of first row
    - ...
    - :meth:`row` call for second row
    - ...
    - :meth:`sheet` call for second sheet
    - ...
    - :meth:`workbook` call for second workbook
    - ...
    - :meth:`finish` call

    Usually, only the :meth:`get_stream` method needs to be implemented in subclasses.
    """
    
    wtbook = None
    
    close_after_write = True

    def get_stream(self,filename):
        """
        This method is called once for each file written.
        The filename of the file to be created is passed and something with
        :meth:`~file.write` and :meth:`~file.close`
        methods that behave like a :class:`file` object's must be returned.
        """
        raise NotImplementedError

    def start(self):
        """
        This method should be called before processing of a batch of input.
        This allows the filter to initialise any required data
        structures and dispose of any existing state from previous
        batches. 
        """
        self.wtbook = None
        
    def close(self):
        if self.wtbook is not None:
            stream = self.get_stream(self.wtname)
            self.wtbook.save(stream)
            if self.close_after_write:
                stream.close()
            del self.wtbook
            del self.rdbook
            del self.rdsheet
            del self.wtsheet
            del self.style_list

    def workbook(self,rdbook,wtbook_name):
        """
        This method should be called every time processing of a new
        workbook starts.

        :param rdbook: the :class:`~xlrd.Book` object from which the new workbook
                 will be created.

        :param wtbook_name: the name of the workbook into which content
                      will be written.
        """
        self.close()        
        self.rdbook = rdbook
        self.wtbook = xlwt.Workbook(style_compression=2)
        self.wtbook.dates_1904 = rdbook.datemode
        self.wtname = wtbook_name
        self.style_list = []
        self.wtsheet_names = set()
        # the index of the current wtsheet being written
        self.wtsheet_index = 0
        # have we set a visible sheet already?
        self.sheet_visible = False
        if not rdbook.formatting_info:
            return
        for rdxf in rdbook.xf_list:
            wtxf = xlwt.Style.XFStyle()
            #
            # number format
            #
            wtxf.num_format_str = rdbook.format_map[rdxf.format_key].format_str
            #
            # font
            #
            wtf = wtxf.font
            rdf = rdbook.font_list[rdxf.font_index]
            wtf.height = rdf.height
            wtf.italic = rdf.italic
            wtf.struck_out = rdf.struck_out
            wtf.outline = rdf.outline
            wtf.shadow = rdf.outline
            wtf.colour_index = rdf.colour_index
            wtf.bold = rdf.bold #### This attribute is redundant, should be driven by weight
            wtf._weight = rdf.weight #### Why "private"?
            wtf.escapement = rdf.escapement
            wtf.underline = rdf.underline_type #### 
            # wtf.???? = rdf.underline #### redundant attribute, set on the fly when writing
            wtf.family = rdf.family
            wtf.charset = rdf.character_set
            wtf.name = rdf.name
            # 
            # protection
            #
            wtp = wtxf.protection
            rdp = rdxf.protection
            wtp.cell_locked = rdp.cell_locked
            wtp.formula_hidden = rdp.formula_hidden
            #
            # border(s) (rename ????)
            #
            wtb = wtxf.borders
            rdb = rdxf.border
            wtb.left   = rdb.left_line_style
            wtb.right  = rdb.right_line_style
            wtb.top    = rdb.top_line_style
            wtb.bottom = rdb.bottom_line_style 
            wtb.diag   = rdb.diag_line_style
            wtb.left_colour   = rdb.left_colour_index 
            wtb.right_colour  = rdb.right_colour_index 
            wtb.top_colour    = rdb.top_colour_index
            wtb.bottom_colour = rdb.bottom_colour_index 
            wtb.diag_colour   = rdb.diag_colour_index 
            wtb.need_diag1 = rdb.diag_down
            wtb.need_diag2 = rdb.diag_up
            #
            # background / pattern (rename???)
            #
            wtpat = wtxf.pattern
            rdbg = rdxf.background
            wtpat.pattern = rdbg.fill_pattern
            wtpat.pattern_fore_colour = rdbg.pattern_colour_index
            wtpat.pattern_back_colour = rdbg.background_colour_index
            #
            # alignment
            #
            wta = wtxf.alignment
            rda = rdxf.alignment
            wta.horz = rda.hor_align
            wta.vert = rda.vert_align
            wta.dire = rda.text_direction
            # wta.orie # orientation doesn't occur in BIFF8! Superceded by rotation ("rota").
            wta.rota = rda.rotation
            wta.wrap = rda.text_wrapped
            wta.shri = rda.shrink_to_fit
            wta.inde = rda.indent_level
            # wta.merg = ????
            #
            self.style_list.append(wtxf)
   
    def sheet(self,rdsheet,wtsheet_name):
        """
        This method should be called every time processing of a new
        sheet in the current workbook starts.

        :param rdsheet: the :class:`~xlrd.sheet.Sheet` object from which the new
                  sheet will be created.

        :param wtsheet_name: the name of the sheet into which content
                       will be written.
        """
        
        # these checks should really be done by xlwt!
        if not wtsheet_name:
            raise ValueError('Empty sheet name will result in invalid Excel file!')
        l_wtsheet_name = wtsheet_name.lower()
        if l_wtsheet_name in self.wtsheet_names:
            raise ValueError('A sheet named %r has already been added!'%l_wtsheet_name)
        self.wtsheet_names.add(l_wtsheet_name)
        l_wtsheet_name = len(wtsheet_name)
        if len(wtsheet_name)>31:
            raise ValueError('Sheet name cannot be more than 31 characters long, '
                             'supplied name was %i characters long!'%l_wtsheet_name)
        
        self.rdsheet = rdsheet
        self.wtsheet_name=wtsheet_name
        self.wtsheet = wtsheet = self.wtbook.add_sheet(wtsheet_name,cell_overwrite_ok=True)
        self.wtcols = set() # keep track of which columns have had their attributes set up
        #
        # MERGEDCELLS
        # 
        mc_map = {}
        mc_nfa = set()
        for crange in rdsheet.merged_cells:
            rlo, rhi, clo, chi = crange
            mc_map[(rlo, clo)] = crange
            for rowx in xrange(rlo, rhi):
                for colx in xrange(clo, chi):
                    mc_nfa.add((rowx, colx))
        self.merged_cell_top_left_map = mc_map
        self.merged_cell_already_set = mc_nfa
        if not rdsheet.formatting_info:
            return
        #
        # default column width: STANDARDWIDTH, DEFCOLWIDTH
        #
        if rdsheet.standardwidth is not None:
            # STANDARDWIDTH is expressed in units of 1/256 of a 
            # character-width, but DEFCOLWIDTH is expressed in units of
            # character-width; we lose precision by rounding to
            # the higher whole number of characters.
            #### XXXX TODO: implement STANDARDWIDTH record in xlwt.
            wtsheet.col_default_width = \
                (rdsheet.standardwidth + 255) // 256
        elif rdsheet.defcolwidth is not None:
            wtsheet.col_default_width = rdsheet.defcolwidth
        #
        # WINDOW2
        #
        wtsheet.show_formulas = rdsheet.show_formulas
        wtsheet.show_grid = rdsheet.show_grid_lines
        wtsheet.show_headers = rdsheet.show_sheet_headers
        wtsheet.panes_frozen = rdsheet.panes_are_frozen
        wtsheet.show_zero_values = rdsheet.show_zero_values
        wtsheet.auto_colour_grid = rdsheet.automatic_grid_line_colour
        wtsheet.cols_right_to_left = rdsheet.columns_from_right_to_left
        wtsheet.show_outline = rdsheet.show_outline_symbols
        wtsheet.remove_splits = rdsheet.remove_splits_if_pane_freeze_is_removed
        wtsheet.selected = rdsheet.sheet_selected
        # xlrd doesn't read WINDOW1 records, so we have to make a best
        # guess at which the active sheet should be:
        # (at a guess, only one sheet should be marked as visible)
        if not self.sheet_visible and rdsheet.sheet_visible:
            self.wtbook.active_sheet = self.wtsheet_index
            wtsheet.sheet_visible = 1
        self.wtsheet_index +=1
        
        wtsheet.page_preview = rdsheet.show_in_page_break_preview
        wtsheet.first_visible_row = rdsheet.first_visible_rowx
        wtsheet.first_visible_col = rdsheet.first_visible_colx
        wtsheet.grid_colour = rdsheet.gridline_colour_index
        wtsheet.preview_magn = rdsheet.cooked_page_break_preview_mag_factor
        wtsheet.normal_magn = rdsheet.cooked_normal_view_mag_factor
        #
        # DEFAULTROWHEIGHT
        #
        if rdsheet.default_row_height is not None:
            wtsheet.row_default_height =          rdsheet.default_row_height
        wtsheet.row_default_height_mismatch = rdsheet.default_row_height_mismatch
        wtsheet.row_default_hidden =          rdsheet.default_row_hidden
        wtsheet.row_default_space_above =     rdsheet.default_additional_space_above
        wtsheet.row_default_space_below =     rdsheet.default_additional_space_below
        #
        # BOUNDSHEET
        #
        wtsheet.visibility = rdsheet.visibility
       
        #
        # PANE
        #
        if rdsheet.has_pane_record:
            wtsheet.split_position_units_are_twips = True
            wtsheet.active_pane =              rdsheet.split_active_pane
            wtsheet.horz_split_pos =           rdsheet.horz_split_pos
            wtsheet.horz_split_first_visible = rdsheet.horz_split_first_visible
            wtsheet.vert_split_pos =           rdsheet.vert_split_pos
            wtsheet.vert_split_first_visible = rdsheet.vert_split_first_visible
            
    def set_rdsheet(self,rdsheet):
        """
        This should only ever called by a filter that
        wishes to change the source of cells mid-way through writing a
        sheet.

        :param rdsheet: the :class:`~xlrd.sheet.Sheet` object from which cells from
                  this point forward will be read.

        """
        self.rdsheet = rdsheet
        
    def row(self,rdrowx,wtrowx):
        """
        This should be called every time processing of a new
        row in the current sheet starts.

        :param rdrowx: the index of the row in the current sheet from which
                 information for the row to be written will be
                 copied.

        :param wtrowx: the index of the row in sheet to be written to which
                 information will be written for the row being read.
        """
        wtrow = self.wtsheet.row(wtrowx)
        # empty rows may not have a rowinfo record
        rdrow = self.rdsheet.rowinfo_map.get(rdrowx)
        if rdrow:
            wtrow.height = rdrow.height
            wtrow.has_default_height = rdrow.has_default_height
            wtrow.height_mismatch = rdrow.height_mismatch
            wtrow.level = rdrow.outline_level
            wtrow.collapse = rdrow.outline_group_starts_ends # No kiddin'
            wtrow.hidden = rdrow.hidden
            wtrow.space_above = rdrow.additional_space_above
            wtrow.space_below = rdrow.additional_space_below
            if rdrow.has_default_xf_index:
                wtrow.set_style(self.style_list[rdrow.xf_index])

    def cell(self,rdrowx,rdcolx,wtrowx,wtcolx):
        """
        This should be called for every cell in the sheet being processed.

        :param rdrowx: the index of the row to be read from in the current sheet. 
        :param rdcolx: the index of the column to be read from in the current sheet. 
        :param wtrowx: the index of the row to be written to in the current output sheet. 
        :param wtcolx: the index of the column to be written to in the current output sheet. 
        """
        cell = self.rdsheet.cell(rdrowx,rdcolx)
        # setup column attributes if not already set
        if wtcolx not in self.wtcols and rdcolx in self.rdsheet.colinfo_map:
            rdcol = self.rdsheet.colinfo_map[rdcolx]
            wtcol = self.wtsheet.col(wtcolx)
            wtcol.width = rdcol.width
            wtcol.set_style(self.style_list[rdcol.xf_index])
            wtcol.hidden = rdcol.hidden
            wtcol.level = rdcol.outline_level
            wtcol.collapsed = rdcol.collapsed
            self.wtcols.add(wtcolx)
        # copy cell
        cty = cell.ctype
        if cty == xlrd.XL_CELL_EMPTY:
            return
        if cell.xf_index is not None:
            style = self.style_list[cell.xf_index]
        else:
            style = default_style
        rdcoords2d = (rdrowx, rdcolx)
        if rdcoords2d in self.merged_cell_top_left_map:
            # The cell is the governing cell of a group of 
            # merged cells.
            rlo, rhi, clo, chi = self.merged_cell_top_left_map[rdcoords2d]
            assert (rlo, clo) == rdcoords2d
            self.wtsheet.write_merge(
                wtrowx, wtrowx + rhi - rlo - 1,
                wtcolx, wtcolx + chi - clo - 1, 
                cell.value, style)
            return
        if rdcoords2d in self.merged_cell_already_set:
            # The cell is in a group of merged cells.
            # It has been handled by the write_merge() call above.
            # We avoid writing a record again because:
            # (1) It's a waste of CPU time and disk space.
            # (2) xlwt does not (as at 2007-01-12) ensure that only
            # the last record is written to the file.
            # (3) If you write a data record for a cell
            # followed by a blank record for the same cell,
            # Excel will display a blank but OOo Calc and
            # Gnumeric will display the data :-(
            return
        wtrow = self.wtsheet.row(wtrowx)
        if cty == xlrd.XL_CELL_TEXT:
            wtrow.set_cell_text(wtcolx, cell.value, style)
        elif cty == xlrd.XL_CELL_NUMBER or cty == xlrd.XL_CELL_DATE:
            wtrow.set_cell_number(wtcolx, cell.value, style)
        elif cty == xlrd.XL_CELL_BLANK:
            wtrow.set_cell_blank(wtcolx, style)
        elif cty == xlrd.XL_CELL_BOOLEAN:
            wtrow.set_cell_boolean(wtcolx, cell.value, style)
        elif cty == xlrd.XL_CELL_ERROR:
            wtrow.set_cell_error(wtcolx, cell.value, style)
        else:
            raise Exception(
                "Unknown xlrd cell type %r with value %r at (sheet=%r,rowx=%r,colx=%r)" \
                % (cty, cell.value, self.rdsheet.name, rdrowx, rdcolx)
                )

    def finish(self):
        """
        This method should be called once processing of all workbooks has
        been completed.
        """
        self.close()

    
class GlobReader(BaseReader):
    "A reader that emits events for all files that match the glob in the spec."

    def __init__(self,spec):
        self.spec = spec
        
    def get_filepaths(self):
        return sorted(glob(self.spec))

class XLRDReader(BaseReader):
    "A reader that uses an in-memory :class:`xlrd.Book` object as its source of events."

    def __init__(self,wb,filename):
        self.wb = wb
        self.filename = filename
        
    def get_workbooks(self):
        "Yield the workbook passed during instantiation."
        yield (self.wb,self.filename)

class DirectoryWriter(BaseWriter):
    "A writer that stores files in a filesystem directory"

    def __init__(self,path):
        self.dir_path = path
        
    def get_stream(self,filename):
        """
        Returns a stream for the file in the configured directory
        with the specified name.
        """
        return open(os.path.join(self.dir_path, filename), 'wb')

class StreamWriter(BaseWriter):
    "A writer for writing exactly one workbook to the supplied stream"

    fired = False
    close_after_write = False
    
    def __init__(self,stream):
        self.stream = stream
        
    def get_stream(self,filename):
        "Returns the stream passed during instantiation."
        if self.fired:
            raise Exception('Attempt to write more than one workbook')
        self.fired = True
        return self.stream

class XLWTWriter(BaseWriter):
    "A writer that writes to a sequence of in-memory :class:`xlwt.Workbook` objects."

    def __init__(self):
        self.output = []

    def close(self):
        if self.wtbook is not None:
            self.output.append((self.wtname,self.wtbook))
            del self.wtbook

class MethodFilter(BaseFilter):
    """
    This is a base class that implements functionality for filters
    that want to do a common task such as logging, printing or memory
    usage recording on certain calls configured at filter instantiation
    time.

    :ref:`echo` is an example of this.
    """
    
    def method(self,name,*args):
        """
        This is the method that needs to be implemented.
        It is called with the name of the method that was called on
        the MethodFilter and the arguments that were passed to that
        method. 
        """
        raise NotImplementedError
    
    def __init__(self,methods=True):
        if methods==True or methods=='True' or (len(methods)==1 and methods[0]=='True'):
            methods = self.all_methods
        for name in methods:
            if name not in self.all_methods:
                raise ValueError('%r is not a valid method name'%(name,))
            setattr(self,name,partial(self.caller,name))

    def caller(self,name,*args):
        self.method(name,*args)
        getattr(self.next,name)(*args)

class Echo(MethodFilter):
    """
    This filter will print calls to the methods configured when the
    filter is created along with the arguments passed.

    For more details, see the :ref:`documentation <echo>`.
    """

    def __init__(self,name=None,methods=True):
        MethodFilter.__init__(self,methods)
        self.name = name

    def method(self,name,*args):
        if self.name:
            print(repr(self.name), end=' ')
        print("%s:%r" % (name, args))
        
try:
    from guppy import hpy
    guppy = True
except ImportError:
    guppy = False
    
class MemoryLogger(MethodFilter):
    """
    This filter will dump stats to the path it was configured with using
    the heapy package if it is available.
    """

    def __init__(self,path,methods=True):
        MethodFilter.__init__(self,methods)
        self.path = path
        
    def method(self,name,*args):
        if guppy:
            # We instantiate the heapy environment here
            # so that the memory it consumes doesn't hang
            # around for the whole process
            hpy().heap().stat.dump(self.path)
        else:
            logger.error('guppy is not availabe, cannot log memory usage!')
            
            
class ErrorFilter(BaseReader,BaseWriter):
    """
    A filter that gates downstream writers or filters on whether
    or not any errors have occurred.

    See :ref:`error-filters` for details.
    """
    temp_path = None
    
    def __init__(self,level=logging.ERROR,message='No output as errors have occurred.'):
        from errorhandler import ErrorHandler
        self.handler = ErrorHandler(level)
        self.message = message

    def start(self,create=True):
        self.prefix = 0
        self.handler.reset()
        if self.temp_path is not None:
            rmtree(self.temp_path)
        if create:
            self.temp_path = mkdtemp()
        else:
            self.temp_path = None
        BaseWriter.start(self)
        
    def get_stream(self,filename):
        self.prefix+=1
        return open(os.path.join(self.temp_path,str(self.prefix)+'-'+filename),'wb')

    def get_workbooks(self):
        if self.temp_path is None:
            return
        filenames = []
        for name in os.listdir(self.temp_path):
            d = name.split('-',1)
            d.append(name)
            filenames.append(d)
        filenames.sort()
        for i,filename,pathname in filenames:
            yield (
                # We currently don't open with on_demand=True here
                # as error filters should be lastish in the chain
                # so there's not much win.
                # However, if we did, getting rid of the temp dirs
                # becomes a problem as, on Windows, they can't be
                # deleted until the xlrd.Book object is done with
                # and we don't know when that might be :-(
                xlrd.open_workbook(
                    os.path.join(self.temp_path,pathname),
                    formatting_info=1,
                    on_demand=False,
                    ragged_rows=True
                    ),
                filename
                )

    def sheet(self,rdsheet,wtsheet_name):
        self.rdsheet = rdsheet
        BaseWriter.sheet(self,rdsheet,wtsheet_name)

    def cell(self,rdrowx,rdcolx,wtrowx,wtcolx):
        cell = self.rdsheet.cell(rdrowx,rdcolx)
        if cell.ctype == xlrd.XL_CELL_EMPTY:
            return
        if cell.ctype == xlrd.XL_CELL_ERROR:
            logger.error("Cell %s of sheet %r contains a bad value: %s" % (
                        xlrd.cellname(rdrowx, rdcolx),
                        quoted_sheet_name(self.rdsheet.name),
                        cell_display(cell,self.rdbook.datemode),
                        ))
            return
        BaseWriter.cell(self,rdrowx,rdcolx,wtrowx,wtcolx)

    def finish(self):
        """
        The method that triggers downstream filters and writers
        if no errors have occurred.
        """
        BaseWriter.finish(self)
        if self.handler.fired:
            logger.error(self.message)
        else:
            self(self.next)
        self.start(create=False)
        for attr in ('rdbook','rdsheet'):
            if hasattr(self,attr):
                delattr(self,attr)

class Range(object):
    __slots__ = ('rsn','rr','rc','wr','wc','r','c')
    def __init__(self,rsn,rr,rc,wr,wc):
        self.rsn = rsn
        self.rr = rr
        self.rc = rc
        self.wr = wr
        self.wc = wc
        self.r = self.c = 1
    def __repr__(self):
        return '<range:%r:(%i,%i)->(%i,%i)-r:%i,c:%i>' % (
            self.rsn,self.rr,self.rc,self.wr,self.wc,self.r,self.c
            )

class ColumnTrimmer(BaseFilter):
    """
    This filter will strip columns containing no useful data from the
    end of sheets.

    See the :ref:`column_trimmer` documentation for an example.
    """

    def __init__(self,is_junk=None):
        self.is_junk = is_junk
        
    def start(self,chain=True):
        self.rdsheet = None
        self.pending_rdsheet = None
        self.ranges = []
        self.max_nonjunk = 0
        self.max = 0
        if chain:
            self.next.start()
        
    def flush(self):
        if self.rdsheet is not None:
            rsn = None
            for ra in self.ranges:
                if rsn is None:
                    rsn = ra.rsn
                elif ra.rsn!=rsn:
                    self.rdsheet = self.rdbook.sheet_by_name(ra.rsn)
                    self.next.set_rdsheet(self.rdsheet)
                    rsn = ra.rsn
                for r in range(ra.r):
                    for c in range(ra.c):
                        wtcolx=ra.wc+c
                        if wtcolx<=self.max_nonjunk:
                            self.next.cell(ra.rr+r,ra.rc+c,ra.wr+r,ra.wc+c)
            if self.max!=self.max_nonjunk:
                logger.debug("Number of columns trimmed from %d to %d for sheet %r",
                             self.max+1,
                             self.max_nonjunk+1,
                             quoted_sheet_name(self.wtsheet_name))
        self.start(chain=False)

    def workbook(self,rdbook,wtbook_name):
        self.rdbook = rdbook
        self.flush()
        self.next.workbook(rdbook,wtbook_name)
        
    def sheet(self,rdsheet,wtsheet_name):
        self.flush()
        self.rdsheet = rdsheet
        self.wtsheet_name = wtsheet_name
        self.next.sheet(self.rdsheet,wtsheet_name)
        
    def set_rdsheet(self,rdsheet):
        self.pending_rdsheet = rdsheet
        self.rdsheet = rdsheet
    
    def add_range(self,rdrowx,rdcolx,wtrowx,wtcolx):
        if len(self.ranges)>1:
            to_collapse = self.ranges[-1]
            possible = self.ranges[-2]
            if to_collapse.rc==possible.rc and \
               to_collapse.c==possible.c and \
               to_collapse.rr==possible.rr+possible.r:
                possible.r+=to_collapse.r
                self.ranges.pop()
        self.ranges.append(Range(
                self.rdsheet.name,rdrowx,rdcolx,wtrowx,wtcolx
                ))
        
    def cell(self,rdrowx,rdcolx,wtrowx,wtcolx):
        if wtcolx>self.max:
            self.max = wtcolx
        cell = self.rdsheet.cell(rdrowx,rdcolx)
        if wtcolx>self.max_nonjunk and not cells_all_junk((cell,),self.is_junk):
            self.max_nonjunk = wtcolx
        if not self.ranges:
            self.add_range(rdrowx,rdcolx,wtrowx,wtcolx)
        elif self.pending_rdsheet is not None: 
            self.add_range(rdrowx,rdcolx,wtrowx,wtcolx)
            self.pending_rdsheet = None
        else:
            r = self.ranges[-1]
            if rdrowx==r.rr and wtrowx==r.wr and rdcolx==r.rc+r.c and wtcolx==r.wc+r.c:
                r.c+=1
            else:
                self.add_range(rdrowx,rdcolx,wtrowx,wtcolx)
                                                          
    def finish(self):
        self.flush()
        del self.rdbook
        self.next.finish()
        
def process(reader, *chain):
    """
    The driver function for the :mod:`xlutils.filter` module.

    It takes a chain of one :ref:`reader <reader>`, followed by zero or more
    :ref:`filters <filter>` and ending with one :ref:`writer <writer>`.

    All the components are chained together by the :func:`process` function
    setting their ``next`` attributes appropriately. The
    :ref:`reader <reader>` is then called with the first
    :ref:`filter <filter>` in the chain.
    """
    for i in range(len(chain)-1):
        chain[i].next = chain[i+1]
    reader(chain[0])
