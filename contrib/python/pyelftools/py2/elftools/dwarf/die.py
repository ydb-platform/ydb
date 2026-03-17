#-------------------------------------------------------------------------------
# elftools: dwarf/die.py
#
# DWARF Debugging Information Entry
#
# Eli Bendersky (eliben@gmail.com)
# This code is in the public domain
#-------------------------------------------------------------------------------
from collections import namedtuple, OrderedDict
import os

from ..common.exceptions import DWARFError
from ..common.py3compat import bytes2str, iteritems
from ..common.utils import struct_parse, preserve_stream_pos
from .enums import DW_FORM_raw2name
from .dwarf_util import _resolve_via_offset_table, _get_base_offset


# AttributeValue - describes an attribute value in the DIE:
#
# name:
#   The name (DW_AT_*) of this attribute
#
# form:
#   The DW_FORM_* name of this attribute
#
# value:
#   The value parsed from the section and translated accordingly to the form
#   (e.g. for a DW_FORM_strp it's the actual string taken from the string table)
#
# raw_value:
#   Raw value as parsed from the section - used for debugging and presentation
#   (e.g. for a DW_FORM_strp it's the raw string offset into the table)
#
# offset:
#   Offset of this attribute's value in the stream (absolute offset, relative
#   the beginning of the whole stream)
#
AttributeValue = namedtuple(
    'AttributeValue', 'name form value raw_value offset')


class DIE(object):
    """ A DWARF debugging information entry. On creation, parses itself from
        the stream. Each DIE is held by a CU.

        Accessible attributes:

            tag:
                The DIE tag

            size:
                The size this DIE occupies in the section

            offset:
                The offset of this DIE in the stream

            attributes:
                An ordered dictionary mapping attribute names to values. It's
                ordered to preserve the order of attributes in the section

            has_children:
                Specifies whether this DIE has children

            abbrev_code:
                The abbreviation code pointing to an abbreviation entry (note
                that this is for informational purposes only - this object
                interacts with its abbreviation table transparently).

        See also the public methods.
    """
    def __init__(self, cu, stream, offset):
        """ cu:
                CompileUnit object this DIE belongs to. Used to obtain context
                information (structs, abbrev table, etc.)

            stream, offset:
                The stream and offset into it where this DIE's data is located
        """
        self.cu = cu
        self.dwarfinfo = self.cu.dwarfinfo # get DWARFInfo context
        self.stream = stream
        self.offset = offset

        self.attributes = OrderedDict()
        self.tag = None
        self.has_children = None
        self.abbrev_code = None
        self.size = 0
        # Null DIE terminator. It can be used to obtain offset range occupied
        # by this DIE including its whole subtree.
        self._terminator = None
        self._parent = None

        self._parse_DIE()

    def is_null(self):
        """ Is this a null entry?
        """
        return self.tag is None

    def get_DIE_from_attribute(self, name):
        """ Return the DIE referenced by the named attribute of this DIE.
            The attribute must be in the reference attribute class.

            name:
                The name of the attribute in the reference class.
        """
        attr = self.attributes[name]
        if attr.form in ('DW_FORM_ref1', 'DW_FORM_ref2', 'DW_FORM_ref4',
                         'DW_FORM_ref8', 'DW_FORM_ref', 'DW_FORM_ref_udata'):
            refaddr = self.cu.cu_offset + attr.raw_value
            return self.cu.get_DIE_from_refaddr(refaddr)
        elif attr.form in ('DW_FORM_ref_addr'):
            return self.cu.dwarfinfo.get_DIE_from_refaddr(attr.raw_value)
        elif attr.form in ('DW_FORM_ref_sig8'):
            # Implement search type units for matching signature
            raise NotImplementedError('%s (type unit by signature)' % attr.form)
        elif attr.form in ('DW_FORM_ref_sup4', 'DW_FORM_ref_sup8', 'DW_FORM_GNU_ref_alt'):
            if self.dwarfinfo.supplementary_dwarfinfo:
                return self.dwarfinfo.supplementary_dwarfinfo.get_DIE_from_refaddr(attr.raw_value)
            # FIXME: how to distinguish supplementary files from dwo ?
            raise NotImplementedError('%s to dwo' % attr.form)
        else:
            raise DWARFError('%s is not a reference class form attribute' % attr)

    def get_parent(self):
        """ Return the parent DIE of this DIE, or None if the DIE has no
            parent (i.e. is a top-level DIE).
        """
        if self._parent is None:
            self._search_ancestor_offspring()
        return self._parent

    def get_full_path(self):
        """ Return the full path filename for the DIE.

            The filename is the join of 'DW_AT_comp_dir' and 'DW_AT_name',
            either of which may be missing in practice. Note that its value is
            usually a string taken from the .debug_string section and the
            returned value will be a string.
        """
        comp_dir_attr = self.attributes.get('DW_AT_comp_dir', None)
        comp_dir = bytes2str(comp_dir_attr.value) if comp_dir_attr else ''
        fname_attr = self.attributes.get('DW_AT_name', None)
        fname = bytes2str(fname_attr.value) if fname_attr else ''
        return os.path.join(comp_dir, fname)

    def iter_children(self):
        """ Iterates all children of this DIE
        """
        return self.cu.iter_DIE_children(self)

    def iter_siblings(self):
        """ Yield all siblings of this DIE
        """
        parent = self.get_parent()
        if parent:
            for sibling in parent.iter_children():
                if sibling is not self:
                    yield sibling
        else:
            raise StopIteration()

    # The following methods are used while creating the DIE and should not be
    # interesting to consumers
    #

    def set_parent(self, die):
        self._parent = die

    #------ PRIVATE ------#

    def _search_ancestor_offspring(self):
        """ Search our ancestors identifying their offspring to find our parent.

            DIEs are stored as a flattened tree.  The top DIE is the ancestor
            of all DIEs in the unit.  Each parent is guaranteed to be at
            an offset less than their children.  In each generation of children
            the sibling with the closest offset not greater than our offset is
            our ancestor.
        """
        # This code is called when get_parent notices that the _parent has
        # not been identified.  To avoid execution for each sibling record all
        # the children of any parent iterated.  Assuming get_parent will also be
        # called for siblings, it is more efficient if siblings references are
        # provided and no worse than a single walk if they are missing, while
        # stopping iteration early could result in O(n^2) walks.
        search = self.cu.get_top_DIE()
        while search.offset < self.offset:
            prev = search
            for child in search.iter_children():
                child.set_parent(search)
                if child.offset <= self.offset:
                    prev = child

            # We also need to check the offset of the terminator DIE
            if search.has_children and search._terminator.offset <= self.offset:
                    prev = search._terminator

            # If we didn't find a closer parent, give up, don't loop.
            # Either we mis-parsed an ancestor or someone created a DIE
            # by an offset that was not actually the start of a DIE.
            if prev is search:
                raise ValueError("offset %s not in CU %s DIE tree" %
                    (self.offset, self.cu.cu_offset))

            search = prev

    def __repr__(self):
        s = 'DIE %s, size=%s, has_children=%s\n' % (
            self.tag, self.size, self.has_children)
        for attrname, attrval in iteritems(self.attributes):
            s += '    |%-18s:  %s\n' % (attrname, attrval)
        return s

    def __str__(self):
        return self.__repr__()

    def _parse_DIE(self):
        """ Parses the DIE info from the section, based on the abbreviation
            table of the CU
        """
        structs = self.cu.structs

        # A DIE begins with the abbreviation code. Read it and use it to
        # obtain the abbrev declaration for this DIE.
        # Note: here and elsewhere, preserve_stream_pos is used on operations
        # that manipulate the stream by reading data from it.
        self.abbrev_code = struct_parse(
            structs.Dwarf_uleb128(''), self.stream, self.offset)

        # This may be a null entry
        if self.abbrev_code == 0:
            self.size = self.stream.tell() - self.offset
            return

        abbrev_decl = self.cu.get_abbrev_table().get_abbrev(self.abbrev_code)
        self.tag = abbrev_decl['tag']
        self.has_children = abbrev_decl.has_children()

        # Guided by the attributes listed in the abbreviation declaration, parse
        # values from the stream.
        for spec in abbrev_decl['attr_spec']:
            form = spec.form
            name = spec.name
            attr_offset = self.stream.tell()
            # Special case here: the attribute value is stored in the attribute
            # definition in the abbreviation spec, not in the DIE itself.
            if form == 'DW_FORM_implicit_const':
                value = spec.value
                raw_value = value
            else:
                raw_value = struct_parse(structs.Dwarf_dw_form[form], self.stream)
                value = self._translate_attr_value(form, raw_value)
            self.attributes[name] = AttributeValue(
                name=name,
                form=form,
                value=value,
                raw_value=raw_value,
                offset=attr_offset)

        self.size = self.stream.tell() - self.offset

    def _translate_attr_value(self, form, raw_value):
        """ Translate a raw attr value according to the form
        """
        # Indirect forms can only be parsed if the top DIE of this CU has already been parsed
        # and listed in the CU, since the top DIE would have to contain the DW_AT_xxx_base attributes.
        # This breaks if there is an indirect encoding in the top DIE itself before the
        # corresponding _base, and it was seen in the wild.
        # There is a hook in get_top_DIE() to resolve those lazily.
        translate_indirect = self.cu.has_top_DIE() or self.offset != self.cu.cu_die_offset        
        value = None
        if form == 'DW_FORM_strp':
            with preserve_stream_pos(self.stream):
                value = self.dwarfinfo.get_string_from_table(raw_value)
        elif form == 'DW_FORM_line_strp':
            with preserve_stream_pos(self.stream):
                value = self.dwarfinfo.get_string_from_linetable(raw_value)
        elif form in ('DW_FORM_GNU_strp_alt', 'DW_FORM_strp_sup'):
            if self.dwarfinfo.supplementary_dwarfinfo:
                return self.dwarfinfo.supplementary_dwarfinfo.get_string_from_table(raw_value)
            else:
                value = raw_value
        elif form == 'DW_FORM_flag':
            value = not raw_value == 0
        elif form == 'DW_FORM_flag_present':
            value = True
        elif form == 'DW_FORM_indirect':
            try:
                form = DW_FORM_raw2name[raw_value]
            except KeyError as err:
                raise DWARFError(
                        'Found DW_FORM_indirect with unknown raw_value=' +
                        str(raw_value))

            raw_value = struct_parse(
                self.cu.structs.Dwarf_dw_form[form], self.stream)
            # Let's hope this doesn't get too deep :-)
            return self._translate_attr_value(form, raw_value)
        elif form in ('DW_FORM_addrx', 'DW_FORM_addrx1', 'DW_FORM_addrx2', 'DW_FORM_addrx3', 'DW_FORM_addrx4') and translate_indirect:
            value = self.cu.dwarfinfo.get_addr(self.cu, raw_value)
        elif form in ('DW_FORM_strx', 'DW_FORM_strx1', 'DW_FORM_strx2', 'DW_FORM_strx3', 'DW_FORM_strx4') and translate_indirect:
            stream = self.dwarfinfo.debug_str_offsets_sec.stream
            base_offset = _get_base_offset(self.cu, 'DW_AT_str_offsets_base')
            offset_size = 4 if self.cu.structs.dwarf_format == 32 else 8
            with preserve_stream_pos(stream):
                str_offset = struct_parse(self.cu.structs.Dwarf_offset(''), stream, base_offset + raw_value*offset_size)
            value = self.dwarfinfo.get_string_from_table(str_offset)
        elif form == 'DW_FORM_loclistx' and translate_indirect:
            value = _resolve_via_offset_table(self.dwarfinfo.debug_loclists_sec.stream, self.cu, raw_value, 'DW_AT_loclists_base')
        elif form == 'DW_FORM_rnglistx' and translate_indirect:
            value = _resolve_via_offset_table(self.dwarfinfo.debug_rnglists_sec.stream, self.cu, raw_value, 'DW_AT_rnglists_base')
        else:
            value = raw_value
        return value

    def _translate_indirect_attributes(self):
        """ This is a hook to translate the DW_FORM_...x values in the top DIE
            once the top DIE is parsed to the end. They can't be translated 
            while the top DIE is being parsed, because they implicitly make a
            reference to the DW_AT_xxx_base attribute in the same DIE that may
            not have been parsed yet.
        """
        for key in self.attributes:
            attr = self.attributes[key]
            if attr.form in ('DW_FORM_strx', 'DW_FORM_strx1', 'DW_FORM_strx2', 'DW_FORM_strx3', 'DW_FORM_strx4',
                'DW_FORM_addrx', 'DW_FORM_addrx1', 'DW_FORM_addrx2', 'DW_FORM_addrx3', 'DW_FORM_addrx4',
                'DW_FORM_loclistx', 'DW_FORM_rnglistx'):
                # Can't change value in place, got to replace the whole attribute record
                self.attributes[key] = AttributeValue(
                    name=attr.name,
                    form=attr.form,
                    value=self._translate_attr_value(attr.form, attr.raw_value),
                    raw_value=attr.raw_value,
                    offset=attr.offset)        
