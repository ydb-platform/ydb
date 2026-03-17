"""
Utilities for writing PDF files.
Contains code from the PyPDF2 project; see :ref:`here <pypdf2-license>`
for the original license.
"""

import logging
import os
import typing
from typing import Dict, Iterable, List, Optional, Set, Tuple, Union, cast

from asn1crypto import x509

from pyhanko.pdf_utils import generic
from pyhanko.pdf_utils.crypt import (
    PubKeySecurityHandler,
    SecurityHandler,
    StandardSecurityHandler,
)
from pyhanko.pdf_utils.extensions import (
    DeveloperExtension,
    DevExtensionMultivalued,
)
from pyhanko.pdf_utils.generic import pdf_name
from pyhanko.pdf_utils.metadata.info import (
    update_info_dict,
    view_from_info_dict,
)
from pyhanko.pdf_utils.metadata.model import DocumentMetadata
from pyhanko.pdf_utils.misc import (
    IndirectObjectExpected,
    PdfError,
    PdfReadError,
    PdfWriteError,
    instance_test,
)
from pyhanko.pdf_utils.rw_common import PdfHandler
from pyhanko.pdf_utils.xref import (
    OBJSTREAM_FORBIDDEN,
    ObjectStream,
    PositionDict,
    XRefStream,
    write_xref_table,
)

if typing.TYPE_CHECKING:
    from .font.api import FontSubsetCollection

__all__ = [
    'BasePdfFileWriter',
    'PageObject',
    'PdfFileWriter',
    'init_xobject_dictionary',
    'copy_into_new_writer',
]

logger = logging.getLogger(__name__)


# TODO move this to content.py?
def init_xobject_dictionary(
    command_stream: bytes,
    box_width,
    box_height,
    resources: Optional[generic.DictionaryObject] = None,
) -> generic.StreamObject:
    """
    Helper function to initialise form XObject dictionaries.

    .. note::
        For utilities to handle image XObjects, see :mod:`.images`.

    :param command_stream:
        The XObject's raw appearance stream.
    :param box_width:
        The width of the XObject's bounding box.
    :param box_height:
        The height of the XObject's bounding box.
    :param resources:
        A resource dictionary to include with the form object.
    :return:
        A :class:`~.generic.StreamObject` representation of the form XObject.
    """
    resources = resources or generic.DictionaryObject()
    return generic.StreamObject(
        {
            pdf_name('/BBox'): generic.ArrayObject(
                list(
                    map(generic.FloatObject, (0.0, box_height, box_width, 0.0))
                )
            ),
            pdf_name('/Resources'): resources,
            pdf_name('/Type'): pdf_name('/XObject'),
            pdf_name('/Subtype'): pdf_name('/Form'),
        },
        stream_data=command_stream,
    )


class BasePdfFileWriter(PdfHandler):
    """Base class for PDF writers."""

    output_version = (1, 7)
    """Output version to be declared in the output file."""

    stream_xrefs: bool
    """
    Boolean controlling whether or not the output file will contain 
    its cross-references in stream format, or as a classical XRef table.
    
    The default for new files is ``True``. For incremental updates,
    the writer adapts to the system used in the previous iteration of the
    document (as mandated by the standard).
    """

    def __init__(
        self,
        root: Union[generic.IndirectObject, generic.DictionaryObject],
        info: Union[generic.IndirectObject, generic.DictionaryObject, None],
        document_id: generic.ArrayObject,
        obj_id_start: int = 0,
        stream_xrefs: bool = True,
    ):
        self.objects: Dict[Tuple[int, int], generic.PdfObject] = {}
        self.object_streams: List[ObjectStream] = list()
        self.objs_in_streams: Dict[int, generic.PdfObject] = {}
        self._lastobj_id = obj_id_start
        self._resolves_objs_from: Iterable[PdfHandler] = (self,)
        self._allocated_placeholders: Set[int] = set()

        if isinstance(root, generic.IndirectObject):
            self._root = root
        else:
            self._root = self.add_object(root)

        self.security_handler: Optional[SecurityHandler] = None
        self._encrypt: Optional[generic.IndirectObject] = None
        self._encrypt_key: Optional[bytes] = None
        self._document_id = document_id
        self.stream_xrefs = stream_xrefs
        info_ref = None
        if info is not None:
            if not isinstance(info, generic.IndirectObject):
                info_ref = self.add_object(info)
            else:
                info_ref = generic.IndirectObject(
                    info.idnum, info.generation, self
                )
        self._info = info_ref
        self._meta: DocumentMetadata = DocumentMetadata()

        self._font_resources: Dict[str, 'FontSubsetCollection'] = {}
        self.digest_aware_write = False
        self._mac_handler_cls = None

    def get_subset_collection(self, base_postscript_name: str):
        from .font.api import FontSubsetCollection

        try:
            fsc = self._font_resources[base_postscript_name]
        except KeyError:
            fsc = FontSubsetCollection(base_postscript_name)
            self._font_resources[base_postscript_name] = fsc

        return fsc

    @property
    def document_meta(self) -> DocumentMetadata:
        return self._meta

    @property
    def document_meta_view(self) -> DocumentMetadata:
        # we need the view_over (not just a copy) because
        # e.g. copy_into_new_writer will populate the info dict with
        # base values that can then be "cleanly" overridden in the
        # high-level API without destroying any unsupported entries
        # in the original info dict
        if self._info:
            base = view_from_info_dict(self._info.get_object())
        else:
            base = DocumentMetadata()
        return self._meta.view_over(base)

    def ensure_output_version(self, version):
        if self.output_version < version:
            self.output_version = version

    def set_info(
        self,
        info: Union[generic.IndirectObject, generic.DictionaryObject, None],
    ) -> Optional[generic.IndirectObject]:
        """
        Set the ``/Info`` entry of the document trailer.

        :param info:
            The new ``/Info`` dictionary, as an indirect reference.
        """
        new_info: Optional[generic.IndirectObject]
        if info is not None and not isinstance(info, generic.IndirectObject):
            self._info = new_info = self.add_object(info)
        else:
            self._info = new_info = info
        return new_info

    def set_custom_trailer_entry(
        self, key: generic.NameObject, value: generic.PdfObject
    ):
        """
        Set a custom, unmanaged entry in the document trailer or cross-reference
        stream dictionary.

        .. warning::
            Calling this method to set an entry that is managed by pyHanko
            internally (info dictionary, document catalog, etc.) has undefined
            results.

        :param key:
            Dictionary key to use in the trailer.
        :param value:
            Value to set
        """
        raise NotImplementedError

    @property
    def document_id(self) -> Tuple[bytes, bytes]:
        id_arr = self._document_id
        return id_arr[0].original_bytes, id_arr[1].original_bytes

    def mark_update(
        self, obj_ref: Union[generic.Reference, generic.IndirectObject]
    ):
        """
        Mark an object reference to be updated.
        This is only relevant for incremental updates, but is included
        as a no-op by default for interoperability reasons.

        :param obj_ref:
            An indirect object instance or a reference.
        """
        pass

    def update_container(self, obj: generic.PdfObject):
        """
        Mark the container of an object (as indicated by the
        :attr:`~.generic.PdfObject.container_ref` attribute on
        :class:`~.generic.PdfObject`) for an update.

        As with :meth:`mark_update`, this only applies to incremental updates,
        but defaults to a no-op.

        :param obj:
            The object whose top-level container needs to be rewritten.
        """
        pass

    @property
    def root_ref(self) -> generic.Reference:
        """
        :return:
            A reference to the document catalog.
        """
        return self._root.reference

    def update_root(self):
        """
        Signal that the document catalog should be written to the output.
        Equivalent to calling :meth:`mark_update` with :attr:`root_ref`.
        """
        pass

    def register_extension(self, ext: DeveloperExtension):
        try:
            extensions = self.root['/Extensions']
        except KeyError:
            self.root['/Extensions'] = extensions = generic.DictionaryObject()

        # check if the extension is already registered,
        try:
            cur_ext_value = extensions.raw_get(
                ext.prefix_name, decrypt=generic.EncryptedObjAccess.RAW
            )
        except KeyError:
            cur_ext_value = None
        extension_dicts: Iterable[generic.PdfObject] = ()
        old_ext_multivalued = False
        if isinstance(cur_ext_value, generic.DictionaryObject):
            extension_dicts = (cur_ext_value,)
        elif isinstance(cur_ext_value, generic.ArrayObject):
            extension_dicts = tuple(cur_ext_value)
            old_ext_multivalued = True
        elif cur_ext_value is not None:
            cls_name = type(cur_ext_value).__name__
            raise PdfReadError(
                f"PDF extension value is of type {cls_name}, "
                f"expected (direct) PDF dictionary or array."
            )

        # investigate the relationship between the new extension's extension
        # level and that of any existing extension dictionaries with the same
        # prefix
        for ix, ext_dict in enumerate(extension_dicts):
            if not isinstance(ext_dict, generic.DictionaryObject):
                cls_name = type(ext_dict).__name__
                raise PdfReadError(
                    f"PDF extension array entry is of type "
                    f"{cls_name}, expected (direct) PDF dictionary"
                )
            try:
                lvl = int(ext_dict.raw_get('/ExtensionLevel'))
            except (TypeError, ValueError, KeyError):
                raise PdfReadError("Could not read developer extension level")
            # TODO is this still appropriate with the new ExtensionRevision
            #  values?
            if lvl == ext.extension_level:
                return  # nothing to do
            elif ext.compare_by_level:
                old_ext_applies = lvl >= ext.extension_level
                replace_old = not old_ext_applies
            else:
                # this covers the case where there's no obvious comparison
                old_ext_applies = lvl in ext.subsumed_by
                replace_old = lvl in ext.subsumes
            if old_ext_applies:
                return  # nothing to do, old extension doesn't need replacing
            elif replace_old:
                # New extension takes priority,
                # replace the old extension
                if old_ext_multivalued:
                    # it was an array, so replace the value at the proper
                    # index
                    cur_ext_value[ix] = ext.as_pdf_object()
                elif ext.multivalued == DevExtensionMultivalued.ALWAYS:
                    # extension was previously not multivalued, but we now
                    # make it so
                    extensions[ext.prefix_name] = generic.ArrayObject(
                        [ext.as_pdf_object()]
                    )
                else:
                    extensions[ext.prefix_name] = ext.as_pdf_object()
                self.update_container(extensions)
                return  # we're done here
            elif ext.multivalued == DevExtensionMultivalued.NEVER:
                # there is a matching extension in the file, but we can't
                # replace it -> error
                raise PdfWriteError(
                    f"Could not register extension with prefix "
                    f"{ext.prefix_name} and level {ext.extension_level}; "
                    f"file contains extension with same prefix and "
                    f"extension level {lvl}. If this extension level is "
                    f"safe to override, mark it as subsumed."
                )
        # if we got here, we're going to add the new extension without
        # replacing any existing ones
        ext_dict = ext.as_pdf_object()
        if cur_ext_value is None:
            # nothing there yet, just write the extension
            if ext.multivalued == DevExtensionMultivalued.ALWAYS:
                extensions[ext.prefix_name] = generic.ArrayObject([ext_dict])
            else:
                extensions[ext.prefix_name] = ext_dict
        else:
            if old_ext_multivalued:
                # there was already an array, so just tack on our new value
                cur_ext_value.append(ext_dict)
            else:
                # turn the extension dict into an array
                extensions[ext.prefix_name] = generic.ArrayObject(
                    [cur_ext_value, ext_dict]
                )
        self.update_container(extensions)

    def get_object(self, ido, as_metadata_stream: bool = False):
        if ido.pdf not in self._resolves_objs_from:
            raise PdfError(
                f'Reference {ido} has no relation to this PDF writer.'
            )
        idnum = ido.idnum
        generation = ido.generation
        try:
            return self.objects[(generation, idnum)]
        except KeyError:
            if generation == 0:
                if idnum in self._allocated_placeholders:
                    return generic.NullObject()
                try:
                    return self.objs_in_streams[idnum]
                except KeyError:
                    pass
            raise KeyError(ido)

    def allocate_placeholder(self) -> generic.IndirectObject:
        """
        Allocate an object reference to populate later.
        Calls to :meth:`get_object` for this reference will
        return :class:`~.generic.NullObject` until it is populated using
        :meth:`add_object`.

        This method is only relevant in certain advanced contexts where
        an object ID needs to be known before the object it refers
        to can be built; chances are you'll never need it.

        :return:
            A :class:`~.generic.IndirectObject` instance referring to
            the object just allocated.
        """

        idnum = self._lastobj_id + 1
        self._allocated_placeholders.add(idnum)
        self._lastobj_id += 1
        return generic.IndirectObject(idnum, 0, self)

    def add_object(
        self, obj, obj_stream: Optional[ObjectStream] = None, idnum=None
    ) -> generic.IndirectObject:
        """
        Add a new object to this writer.

        :param obj:
            The object to add.
        :param obj_stream:
            An object stream to add the object to.
        :param idnum:
            Manually specify the object ID of the object to be added.
            This is only allowed for object IDs that have previously been
            allocated using :meth:`allocate_placeholder`.
        :return:
            A :class:`~.generic.IndirectObject` instance referring to
            the object just added.
        """

        if idnum is not None:
            if idnum not in self._allocated_placeholders:
                raise PdfWriteError(
                    "Manually specifying idnum is only allowed for "
                    "references previously allocated using "
                    "allocate_placeholder()."
                )
            preallocated = True
        else:
            preallocated = False
            idnum = self._lastobj_id + 1

        if obj_stream is None:
            self.objects[(0, idnum)] = obj
        elif obj_stream in self.object_streams:
            obj_stream.add_object(idnum, obj)
            self.objs_in_streams[idnum] = obj
        else:
            raise PdfWriteError(
                f'Stream {repr(obj_stream)} is unknown to this PDF writer.'
            )

        if preallocated:
            self._allocated_placeholders.remove(idnum)
        else:
            self._lastobj_id += 1
        return generic.IndirectObject(idnum, 0, self)

    def prepare_object_stream(self, compress=True):
        """Prepare and return a new :class:`.ObjectStream` object.

        :param compress:
            Indicates whether the resulting object stream should be compressed.
        :return:
            An :class:`.ObjectStream` object.
        """
        if not self.stream_xrefs:
            raise PdfWriteError(
                'Object streams require Xref streams to be enabled.'
            )
        stream = ObjectStream(compress=compress)
        self.object_streams.append(stream)
        return stream

    def _write_header(self, stream):
        raise NotImplementedError

    def _assign_security_handler(self, sh: SecurityHandler):
        self.security_handler = sh
        self._encrypt = self.add_object(sh.as_pdf_object())
        min_pdf_version = sh.get_min_pdf_version()
        if min_pdf_version is not None:
            self.ensure_output_version(min_pdf_version)
        for extension in self.security_handler.get_extensions():
            self.register_extension(extension)

    def _flush_obj_stream(self, obj_stm: ObjectStream):
        """
        Internal method to flush an object stream as part of the file
        writing process.
        """
        stream_ref = obj_stm.ref
        if obj_stm and not stream_ref:
            # first, register the object stream object
            #  (will get written later)
            obj_stm.ref = stream_ref = self.add_object(obj_stm.as_pdf_object())
        # loop over all objects in the stream, and prepare
        # the data to put in the XRef table
        for ix, (idnum, obj) in enumerate(obj_stm):
            yield idnum, (stream_ref.idnum, ix)

    def _write_objects(self, stream, object_position_dict: PositionDict):
        # deal with objects in object streams first
        for obj_stream in self.object_streams:
            for idnum, pos_record in self._flush_obj_stream(obj_stream):
                object_position_dict[(0, idnum)] = pos_record

        for ix in sorted(self.objects.keys()):
            generation, idnum = ix
            obj = self.objects[ix]
            object_position_dict[ix] = stream.tell()
            stream.write(('%d %d obj\n' % (idnum, generation)).encode('ascii'))
            handler: Optional[SecurityHandler] = None
            if self.security_handler is not None:
                assert self._encrypt is not None
                if idnum != self._encrypt.idnum:
                    handler = self.security_handler
            container_ref = generic.Reference(idnum, generation, self)
            obj.write_to_stream(stream, handler, container_ref)
            stream.write(b'\nendobj\n')

    def _prep_dom_for_writing(self):
        # ensure that all font resources are flushed
        for fsc in self._font_resources.values():
            for engine in fsc.subsets.values():
                engine.prepare_write()

        self._update_meta()

    def _update_meta(self):
        xmp_xml = None
        try:
            # delayed import since the namespace registration operation
            # is global (thank you ElementTree...)
            # also, we want the XML dep(s) to be optional
            # noinspection PyUnresolvedReferences
            from pyhanko.pdf_utils.metadata import xmp_xml

            need_xmp = (
                self._meta.xmp_unmanaged
                or bool(self._meta.xmp_extra)
                or self.output_version >= (2, 0)
                or '/Metadata' in self.root
            )
        except ImportError:  # pragma: nocover
            need_xmp = False

        self._meta.last_modified = 'now'
        if self._info is not None:
            mod = update_info_dict(
                self._meta,
                self._info.get_object(),
                # if we write XMP, we only update existing entries
                only_update_existing=need_xmp,
            )
            if mod:
                self.mark_update(self._info)
        elif not need_xmp:
            info_dict = generic.DictionaryObject()
            update_info_dict(self._meta, info_dict)
            self._info = self.add_object(info_dict)
            # if there's no info dict, and we're going to write XMP anyhow,
            # don't bother creating one

        if need_xmp:
            meta_stm = None
            if '/Metadata' in self.root:
                meta_obj = self.root['/Metadata']
                if isinstance(meta_obj, xmp_xml.MetadataStream):
                    meta_stm = meta_obj
                    meta_stm.update_xmp_with_meta(self._meta)
                    self.update_container(meta_stm)
            if meta_stm is None:
                meta_stm = xmp_xml.MetadataStream.from_xmp(
                    xmp_xml.update_xmp_with_meta(self._meta)
                )
                sh = self.security_handler
                meta_stm._handler = sh
                if (
                    sh is not None
                    and not self.security_handler.encrypt_metadata
                ):
                    # note: this will add the /Identity crypt filter, hence
                    # metadata encryption will be omitted
                    meta_stm.add_crypt_filter()
                self.root['/Metadata'] = self.add_object(meta_stm)
                self.update_root()
            self.update_root()

    def _populate_trailer(self, trailer):
        # prepare trailer dictionary entries
        trailer[pdf_name('/Root')] = self._root
        if self._info is not None:
            trailer[pdf_name('/Info')] = self._info
        if self._encrypt is not None:
            trailer[pdf_name('/Encrypt')] = self._encrypt
        # before doing anything else, we attempt to load the crypto-relevant
        # data, so that we can bail early if something's not right
        trailer[pdf_name('/ID')] = self._document_id

    @property
    def trailer_view(self) -> generic.DictionaryObject:
        trailer = generic.DictionaryObject()
        self._populate_trailer(trailer)
        return trailer

    @property
    def _need_mac_on_write(self):
        return (
            not self.digest_aware_write
            and self.security_handler is not None
            and self.security_handler.pdf_mac_enabled
        )

    def write(self, stream):
        """
        Write the contents of this PDF writer to a stream.

        :param stream:
            A writable output stream.
        """
        self._prep_dom_for_writing()
        if self._need_mac_on_write:
            from pyhanko.pdf_utils.crypt import pdfmac

            self.register_extension(pdfmac.ISO32004)
            pdfmac.add_standalone_mac(self, output=stream)
        else:
            self._write(stream)

    def _init_mac_handler(self, md_algorithm: str = 'sha256'):
        """
        MAC-adding method that can easily be manipulated in tests.
        """
        from pyhanko.pdf_utils.crypt import pdfmac

        cls = self._mac_handler_cls or pdfmac.PdfMacTokenHandler
        sh = self.security_handler
        assert sh is not None
        return cls.from_key_mat(
            file_encryption_key=sh.get_file_encryption_key(),
            kdf_salt=sh.get_kdf_salt(),
            md_algorithm=md_algorithm,
        )

    def _write(self, stream, skip_header: bool = False):
        object_positions: PositionDict = {}

        trailer: generic.DictionaryObject
        if self.stream_xrefs:
            xmp_trailer = XRefStream(object_positions)
            xmp_trailer.compress()
            trailer = xmp_trailer
        else:
            trailer = generic.DictionaryObject()

        if not skip_header:
            self._write_header(stream)
        self._populate_trailer(trailer)
        self._write_objects(stream, object_positions)

        if self.stream_xrefs:
            xref_location = stream.tell()
            xrefs_id = self._lastobj_id + 1
            # add position of XRef stream to the XRef stream
            object_positions[(0, xrefs_id)] = xref_location
            trailer[pdf_name('/Size')] = generic.NumberObject(xrefs_id + 1)
            # write XRef stream
            stream.write(('%d %d obj\n' % (xrefs_id, 0)).encode('ascii'))
            trailer.write_to_stream(stream, None)
            stream.write(b'\nendobj\n')
        else:
            # classical xref table
            xref_location = write_xref_table(
                stream, cast(Dict[Tuple[int, int], int], object_positions)
            )
            trailer[pdf_name('/Size')] = generic.NumberObject(
                self._lastobj_id + 1
            )
            # write trailer
            stream.write(b'trailer\n')
            trailer.write_to_stream(stream, None)

        # write xref table pointer and EOF
        xref_pointer_string = '\nstartxref\n%s\n' % xref_location
        stream.write(xref_pointer_string.encode('ascii') + b'%%EOF\n')

    def register_annotation(self, page_ref, annot_ref):
        """
        Register an annotation to be added to a page.
        This convenience function takes care of calling :meth:`mark_update`
        where necessary.

        :param page_ref:
            Reference to the page object involved.
        :param annot_ref:
            Reference to the annotation object to be added.
        """
        page_obj = page_ref.get_object()
        try:
            annot_arr_ref = page_obj.raw_get('/Annots')
            if isinstance(annot_arr_ref, generic.IndirectObject):
                annots = annot_arr_ref.get_object()
                self.mark_update(annot_arr_ref)
            else:
                # we need to update the entire page object if the annots array
                # is a direct object
                annots = annot_arr_ref
                self.mark_update(page_ref)
        except KeyError:
            annots = generic.ArrayObject()
            self.mark_update(page_ref)
            page_obj[pdf_name('/Annots')] = annots

        annots.append(annot_ref)

    def insert_page(self, new_page, after=None):
        """
        Insert a page object into the tree.

        :param new_page:
            Page object to insert.
        :param after:
            Page number (zero-indexed) after which to insert the page.
        :return:
            A reference to the newly inserted page.
        """
        if new_page['/Type'] != pdf_name('/Page'):
            raise PdfWriteError('Not a page object')
        if '/Parent' in new_page:
            raise PdfWriteError('/Parent must not be set.')

        page_tree_root_ref = self.root.raw_get('/Pages')
        if after is None:
            page_count = page_tree_root_ref.get_object()['/Count']
            after = page_count - 1

        if after == -1:
            # there are no pages yet, this will be the first
            pages_obj_ref = page_tree_root_ref
            kid_ix = -1
        else:
            pages_obj_ref, kid_ix, _ = self.find_page_container(after)

        pages_obj = pages_obj_ref.get_object()
        try:
            kids = pages_obj['/Kids']
        except KeyError:  # pragma: nocover
            raise PdfError('/Pages must have /Kids')

        # increase page count for all parents
        parent = pages_obj
        while parent is not None:
            # can't use += 1 because of the way PyPDF2's generic types work
            count = parent['/Count']
            parent[pdf_name('/Count')] = generic.NumberObject(count + 1)
            parent = parent.get('/Parent')
        new_page_ref = self.add_object(new_page)
        kids.insert(kid_ix + 1, new_page_ref)
        new_page[pdf_name('/Parent')] = pages_obj_ref
        self.update_container(pages_obj)
        self.update_container(kids)

        return new_page_ref

    def import_object(
        self, obj: generic.PdfObject, obj_stream: Optional[ObjectStream] = None
    ) -> generic.PdfObject:
        """
        Deep-copy an object into this writer, dealing with resolving indirect
        references in the process.

        .. danger::
            The table mapping indirect references in the input to indirect
            references in the writer is not preserved between calls.
            Concretely, this means that invoking :meth:`import_object` twice
            on the same input reader may cause object duplication.

        :param obj:
            The object to import.
        :param obj_stream:
            The object stream to import objects into.

            .. note::
                Stream objects and bare references will not be put into
                the object stream; the standard forbids this.
        :return:
            The object as associated with this writer.
            If the input object was an indirect reference, a dictionary
            (incl. streams) or an array, the returned value will always be
            a new instance.
        """

        importer = _ObjectImporter(
            source=obj.get_container_ref().get_pdf_handler(),
            target=self,
            obj_stream=obj_stream,
            reference_map={},
        )
        return importer.import_object(obj)

    def import_page_as_xobject(
        self, other: PdfHandler, page_ix=0, inherit_filters=True
    ):
        """
        Import a page content stream from some other
        :class:`~.rw_common.PdfHandler` into the current one as a form XObject.

        :param other:
            A :class:`~.rw_common.PdfHandler`
        :param page_ix:
            Index of the page to copy (default: 0)
        :param inherit_filters:
            Inherit the content stream's filters, if present.
        :return:
            An :class:`~.generic.IndirectObject` referring to the page object
            as added to the current reader.
        """
        page_ref, resources = other.find_page_for_modification(page_ix)
        page_obj = page_ref.get_object()

        # find the page's /MediaBox by going up the tree until we encounter it
        pagetree_obj = page_obj
        while True:
            try:
                mb = pagetree_obj['/MediaBox']
                break
            except KeyError:
                try:
                    pagetree_obj = pagetree_obj['/Parent']
                except KeyError:  # pragma: nocover
                    raise PdfReadError(
                        f'Page {page_ix} does not have a /MediaBox'
                    )

        stream_dict = {
            pdf_name('/BBox'): mb,
            pdf_name('/Resources'): self.import_object(resources),
            pdf_name('/Type'): pdf_name('/XObject'),
            pdf_name('/Subtype'): pdf_name('/Form'),
        }
        command_stream = page_obj['/Contents']
        # if the page /Contents is an array, retrieve the content stream
        # with the appropriate index
        if isinstance(command_stream, generic.ArrayObject):
            if len(command_stream) == 1 and inherit_filters:
                command_stream = command_stream[0].get_object()
            else:
                # There are multiple streams, so inheriting filters is not
                # a well-defined operations.
                # Decode and concatenate, then put in a flate filter, and return
                result = generic.StreamObject(
                    stream_dict,
                    stream_data=b''.join(
                        partial_stream.get_object().data
                        for partial_stream in command_stream
                    ),
                )
                result.compress()
                return self.add_object(result)
        assert isinstance(command_stream, generic.StreamObject)
        filters = None
        if inherit_filters:
            try:
                # try to inherit filters from the original command stream
                filters = command_stream['/Filter']
            except KeyError:
                pass

        if filters is not None:
            stream_dict[pdf_name('/Filter')] = self.import_object(filters)
            result = generic.StreamObject(
                stream_dict, encoded_data=command_stream.encoded_data
            )
        else:
            result = generic.StreamObject(
                stream_dict, stream_data=command_stream.data
            )

        return self.add_object(result)

    # TODO these can be simplified considerably using the new update_container
    def add_stream_to_page(
        self, page_ix, stream_ref, resources=None, prepend=False
    ):
        """Append an indirect stream object to a page in a PDF as a content
        stream.

        :param page_ix:
            Index of the page to modify.
            The first page has index `0`.
        :param stream_ref:
            :class:`~.generic.IndirectObject` reference to the stream
            object to add.
        :param resources:
            Resource dictionary containing resources to add to the page's
            existing resource dictionary.
        :param prepend:
            Prepend the content stream to the list of content streams, as
            opposed to appending it to the end.
            This has the effect of causing the stream to be rendered
            underneath the already existing content on the page.
        :return:
            An :class:`~.generic.IndirectObject` reference to the page object
            that was modified.
        """

        page_obj_ref, res_ref = self.find_page_for_modification(page_ix)

        page_obj = page_obj_ref.get_object()

        contents_ref = page_obj.raw_get('/Contents')

        if isinstance(contents_ref, generic.IndirectObject):
            contents = contents_ref.get_object()
            if isinstance(contents, generic.ArrayObject):
                # This is the easy case. It suffices to mark
                # this array for an update, and append our stream to it.
                self.mark_update(contents_ref)
                if prepend:
                    contents.insert(0, stream_ref)
                else:
                    contents.append(stream_ref)
            elif isinstance(contents, generic.StreamObject):
                # replace the old stream with an array containing
                # a reference to the original stream, and our own stream.
                new = (
                    [stream_ref, contents_ref]
                    if prepend
                    else [contents_ref, stream_ref]
                )
                contents = generic.ArrayObject(new)
                page_obj[pdf_name('/Contents')] = self.add_object(contents)
                # mark the page to be updated as well
                self.mark_update(page_obj_ref)
            else:
                raise PdfError('Unexpected type for page /Contents')
        elif isinstance(contents_ref, generic.ArrayObject):
            # make /Contents an indirect array, and append our stream
            contents = contents_ref
            if prepend:
                contents.insert(0, stream_ref)
            else:
                contents.append(stream_ref)
            page_obj[pdf_name('/Contents')] = self.add_object(contents)
            self.mark_update(page_obj_ref)
        else:
            raise PdfError('Unexpected type for page /Contents')

        if resources is None:
            return

        if isinstance(res_ref, generic.IndirectObject):
            # we can get away with only updating this reference
            orig_resource_dict = res_ref.get_object()
            assert isinstance(orig_resource_dict, generic.DictionaryObject)
            if self.merge_resources(orig_resource_dict, resources):
                self.mark_update(res_ref)
        else:
            # don't bother trying to update the resource object, just
            # clone it and add it to the current page object.
            orig_resource_dict = generic.DictionaryObject(res_ref)
            page_obj[pdf_name('/Resources')] = self.add_object(
                orig_resource_dict
            )
            self.merge_resources(orig_resource_dict, resources)

        return page_obj_ref

    # TODO this doesn't really belong here
    def merge_resources(
        self,
        orig_dict: generic.DictionaryObject,
        new_dict: generic.DictionaryObject,
    ) -> bool:
        """
        Update an existing resource dictionary object with data from another
        one. Returns ``True`` if the original dict object was modified directly.

        The caller is responsible for avoiding name conflicts with existing
        resources.
        """

        update_needed = False
        for key, value in new_dict.items():
            try:
                orig_value_ref = orig_dict.raw_get(key)
            except KeyError:
                update_needed = True
                orig_dict[key] = value
                continue

            if isinstance(orig_value_ref, generic.IndirectObject):
                orig_value = orig_value_ref.get_object()
                self.mark_update(orig_value_ref)
            else:
                orig_value = orig_value_ref
                update_needed = True

            if (
                isinstance(orig_value, generic.ArrayObject)
                and key == '/ProcSet'
            ):
                orig_value.extend(
                    x
                    for x in value.get_object()
                    if isinstance(x, generic.NameObject) and x not in orig_value
                )
            elif isinstance(orig_value, generic.DictionaryObject):
                for key_, value_ in value.items():
                    if key_ in orig_value:
                        raise PdfError(
                            'Naming conflict in resource of type %s: '
                            'key %s occurs in both.' % (key, key_)
                        )
                    orig_value[key_] = value_

        return update_needed


class PageObject(generic.DictionaryObject):
    """Subclass of :class:`~.generic.DictionaryObject` that handles some of the
    initialisation boilerplate for page objects."""

    # TODO be more clever with inheritable required attributes,
    #  and enforce the requirements on insertion instead
    # (setting /MediaBox at the page tree root seems to make sense, for example)
    def __init__(self, contents, media_box, resources=None):
        resources = resources or generic.DictionaryObject()

        if isinstance(contents, list):
            if not all(map(instance_test(generic.IndirectObject), contents)):
                raise PdfWriteError(
                    'Contents array must consist of indirect references'
                )
            if not isinstance(contents, generic.ArrayObject):
                contents = generic.ArrayObject(contents)
        elif not isinstance(contents, generic.IndirectObject):
            raise PdfWriteError(
                'Contents must be either an indirect reference or an array'
            )

        if len(media_box) != 4:
            raise ValueError('Media box must consist of 4 coordinates.')
        super().__init__(
            {
                pdf_name('/Type'): pdf_name('/Page'),
                pdf_name('/MediaBox'): generic.ArrayObject(
                    map(generic.FloatObject, media_box)
                ),
                pdf_name('/Resources'): resources,
                pdf_name('/Contents'): contents,
            }
        )


class PdfFileWriter(BasePdfFileWriter):
    """Class to write new PDF files."""

    def __init__(self, stream_xrefs=True, init_page_tree=True, info=None):
        # root object
        root = generic.DictionaryObject(
            {
                pdf_name("/Type"): pdf_name("/Catalog"),
            }
        )

        id1 = generic.ByteStringObject(os.urandom(16))
        id2 = generic.ByteStringObject(os.urandom(16))
        id_obj = generic.ArrayObject([id1, id2])

        self._custom_trailer_entries = {}
        super().__init__(root, info, id_obj, stream_xrefs=stream_xrefs)

        if init_page_tree:
            pages = generic.DictionaryObject(
                {
                    pdf_name("/Type"): pdf_name("/Pages"),
                    pdf_name("/Count"): generic.NumberObject(0),
                    pdf_name("/Kids"): generic.ArrayObject(),
                }
            )

            root[pdf_name('/Pages')] = self.add_object(pages)

    def _write_header(self, stream):
        major, minor = self.output_version
        stream.write(f'%PDF-{major}.{minor}\n'.encode('ascii'))
        # write some binary characters to make sure the file is flagged
        # as binary (see § 7.5.2 in ISO 32000-1)
        stream.write(b'%\xc2\xa5\xc2\xb1\xc3\xab\n')

    def encrypt(self, owner_pass, user_pass=None, **kwargs):
        """
        Mark this document to be encrypted with PDF 2.0 encryption (AES-256).

        .. caution::
            While pyHanko supports legacy PDF encryption as well, the API
            to create new documents using outdated encryption is left
            largely undocumented on purpose to discourage its use.

            This caveat does *not* apply to incremental updates added to
            existing documents.

        .. danger::
            The PDF 2.0 standard mandates AES-256 in CBC mode, and also includes
            12 bytes of known plaintext by design. This implies that a
            sufficiently knowledgeable attacker can inject arbitrary content
            into your encrypted files without knowledge of the password.

            Adding a digital signature to the encrypted document is **not**
            a foolproof way to deal with this either, since most viewers will
            still allow the document to be opened before signatures are
            validated, and therefore end users are still exposed to potentially
            malicious content.

            Until the standard supports authenticated encryption schemes, you
            should **never** rely on its encryption provisions if tampering
            is a concern.


        :param owner_pass:
            The desired owner password.
        :param user_pass:
            The desired user password (defaults to the owner password
            if not specified)
        :param kwargs:
            Other keyword arguments to be passed to
            :meth:`.StandardSecurityHandler.build_from_pw`.
        """
        sh = StandardSecurityHandler.build_from_pw(
            owner_pass, user_pass, **kwargs
        )
        self._assign_security_handler(sh)

    def encrypt_pubkey(self, recipients: List[x509.Certificate], **kwargs):
        """
        Mark this document to be encrypted with PDF 2.0 public key encryption.
        The certificates passed in should be RSA certificates.

        PyHanko defaults to AES-256 to encrypt the actual file contents.
        The seed used to derive the file encryption key is also encrypted
        using AES-256 and bundled in a CMS EnvelopedData object.
        The envelope key is then encrypted separately for each recipient, using
        their respective public keys.

        .. caution::
            The caveats for :meth:`encrypt` also apply here.

        :param recipients:
            Certificates of the recipients that should be able to decrypt
            the document.
        :param kwargs:
            Other keyword arguments to be passed to
            :meth:`.PubKeySecurityHandler.build_from_certs`.
        """
        self.output_version = (2, 0)
        sh = PubKeySecurityHandler.build_from_certs(recipients, **kwargs)
        self._assign_security_handler(sh)

    def set_custom_trailer_entry(
        self, key: generic.NameObject, value: generic.PdfObject
    ):
        """
        Set a custom, unmanaged entry in the document trailer or cross-reference
        stream dictionary.

        .. warning::
            Calling this method to set an entry that is managed by pyHanko
            internally (info dictionary, document catalog, etc.) has undefined
            results.

        :param key:
            Dictionary key to use in the trailer.
        :param value:
            Value to set
        """
        self._custom_trailer_entries[key] = value

    def _populate_trailer(self, trailer):
        # allow the base implementation to override internally managed entries
        trailer.update(self._custom_trailer_entries)
        super()._populate_trailer(trailer)


class _ObjectImporter:

    def __init__(
        self,
        source: PdfHandler,
        target: BasePdfFileWriter,
        reference_map: Dict[generic.Reference, generic.IndirectObject],
        obj_stream: Optional[ObjectStream],
    ):
        self.source = source
        self.target = target
        self.obj_stream = obj_stream
        self.queued_references: List[
            Tuple[generic.Reference, generic.Reference]
        ] = []
        self.reference_map = reference_map

    def import_object(self, obj: generic.PdfObject) -> generic.PdfObject:
        result = self._ingest(obj)

        while self.queued_references:
            source_ref, target_ref = self.queued_references.pop()
            source_obj = source_ref.get_object()
            imported = self._ingest(source_obj)

            # if the imported object is a bare reference and/or a stream
            # object, we can't put it into an object stream.
            if isinstance(imported, OBJSTREAM_FORBIDDEN):
                obj_stream = None
            else:
                obj_stream = self.obj_stream

            # fill in the placeholder
            self.target.add_object(
                imported, obj_stream=obj_stream, idnum=target_ref.idnum
            )

        return result

    def _ingest(self, obj: generic.PdfObject):
        if isinstance(obj, generic.DecryptedObjectProxy):
            obj = obj.decrypted
        if isinstance(obj, generic.IndirectObject):
            return self.process_reference(obj.reference)
        elif isinstance(obj, generic.DictionaryObject):
            raw_dict = {
                k: self._ingest(v) for k, v in obj.items() if k != '/Metadata'
            }
            try:
                # make sure to import metadata streams as such
                meta_ref = obj.get_value_as_reference('/Metadata')
                # ensure a MetadataStream object ends up in the cache
                meta_ref.get_pdf_handler().get_object(
                    meta_ref, as_metadata_stream=True
                )
                # ...then import the reference
                raw_dict['/Metadata'] = self.process_reference(meta_ref)
            except (KeyError, IndirectObjectExpected):
                pass

            if isinstance(obj, generic.StreamObject):
                stm_cls = generic.StreamObject
                # again, make sure to import metadata streams as such
                try:
                    # noinspection PyUnresolvedReferences
                    from pyhanko.pdf_utils.metadata import xmp_xml

                    if isinstance(obj, xmp_xml.MetadataStream):
                        stm_cls = xmp_xml.MetadataStream
                except ImportError:  # pragma: nocover
                    pass
                # In the vast majority of use cases, I'd expect the content
                # to be available in encoded form by default.
                # By initialising the stream object in this way, we avoid
                # a potentially costly decoding operation.
                return stm_cls(raw_dict, encoded_data=obj.encoded_data)
            else:
                return generic.DictionaryObject(raw_dict)
        elif isinstance(obj, generic.ArrayObject):
            return generic.ArrayObject(self._ingest(v) for v in obj)
        else:
            return obj

    def process_reference(self, ref: generic.Reference) -> generic.PdfObject:
        try:
            return self.reference_map[ref]
        except KeyError:
            # Add a placeholder to reserve the reference value.
            new_ido = self.target.allocate_placeholder()
            self.reference_map[ref] = new_ido
            self.queued_references.append((ref, new_ido.reference))
            return new_ido

    def preprocess_signature_data(self):
        # Signature /Contents is never encrypted => ensure we respect that
        # (even though the import operation is guaranteed to break the signature
        # there are valid use cases for stripping the encryption on such files,
        # e.g. for downstream processing)
        from ..sign.fields import enumerate_sig_fields

        signature_dict_refs = [
            field_value.reference
            for fq_name, field_value, field_ref in enumerate_sig_fields(
                self.source, filled_status=True
            )
            # this is the case in all valid PDFs
            if isinstance(field_value, generic.IndirectObject)
        ]
        if signature_dict_refs:
            logger.warning(
                "Source document contains filled signature fields--the copy "
                "operation will invalidate them."
            )
        for ref in signature_dict_refs:
            sig_dict = ref.get_object()
            assert isinstance(sig_dict, generic.DictionaryObject)
            raw_dict = {
                k: self._ingest(v)
                for k, v in sig_dict.items()
                if k != '/Contents'
            }
            raw_dict['/Contents'] = generic.ByteStringObject(
                sig_dict.raw_get(
                    '/Contents', decrypt=generic.EncryptedObjAccess.RAW
                ).original_bytes
            )
            self.reference_map[ref] = self.target.add_object(
                generic.DictionaryObject(raw_dict),
                obj_stream=None,
            )


def copy_into_new_writer(
    input_handler: PdfHandler, writer_kwargs: Optional[dict] = None
) -> PdfFileWriter:
    """
    Copy all objects in a given PDF handler into a new :class:`.PdfFileWriter`.
    This operation will attempt to preserve the document catalog
    of the original ``input_handler``.

    Very roughly, calling this function and then immediately invoking
    :meth:`~.BasePdfFileWriter.write` on the resulting writer should result
    in an equivalent document as far as presentation is concerned.
    As a general rule, behaviour that is controlled from outside the document
    catalog (e.g. encryption) or that requires byte-for-byte equivalence with
    the original (e.g. digital signatures) will not survive this translation.


    :param input_handler:
        :class:`.PdfHandler` to source objects from.
    :param writer_kwargs:
        Keyword arguments to pass to the writer.
    :return:
        New :class:`.PdfFileWriter` containing all objects from the input
        handler.
    """

    writer_kwargs = writer_kwargs or {}
    writer_kwargs.setdefault("stream_xrefs", False)

    # TODO try to be more clever with object streams
    w = PdfFileWriter(init_page_tree=False, **writer_kwargs)
    input_root_ref = input_handler.root_ref
    output_root_ref = w.root_ref
    # call _ObjectImporter in such a way that we translate the input handler's
    # root to the new writer's root.
    # From a technical PoV this doesn't matter, but it makes the output file
    # somewhat "cleaner" (i.e. it doesn't leave an orphaned document catalog
    # cluttering up the file)
    importer = _ObjectImporter(
        source=input_handler,
        target=w,
        reference_map={
            input_root_ref: generic.IndirectObject(
                idnum=output_root_ref.idnum,
                generation=output_root_ref.generation,
                pdf=w,
            )
        },
        obj_stream=None,
    )
    importer.preprocess_signature_data()
    new_root_dict = importer.import_object(input_handler.root)
    # override the old root ref
    ix = (output_root_ref.generation, output_root_ref.idnum)
    w.objects[ix] = new_root_dict

    if "info" not in writer_kwargs:
        try:
            # migrate the info dict. We do this low-level to avoid issues
            # with the producer string handling, and to keep a nice separation
            # between user-supplied metadata values and values that were present
            # in the original doc.
            info_dict = input_handler.trailer_view['/Info']
        except KeyError:
            info_dict = None
        if info_dict is not None:
            importer = _ObjectImporter(
                source=input_handler,
                target=w,
                reference_map={},
                obj_stream=None,
            )
            imported_info = importer.import_object(info_dict)
            w._info = w.add_object(imported_info)

    return w
