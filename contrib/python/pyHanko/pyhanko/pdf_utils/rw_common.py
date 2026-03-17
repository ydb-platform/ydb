"""Utilities common to reading and writing PDF files."""

from typing import Tuple

from . import generic, misc
from .metadata.model import DocumentMetadata
from .misc import PdfError

__all__ = ['PdfHandler']


class PdfHandler:
    """Abstract class providing a general interface for quering objects
    in PDF readers and writers alike."""

    def get_object(
        self, ref: generic.Reference, as_metadata_stream: bool = False
    ):
        """
        Retrieve the object associated with the provided reference from
        this PDF handler.

        :param ref:
            An instance of :class:`.generic.Reference`.
        :param as_metadata_stream:
            Whether to dereference the object as an XMP metadata stream.
        :return:
            A PDF object.
        """
        raise NotImplementedError

    @property
    def trailer_view(self) -> generic.DictionaryObject:
        """
        Returns a view of the document trailer of the document represented
        by this :class:`.PdfHandler` instance.

        The view is effectively read-only, in the sense that any writes
        will not be reflected in the actual trailer (if the handler supports
        writing, that is).

        :return:
            A :class:`.generic.DictionaryObject` representing the current state
            of the document trailer.
        """
        raise NotImplementedError

    @property
    def document_meta_view(self) -> DocumentMetadata:
        raise NotImplementedError

    @property
    def root_ref(self) -> generic.Reference:
        """
        :return: A reference to the document catalog of this PDF handler.
        """
        raise NotImplementedError

    @property
    def root(self) -> generic.DictionaryObject:
        """
        :return: The document catalog of this PDF handler.
        """
        root = self.root_ref.get_object()
        assert isinstance(root, generic.DictionaryObject)
        return root

    @property
    def document_id(self) -> Tuple[bytes, bytes]:
        raise NotImplementedError

    # TODO write tests specifically for this helper function
    def _walk_page_tree(self, page_ix, retrieve_parent):
        # the spec says that this will always be an indirect reference
        page_tree_root_ref = self.root.raw_get('/Pages')
        assert isinstance(page_tree_root_ref, generic.IndirectObject)
        page_tree_root = page_tree_root_ref.get_object()
        assert isinstance(page_tree_root, generic.DictionaryObject)
        try:
            root_resources = page_tree_root['/Resources']
        except KeyError:
            root_resources = generic.DictionaryObject()

        page_count = page_tree_root['/Count']
        if page_ix < 0:
            page_ix = page_count + page_ix
        if not (0 <= page_ix < page_count):
            raise PdfError('Page index out of range')

        def _recurse(first_page_ix, pages_obj_ref, last_rsrc_dict, refs_seen):
            pages_obj = pages_obj_ref.get_object()
            kids = pages_obj['/Kids']
            try:
                last_rsrc_dict = pages_obj.raw_get('/Resources')
            except KeyError:
                pass

            cur_page_ix = first_page_ix
            for kid_index, kid_ref in enumerate(kids):
                if not isinstance(kid_ref, generic.IndirectObject):
                    raise misc.PdfReadError(
                        "Page tree node children must be indirect objects"
                    )
                assert isinstance(kid_ref, generic.IndirectObject)
                if kid_ref.reference in refs_seen:
                    raise misc.PdfReadError("Circular reference in page tree")

                kid = kid_ref.get_object()

                node_type = kid['/Type']
                if node_type == '/Pages':
                    # recurse into this branch if the page we need
                    # is part of it
                    desc_count = kid['/Count']
                    if cur_page_ix <= page_ix < cur_page_ix + desc_count:
                        return _recurse(
                            cur_page_ix,
                            kid_ref,
                            last_rsrc_dict,
                            refs_seen | {kid_ref.reference},
                        )
                    cur_page_ix += desc_count
                elif node_type == '/Page':
                    if cur_page_ix == page_ix:
                        if retrieve_parent:
                            return (pages_obj_ref, kid_index, last_rsrc_dict)
                        else:
                            try:
                                last_rsrc_dict = kid.raw_get('/Resources')
                            except KeyError:
                                pass
                            return kid_ref, last_rsrc_dict
                    else:
                        cur_page_ix += 1
            # This means the PDF is not standards-compliant
            raise PdfError('Page not found')

        return _recurse(0, page_tree_root_ref, root_resources, set())

    def find_page_container(self, page_ix):
        """
        Retrieve the node in the page tree containing the
        page with index ``page_ix``, along with the necessary objects
        to modify it in an incremental update scenario.

        :param page_ix:
            The (zero-indexed) number of the page for which we want to
            retrieve the parent.
            A negative number counts pages from the back of the document,
            with index ``-1`` referring to the last page.
        :return:
            A triple with the ``/Pages`` object (or a reference to it),
            the index of the target page in said ``/Pages`` object, and a
            (possibly inherited) resource dictionary.
        """
        return self._walk_page_tree(page_ix, retrieve_parent=True)

    def find_page_for_modification(self, page_ix):
        """
        Retrieve the page with index ``page_ix`` from the page tree, along with
        the necessary objects to modify it in an incremental update scenario.

        :param page_ix:
            The (zero-indexed) number of the page to retrieve.
            A negative number counts pages from the back of the document,
            with index ``-1`` referring to the last page.
        :return:
            A tuple with a reference to the page object and a
            (possibly inherited) resource dictionary.
        """
        return self._walk_page_tree(page_ix, retrieve_parent=False)
