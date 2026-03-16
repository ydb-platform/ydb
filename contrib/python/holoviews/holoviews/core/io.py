"""Module defining input/output interfaces to HoloViews.

There are two components for input/output:

Exporters : Process (composite) HoloViews objects one at a time. For
           instance, an exporter may render a HoloViews object as a
           svg or perhaps pickle it.

Archives : A collection of HoloViews objects that are first collected
          then processed together. For instance, collecting HoloViews
          objects for a report then generating a PDF or collecting
          HoloViews objects to dump to HDF5.

"""
import itertools
import os
import pickle
import re
import shutil
import string
import tarfile
import time
import zipfile
from collections import defaultdict
from hashlib import sha256
from io import BytesIO

import param
from param.parameterized import bothmethod

from .dimension import LabelledData
from .element import Collator, Element
from .ndmapping import NdMapping, UniformNdMapping
from .options import Store
from .overlay import Layout, Overlay
from .util import group_sanitizer, label_sanitizer, unique_iterator


def sanitizer(name, replacements=None):
    """String sanitizer to avoid problematic characters in filenames.

    """
    if replacements is None:
        replacements = [(':', '_'), ('/', '_'), ('\\', '_')]
    for old,new in replacements:
        name = name.replace(old,new)
    return name


class Reference(param.Parameterized):
    """A Reference allows access to an object to be deferred until it is
    needed in the appropriate context. References are used by
    Collector to capture the state of an object at collection time.

    One particularly important property of references is that they
    should be pickleable. This means that you can pickle Collectors so
    that you can unpickle them in different environments and still
    collect from the required object.

    A Reference only needs to have a resolved_type property and a
    resolve method. The constructor will take some specification of
    where to find the target object (may be the object itself).

    """

    @property
    def resolved_type(self):
        """Returns the type of the object resolved by this references. If
        multiple types are possible, the return is a tuple of types.

        """
        raise NotImplementedError


    def resolve(self, container=None):
        """Return the referenced object. Optionally, a container may be
        passed in from which the object is to be resolved.

        """
        raise NotImplementedError



class Exporter(param.ParameterizedFunction):
    """An Exporter is a parameterized function that accepts a HoloViews
    object and converts it to a new some new format. This mechanism is
    designed to be very general so here are a few examples:

    Pickling :  Native Python, supported by HoloViews.
    Rendering : Any plotting backend may be used (default uses matplotlib)
    Storage :   Saving to a database (e.g. SQL), HDF5 etc.

    """

    # Mime-types that need encoding as utf-8 upon export
    utf8_mime_types = ['image/svg+xml', 'text/html', 'text/json']

    key_fn = param.Callable(doc="""
      Function that generates the metadata key from the HoloViews
      object being saved. The metadata key is a single
      high-dimensional key of values associated with dimension labels.

      The returned dictionary must have string keys and simple
      literals that may be conveniently used for dictionary-style
      indexing. Returns an empty dictionary by default.""")

    info_fn = param.Callable(default=lambda x: {'repr':repr(x)}, doc="""
      Function that generates additional metadata information from the
      HoloViews object being saved.

      Unlike metadata keys, the information returned may be unsuitable
      for use as a key index and may include entries such as the
      object's repr. Regardless, the info metadata should still only
      contain items that will be quick to load and inspect. """)


    @classmethod
    def encode(cls, entry):
        """Classmethod that applies conditional encoding based on
        mime-type. Given an entry as returned by __call__ return the
        data in the appropriate encoding.

        """
        (data, info) = entry
        if info['mime_type'] in cls.utf8_mime_types:
            return data.encode('utf-8')
        else:
            return data

    @bothmethod
    def _filename(self_or_cls, filename):
        """Add the file extension if not already present

        """
        filename = os.fspath(filename)
        if not filename.endswith(self_or_cls.file_ext):
            return f'{filename}.{self_or_cls.file_ext}'
        else:
            return filename

    @bothmethod
    def _merge_metadata(self_or_cls, obj, fn, *dicts):
        """Returns a merged metadata info dictionary from the supplied
        function and additional dictionaries

        """
        merged = {k:v for d in dicts for (k,v) in d.items()}
        return dict(merged, **fn(obj)) if fn else merged

    def __call__(self, obj, fmt=None):
        """Given a HoloViews object return the raw exported data and
        corresponding metadata as the tuple (data, metadata). The
        metadata should include:

        'file-ext' : The file extension if applicable (else empty string)
        'mime_type': The mime-type of the data.

        The fmt argument may be used with exporters that support multiple
        output formats. If not supplied, the exporter is to pick an
        appropriate format automatically.

        """
        raise NotImplementedError("Exporter not implemented.")


    @bothmethod
    def save(self_or_cls, obj, basename, fmt=None, key=None, info=None, **kwargs):
        """Similar to the call method except saves exporter data to disk
        into a file with specified basename. For exporters that
        support multiple formats, the fmt argument may also be
        supplied (which typically corresponds to the file-extension).

        The supplied metadata key and info dictionaries will be used
        to update the output of the relevant key and info functions
        which is then saved (if supported).

        """
        if info is None:
            info = {}
        if key is None:
            key = {}
        raise NotImplementedError("Exporter save method not implemented.")



class Importer(param.ParameterizedFunction):
    """An Importer is a parameterized function that accepts some data in
    some format and returns a HoloViews object. This mechanism is
    designed to be very general so here are a few examples:

    Unpickling : Native Python, supported by HoloViews.
    Servers :    Loading data over a network connection.
    Storage :    Loading from a database (e.g. SQL), HDF5 etc.

    """

    def __call__(self, data):
        """Given raw data in the appropriate format return the
        corresponding HoloViews object. Acts as the inverse of
        Exporter when supplied the data portion of an Exporter's
        output.

        """
        raise NotImplementedError("Importer not implemented.")


    @bothmethod
    def load(self_or_cls, src, entries=None):
        """Given some source (e.g. a filename, a network connection etc),
        return the loaded HoloViews object.

        """
        raise NotImplementedError("Importer load method not implemented.")


    @bothmethod
    def loader(self_or_cls, kwargs):
        return self_or_cls.load(**kwargs)


    @bothmethod
    def info(self_or_cls, src):
        """Returns the 'info' portion of the metadata (if available).

        """
        raise NotImplementedError("Importer info method not implemented.")

    @bothmethod
    def key(self_or_cls, src):
        """Returns the metadata key (if available).

        """
        raise NotImplementedError("Importer keys method not implemented.")



class Serializer(Exporter):
    """A generic exporter that supports any arbitrary serializer

    """

    serializer=param.Callable(default=Store.dumps, doc="""
       The serializer function, set to Store.dumps by default. The
       serializer should take an object and output a serialization as
       a string or byte stream.

       Any suitable serializer may be used. For instance, pickle.dumps
       may be used although this will not save customized options.""")

    mime_type=param.String('application/python-pickle', allow_None=True, doc="""
       The mime-type associated with the serializer (if applicable).""")

    file_ext = param.String('pkl', doc="""
       The file extension associated with the corresponding file
       format (if applicable).""")


    def __call__(self, obj, **kwargs):
        data = self.serializer(obj)
        return data, {'file-ext': self.file_ext, 'mime_type':self.mime_type}

    @bothmethod
    def save(self_or_cls, obj, filename, info=None, key=None, **kwargs):
        if key is None:
            key = {}
        if info is None:
            info = {}
        data, base_info = self_or_cls(obj, **kwargs)
        key = self_or_cls._merge_metadata(obj, self_or_cls.key_fn, key)
        info = self_or_cls._merge_metadata(obj, self_or_cls.info_fn, info, base_info)
        metadata, _ = self_or_cls({'info':info, 'key':key}, **kwargs)
        filename = self_or_cls._filename(filename)
        with open(filename, 'ab') as f:
            f.write(metadata)
            f.write(data)



class Deserializer(Importer):
    """A generic importer that supports any arbitrary de-serializer.

    """

    deserializer=param.Callable(default=Store.load, doc="""
       The deserializer function, set to Store.load by default. The
       deserializer should take a file-like object that can be read
       from until the first object has been deserialized. If the file
       has not been exhausted, the deserializer should be able to
       continue parsing and loading objects.

       Any suitable deserializer may be used. For instance,
       pickle.load may be used although this will not load customized
       options.""")

    def __call__(self, data):
        return self.deserializer(BytesIO(data))

    @bothmethod
    def load(self_or_cls, filename):
        with open(filename, 'rb') as f:
            data = self_or_cls.deserializer(f)
            try:
                data = self_or_cls.deserializer(f)
            except Exception:
                pass
        return data

    @bothmethod
    def key(self_or_cls, filename):
        with open(filename, "rb") as f:
            metadata = self_or_cls.deserializer(f)
        metadata = metadata if isinstance(metadata, dict) else {}
        return metadata.get('key', {})

    @bothmethod
    def info(self_or_cls, filename):
        with open(filename, "rb") as f:
            metadata = self_or_cls.deserializer(f)
        metadata = metadata if isinstance(metadata, dict) else {}
        return metadata.get('info', {})



class Pickler(Exporter):
    """The recommended pickler for serializing HoloViews object to a .hvz
    file (a simple zip archive of pickle files). In addition to the
    functionality offered by Store.dump and Store.load, this file
    format offers three additional features:

    1. Optional (zip) compression.
    2. Ability to save and load components of a Layout independently.
    3. Support for metadata per saved component.

    The output file with the .hvz file extension is simply a zip
    archive containing pickled HoloViews objects.

    """

    protocol = param.Integer(default=2, doc="""
        The pickling protocol where 0 is ASCII, 1 supports old Python
        versions and 2 is efficient for new style classes.""")

    compress = param.Boolean(default=True, doc="""
        Whether compression is enabled or not""")

    mime_type = 'application/zip'
    file_ext = 'hvz'


    def __call__(self, obj, key=None, info=None, **kwargs):
        if info is None:
            info = {}
        if key is None:
            key = {}
        buff = BytesIO()
        self.save(obj, buff, key=key, info=info, **kwargs)
        buff.seek(0)
        return buff.read(), {'file-ext': 'hvz', 'mime_type':self.mime_type}

    @bothmethod
    def save(self_or_cls, obj, filename, key=None, info=None, **kwargs):
        if info is None:
            info = {}
        if key is None:
            key = {}
        base_info = {'file-ext': 'hvz', 'mime_type':self_or_cls.mime_type}
        key = self_or_cls._merge_metadata(obj, self_or_cls.key_fn, key)
        info = self_or_cls._merge_metadata(obj, self_or_cls.info_fn, info, base_info)
        compression = zipfile.ZIP_STORED if self_or_cls.compress else zipfile.ZIP_DEFLATED

        filename = self_or_cls._filename(filename) if isinstance(filename, (str, os.PathLike)) else filename
        with zipfile.ZipFile(filename, 'w', compression=compression) as f:

            if isinstance(obj, Layout) and not isinstance(obj, Overlay):
                entries = ['.'.join(k) for k in obj.data.keys()]
                components = list(obj.data.values())
                entries = entries if len(entries) > 1 else [entries[0]+'(L)']
            else:
                entries = [f'{group_sanitizer(obj.group, False)}.{label_sanitizer(obj.label, False)}']
                components = [obj]

            for component, entry in zip(components, entries, strict=None):
                f.writestr(entry,
                           Store.dumps(component, protocol=self_or_cls.protocol))
            f.writestr('metadata',
                       pickle.dumps({'info':info, 'key':key}))



class Unpickler(Importer):
    """The inverse of Pickler used to load the .hvz file format which is
    simply a zip archive of pickle objects.

    Unlike a regular pickle file, info and key metadata as well as
    individual components of a Layout may be loaded without needing to
    load the entire file into memory.

    The components that may be individually loaded may be found using
    the entries method.

    """

    def __call__(self, data, entries=None):
        buff = BytesIO(data)
        return self.load(buff, entries=entries)

    @bothmethod
    def load(self_or_cls, filename, entries=None):
        components, single_layout = [], False
        entries = entries if entries else self_or_cls.entries(filename)
        with zipfile.ZipFile(filename, 'r') as f:
            for entry in entries:
                if entry not in f.namelist():
                    raise Exception(f"Entry {entry} not available")
                components.append(Store.loads(f.read(entry)))
                single_layout = entry.endswith('(L)')

        if len(components) == 1 and not single_layout:
            return components[0]
        else:
            return Layout(components)

    @bothmethod
    def _load_metadata(self_or_cls, filename, name):
        with zipfile.ZipFile(filename, 'r') as f:
            if 'metadata' not in f.namelist():
                raise Exception("No metadata available")
            metadata = pickle.loads(f.read('metadata'))
            if name not in metadata:
                raise KeyError(f"Entry {name} is missing from the metadata")
            return metadata[name]

    @bothmethod
    def key(self_or_cls, filename):
        return self_or_cls._load_metadata(filename, 'key')

    @bothmethod
    def info(self_or_cls, filename):
        return self_or_cls._load_metadata(filename, 'info')

    @bothmethod
    def entries(self_or_cls, filename):
        with zipfile.ZipFile(filename, 'r') as f:
            return [el for el in f.namelist() if el != 'metadata']

    @bothmethod
    def collect(self_or_cls, files, drop=None, metadata=True):
        """Given a list or NdMapping type containing file paths return a
        Layout of Collators, which can be called to load a given set
        of files using the current Importer.

        If supplied as a list each file is expected to disambiguate
        itself with contained metadata. If an NdMapping type is
        supplied additional key dimensions may be supplied as long as
        they do not clash with the file metadata. Any key dimension
        may be dropped by name by supplying a drop argument.

        """
        if drop is None:
            drop = []
        aslist = not isinstance(files, (NdMapping, Element))
        if isinstance(files, Element):
            files = Collator(files)
            file_kdims = files.kdims
        else:
            file_kdims = files.kdims
        drop_extra = files.drop if isinstance(files, Collator) else []

        mdata_dims = []
        if metadata:
            fnames = [fname[0] if isinstance(fname, tuple) else fname
                      for fname in files.values()]
            mdata_dims = {kdim for fname in fnames
                          for kdim in self_or_cls.key(fname).keys()}
        file_dims = set(files.dimensions('key', label=True))
        added_dims = set(mdata_dims) - file_dims
        overlap_dims = file_dims & set(mdata_dims)
        kwargs = dict(kdims=file_kdims + sorted(added_dims),
                      vdims=['filename', 'entries'],
                      value_transform=self_or_cls.loader,
                      drop=drop_extra + drop)
        layout_data = defaultdict(lambda: Collator(None, **kwargs))

        for key, fname in files.data.items():
            fname = fname[0] if isinstance(fname, tuple) else fname
            mdata = self_or_cls.key(fname) if metadata else {}
            for odim in overlap_dims:
                kval = key[files.get_dimension_index(odim)]
                if kval != mdata[odim]:
                    raise KeyError("Metadata supplies inconsistent "
                                   f"value for dimension {odim}")
            mkey = tuple(mdata.get(d, None) for d in added_dims)
            key = mkey if aslist else key + mkey
            if isinstance(fname, tuple) and len(fname) == 1:
                (fname,) = fname
            for entry in self_or_cls.entries(fname):
                layout_data[entry][key] = (fname, [entry])
        return Layout(layout_data.items())



class Archive(param.Parameterized):
    """An Archive is a means to collect and store a collection of
    HoloViews objects in any number of different ways. Examples of
    possible archives:

    * Generating tar or zip files (compressed or uncompressed).
    * Collating a report or document (e.g. PDF, HTML, LaTex).
    * Storing a collection of HoloViews objects to a database or HDF5.

    """

    exporters= param.List(default=[], doc="""
        The exporter functions used to convert HoloViews objects into the
        appropriate format(s)."""  )

    def add(self, obj, *args, **kwargs):
        """Add a HoloViews object to the archive.

        """
        raise NotImplementedError

    def export(self,*args, **kwargs):
        """Finalize and close the archive.

        """
        raise NotImplementedError



def simple_name_generator(obj):
    """Simple name_generator designed for HoloViews objects.

    Objects are labeled with {group}-{label} for each nested
    object, based on a depth-first search.  Adjacent objects with
    identical representations yield only a single copy of the
    representation, to avoid long names for the common case of
    a container whose element(s) share the same group and label.

    """
    if isinstance(obj, LabelledData):
        labels = obj.traverse(lambda x:
                              (x.group + ('-'  +x.label if x.label else '')))
        labels=[l[0] for l in itertools.groupby(labels)]
        obj_str = ','.join(labels)
    else:
        obj_str = repr(obj)
    return obj_str



class FileArchive(Archive):
    """A file archive stores files on disk, either unpacked in a
    directory or in an archive format (e.g. a zip file).

    """

    exporters= param.List(default=[Pickler], doc="""
        The exporter functions used to convert HoloViews objects into
        the appropriate format(s).""")

    dimension_formatter = param.String("{name}_{range}", doc="""
        A string formatter for the output file based on the
        supplied HoloViews objects dimension names and values.
        Valid fields are the {name}, {range} and {unit} of the
        dimensions.""")

    object_formatter = param.Callable(default=simple_name_generator, doc="""
        Callable that given an object returns a string suitable for
        inclusion in file and directory names. This is what generates
        the value used in the {obj} field of the filename
        formatter.""")

    filename_formatter = param.String('{dimensions},{obj}', doc="""
        A string formatter for output filename based on the HoloViews
        object that is being rendered to disk.

        The available fields are the {type}, {group}, {label}, {obj}
        of the holoviews object added to the archive as well as
        {timestamp}, {obj} and {SHA}. The {timestamp} is the export
        timestamp using timestamp_format, {obj} is the object
        representation as returned by object_formatter and {SHA} is
        the SHA of the {obj} value used to compress it into a shorter
        string.""")

    timestamp_format = param.String("%Y_%m_%d-%H_%M_%S", doc="""
        The timestamp format that will be substituted for the
        {timestamp} field in the export name.""")

    root = param.String('.', doc="""
        The root directory in which the output directory is
        located. May be an absolute or relative path.""")

    archive_format = param.Selector(default='zip', objects=['zip', 'tar'], doc="""
        The archive format to use if there are multiple files and pack
        is set to True. Supported formats include 'zip' and 'tar'.""")

    pack = param.Boolean(default=False, doc="""
        Whether or not to pack to contents into the specified archive
        format. If pack is False, the contents will be output to a
        directory.

        Note that if there is only a single file in the archive, no
        packing will occur and no directory is created. Instead, the
        file is treated as a single-file archive.""")

    export_name = param.String(default='{timestamp}', doc="""
        The name assigned to the overall export. If an archive file is
        used, this is the correspond filename (e.g. of the exporter zip
        file). Alternatively, if unpack=False, this is the name of the
        output directory. Lastly, for archives of a single file, this
        is the basename of the output file.

        The {timestamp} field is available to include the timestamp at
        the time of export in the chosen timestamp format.""")

    unique_name = param.Boolean(default=False, doc="""
       Whether the export name should be made unique with a numeric
       suffix. If set to False, any existing export of the same name
       will be removed and replaced.""")

    max_filename = param.Integer(default=100, bounds=(0,None), doc="""
       Maximum length to enforce on generated filenames.  100 is the
       practical maximum for zip and tar file generation, but you may
       wish to use a lower value to avoid long filenames.""")

    flush_archive = param.Boolean(default=True, doc="""
       Flushed the contents of the archive after export.
       """)


    ffields = {'type', 'group', 'label', 'obj', 'SHA', 'timestamp', 'dimensions'}
    efields = {'timestamp'}

    @classmethod
    def parse_fields(cls, formatter):
        """Returns the format fields otherwise raise exception

        """
        if formatter is None: return []
        try:
            parse = list(string.Formatter().parse(formatter))
            return {f for f in list(zip(*parse, strict=None))[1] if f is not None}
        except Exception as e:
            raise SyntaxError(f"Could not parse formatter {formatter!r}") from e

    def __init__(self, **params):
        super().__init__(**params)
        #  Items with key: (basename,ext) and value: (data, info)
        self._files = {}
        self._validate_formatters()


    def _dim_formatter(self, obj):
        if not obj: return ''
        key_dims = obj.traverse(lambda x: x.kdims, [UniformNdMapping])
        constant_dims = obj.traverse(lambda x: x.cdims)
        dims = []
        map(dims.extend, key_dims + constant_dims)
        dims = unique_iterator(dims)
        dim_strings = []
        for dim in dims:
            lower, upper = obj.range(dim.name)
            lower, upper = (dim.pprint_value(lower),
                            dim.pprint_value(upper))
            if lower == upper:
                range = dim.pprint_value(lower)
            else:
                range = f"{lower}-{upper}"
            formatters = {'name': dim.name, 'range': range,
                          'unit': dim.unit}
            dim_strings.append(self.dimension_formatter.format(**formatters))
        return '_'.join(dim_strings)


    def _validate_formatters(self):
        if not self.parse_fields(self.filename_formatter).issubset(self.ffields):
            raise Exception(f"Valid filename fields are: {','.join(sorted(self.ffields))}")
        elif not self.parse_fields(self.export_name).issubset(self.efields):
            raise Exception(f"Valid export fields are: {','.join(sorted(self.efields))}")
        try:
            time.strftime(self.timestamp_format, tuple(time.localtime()))
        except Exception as e:
            raise Exception("Timestamp format invalid") from e


    def add(self, obj=None, filename=None, data=None, info=None, **kwargs):
        """If a filename is supplied, it will be used. Otherwise, a
        filename will be generated from the supplied object. Note that
        if the explicit filename uses the {timestamp} field, it will
        be formatted upon export.

        The data to be archived is either supplied explicitly as
        'data' or automatically rendered from the object.

        """
        if info is None:
            info = {}
        if [filename, obj] == [None, None]:
            raise Exception("Either filename or a HoloViews object is "
                            "needed to create an entry in the archive.")
        elif obj is None and not self.parse_fields(filename).issubset({'timestamp'}):
            raise Exception("Only the {timestamp} formatter may be used unless an object is supplied.")
        elif [obj, data] == [None, None]:
            raise Exception("Either an object or explicit data must be "
                            "supplied to create an entry in the archive.")
        elif data and 'mime_type' not in info:
            raise Exception("The mime-type must be supplied in the info dictionary "
                            "when supplying data directly")

        self._validate_formatters()

        entries = []
        if data is None:
            for exporter in self.exporters:
                rendered = exporter(obj)
                if rendered is None: continue
                (data, new_info) = rendered
                info = dict(info, **new_info)
                entries.append((data, info))
        else:
            entries.append((data, info))

        for (data, info) in entries:
            self._add_content(obj, data, info, filename=filename)


    def _add_content(self, obj, data, info, filename=None):
        (unique_key, ext) = self._compute_filename(obj, info, filename=filename)
        self._files[(unique_key, ext)] = (data, info)


    def _compute_filename(self, obj, info, filename=None):
        if filename is None:
            hashfn = sha256()
            obj_str = 'None' if obj is None else self.object_formatter(obj)
            dimensions = self._dim_formatter(obj)
            dimensions = dimensions if dimensions else ''

            hashfn.update(obj_str.encode('utf-8'))
            label = sanitizer(getattr(obj, 'label', 'no-label'))
            group = sanitizer(getattr(obj, 'group', 'no-group'))
            format_values = {'timestamp': '{timestamp}',
                             'dimensions': dimensions,
                             'group':   group,
                             'label':   label,
                             'type':    obj.__class__.__name__,
                             'obj':     sanitizer(obj_str),
                             'SHA':     hashfn.hexdigest()}

            filename = self._format(self.filename_formatter,
                                    dict(info, **format_values))

        filename = self._normalize_name(filename)
        ext = info.get('file-ext', '')
        (unique_key, ext) = self._unique_name(filename, ext,
                                              self._files.keys(), force=True)
        return (unique_key, ext)

    def _zip_archive(self, export_name, files, root):
        archname = '.'.join(self._unique_name(export_name, 'zip', root))
        with zipfile.ZipFile(os.path.join(root, archname), 'w') as zipf:
            for (basename, ext), entry in files:
                filename = self._truncate_name(basename, ext)
                zipf.writestr(f'{export_name}/{filename}',Exporter.encode(entry))

    def _tar_archive(self, export_name, files, root):
        archname = '.'.join(self._unique_name(export_name, 'tar', root))
        with tarfile.TarFile(os.path.join(root, archname), 'w') as tarf:
            for (basename, ext), entry in files:
                filename = self._truncate_name(basename, ext)
                tarinfo = tarfile.TarInfo(f'{export_name}/{filename}')
                filedata = Exporter.encode(entry)
                tarinfo.size = len(filedata)
                tarf.addfile(tarinfo, BytesIO(filedata))

    def _single_file_archive(self, export_name, files, root):
        ((basename, ext), entry) = files[0]
        full_fname = f'{export_name}_{basename}'
        (unique_name, ext) = self._unique_name(full_fname, ext, root)
        filename = self._truncate_name(self._normalize_name(unique_name), ext=ext)
        fpath = os.path.join(root, filename)
        with open(fpath, 'wb') as f:
            f.write(Exporter.encode(entry))

    def _directory_archive(self, export_name, files, root):
        output_dir = os.path.join(root, self._unique_name(export_name,'', root)[0])
        if os.path.isdir(output_dir):
            shutil.rmtree(output_dir)
        os.makedirs(output_dir)

        for (basename, ext), entry in files:
            filename = self._truncate_name(basename, ext)
            fpath = os.path.join(output_dir, filename)
            with open(fpath, 'wb') as f:
                f.write(Exporter.encode(entry))


    def _unique_name(self, basename, ext, existing, force=False):
        """Find a unique basename for a new file/key where existing is
        either a list of (basename, ext) pairs or an absolute path to
        a directory.

        By default, uniqueness is enforced depending on the state of
        the unique_name parameter (for export names). If force is
        True, this parameter is ignored and uniqueness is guaranteed.

        """
        skip = False if force else (not self.unique_name)
        if skip: return (basename, ext)
        ext = '' if ext is None else ext
        if isinstance(existing, str):
            split = [os.path.splitext(el)
                     for el in os.listdir(os.path.abspath(existing))]
            existing = [(n, ex if not ex else ex[1:]) for (n, ex) in split]
        new_name, counter = basename, 1
        while (new_name, ext) in existing:
            new_name = basename+'-'+str(counter)
            counter += 1
        return (sanitizer(new_name), ext)


    def _truncate_name(self, basename, ext='', tail=10, join='...', maxlen=None):
        maxlen = self.max_filename if maxlen is None else maxlen
        max_len = maxlen-len(ext)
        if len(basename) > max_len:
            start = basename[:max_len-(tail + len(join))]
            end = basename[-tail:]
            basename = start + join + end
        filename = f'{basename}.{ext}' if ext else basename

        return filename


    def _normalize_name(self, basename):
        basename=re.sub('-+','-',basename)
        basename=re.sub('^[-,_]','',basename)
        return basename.replace(' ', '_')


    def export(self, timestamp=None, info=None):
        """Export the archive, directory or file.

        """
        if info is None:
            info = {}
        tval = tuple(time.localtime()) if timestamp is None else timestamp
        tstamp = time.strftime(self.timestamp_format, tval)

        info = dict(info, timestamp=tstamp)
        export_name = self._format(self.export_name, info)
        files = [((self._format(base, info), ext), val)
                 for ((base, ext), val) in self._files.items()]
        root = os.path.abspath(self.root)
        # Make directory and populate if multiple files and not packed
        if len(self) > 1 and not self.pack:
            self._directory_archive(export_name, files, root)
        elif len(files) == 1:
            self._single_file_archive(export_name, files, root)
        elif self.archive_format == 'zip':
            self._zip_archive(export_name, files, root)
        elif self.archive_format == 'tar':
            self._tar_archive(export_name, files, root)
        if self.flush_archive:
            self._files = {}

    def _format(self, formatter, info):
        filtered = {k:v for k,v in info.items()
                    if k in self.parse_fields(formatter)}
        return formatter.format(**filtered)

    def __len__(self):
        """The number of files currently specified in the archive.

        """
        return len(self._files)

    def __repr__(self):
        return self.param.pprint()

    def contents(self, maxlen=70):
        """Print the current (unexported) contents of the archive.

        """
        lines = []
        if len(self._files) == 0:
            print(f"Empty {self.__class__.__name__}")
            return

        fnames = [self._truncate_name(*k, maxlen=maxlen) for k in self._files]
        max_len = max([len(f) for f in fnames])
        for name,v in zip(fnames, self._files.values(), strict=None):
            mime_type = v[1].get('mime_type', 'no mime type')
            lines.append(f'{name.ljust(max_len)} : {mime_type}')
        print('\n'.join(lines))

    def listing(self):
        """Return a list of filename entries currently in the archive.

        """
        return [f'{f}.{ext}' if ext else f for (f,ext) in self._files.keys()]

    def clear(self):
        """Clears the file archive

        """
        self._files.clear()
