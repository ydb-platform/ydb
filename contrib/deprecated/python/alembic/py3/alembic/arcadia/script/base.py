import re
from pkgutil import iter_modules
from importlib import import_module

from ... import util
from ...script import revision
from ...script.base import Script, ScriptDirectory


_sourceless_rev_file = re.compile(r"(?!\.\#|__init__)(.*\.py)(c|o)?$")
_only_source_rev_file = re.compile(r"(?!\.\#|__init__)(.*\.py)$")
_legacy_rev = re.compile(r"([a-f0-9]+)\.py$")
_mod_def_re = re.compile(r"(upgrade|downgrade)_([a-z0-9]+)")
_slug_re = re.compile(r"\w+")
_default_file_template = "%(rev)s_%(slug)s"
_split_on_space_comma = re.compile(r", *|(?: +)")


class ArcadiaScriptDirectory(ScriptDirectory):
    def __init__(
        self,
        dir,  # noqa
        file_template=_default_file_template,
        truncate_slug_length=40,
        version_locations=None,
        sourceless=False,
        output_encoding="utf-8",
        timezone=None,
        hook_config=None,
    ):
        self.dir = dir
        self.file_template = file_template
        self.version_locations = version_locations
        self.truncate_slug_length = truncate_slug_length or 40
        self.sourceless = sourceless
        self.output_encoding = output_encoding
        self.revision_map = revision.RevisionMap(self._load_revisions)
        self.timezone = timezone
        self.hook_config = hook_config

    @util.memoized_property
    def _version_locations(self):
        if self.version_locations:
            return list(self.version_locations)
        else:
            return ['.'.join((self.dir, 'versions'))]

    def _load_revisions(self):
        if self.version_locations:
            paths = self._version_locations
        else:
            paths = [self.versions]

        dupes = set()
        for vers in paths:
            for path in ArcadiaScript._list_modules(self, vers):
                if path in dupes:
                    util.warn(
                        "File %s loaded twice! ignoring. Please ensure "
                        "version_locations is unique." % path
                    )
                    continue
                dupes.add(path)
                script = ArcadiaScript._from_modulename(self, vers, path)
                if script is None:
                    continue
                yield script

    @classmethod
    def from_config(cls, config): # DONE
        script_module = config.get_main_option('script_module')
        if script_module is None:
            raise util.CommandError("No 'script_module' key "
                                    "found in configuration.")
        truncate_slug_length = config.get_main_option("truncate_slug_length")
        if truncate_slug_length is not None:
            truncate_slug_length = int(truncate_slug_length)

        version_modules = config.get_main_option("version_modules")
        if version_modules:
            version_modules = _split_on_space_comma.split(version_modules)

        return cls(
            script_module,
            file_template=config.get_main_option(
                'file_template',
                _default_file_template),
            truncate_slug_length=truncate_slug_length,
            sourceless=config.get_main_option("sourceless") == "true",
            output_encoding=config.get_main_option("output_encoding", "utf-8"),
            version_locations=version_modules,
            timezone=config.get_main_option("timezone")
        )

    def run_env(self):
        """Run the script environment.

        This basically runs the ``env.py`` script present
        in the migration environment.   It is called exclusively
        by the command functions in :mod:`alembic.command`.


        """
        import_module('.'.join((self.dir, 'env')))


class ArcadiaScript(Script):
    def __init__(self, module, rev_id, path):
        self.module = module
        self.path = path
        revision.Revision.__init__(
            self,
            rev_id,
            module.down_revision,
            branch_labels=util.to_tuple(
                getattr(module, 'branch_labels', None), default=()),
            dependencies=util.to_tuple(
                getattr(module, 'depends_on', None), default=())
        )

    @staticmethod
    def find_modules_in_path(import_path):
        try:
            module = import_module(import_path)
            for imp, name, is_pkg in iter_modules(path=module.__path__):
                if name[0] not in '_.~':
                    yield name
        except ImportError:
            pass

    @classmethod
    def _list_modules(cls, scriptdir, module_path):
        assert not scriptdir.sourceless
        return ['.'.join((module_path, module)) for module in cls.find_modules_in_path(module_path)]

    @classmethod
    def _from_modulename(cls, scriptdir, dir_, modulename):
        module = import_module(modulename)

        if not hasattr(module, "revision"):
            # attempt to get the revision id from the script name,
            # this for legacy only
            m = _legacy_rev.match(modulename)
            if not m:
                raise util.CommandError(
                    "Could not determine revision id from filename %s. "
                    "Be sure the 'revision' variable is "
                    "declared inside the script (please see 'Upgrading "
                    "from Alembic 0.1 to 0.2' in the documentation)."
                    % modulename)
            else:
                revision = m.group(1)
        else:
            revision = module.revision
        return ArcadiaScript(module, revision, modulename)
