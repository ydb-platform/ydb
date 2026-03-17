"""Hatch build hook to compile Portable Object (.po) translations to .mo."""

from pathlib import Path
import subprocess

from hatchling.builders.hooks.plugin.interface import BuildHookInterface
from hatchling.plugin import hookimpl


def compile_translations() -> None:
    """Compile the .po files into .mo files that contain the translations."""
    i18n_dir = Path(__file__).parent.absolute()

    paths = list(i18n_dir.glob('*/LC_MESSAGES'))

    for p in paths:
        LANG = p.parent.name

        for component in ['nbclassic', 'nbui']:
            po_file = p / f'{component}.po'
            mo_file = p / f'{component}.mo'

            if not po_file.exists():
                print(f"Warning: {po_file} does not exist, skipping.")
                continue

            cmd = [
                'pybabel', 'compile',
                '-D', component,
                '-f',
                '-l', LANG,
                '-i', str(po_file),
                '-o', str(mo_file)
            ]

            try:
                subprocess.run(cmd, check=True)
                print(f"Compiled: {po_file} -> {mo_file}")
            except subprocess.CalledProcessError as e:
                print(f"Error compiling {po_file}: {e}")


class CompileTranslationsHook(BuildHookInterface):
    """Build hook for compiling translation files."""

    def finalize(self, *args) -> None:
        """Finalize the build hook - compile translation files."""
        compile_translations()


@hookimpl
def hatch_register_build_hook():
    return CompileTranslationsHook

if __name__ == "__main__":
    compile_translations()
