import logging
import pathlib

import polib

log = logging.getLogger(__name__)
base_dir = pathlib.Path("../cron_descriptor/locale")

for po_file in base_dir.rglob("*.po"):
    mo_file = po_file.with_suffix(".mo")
    log.info("Compiling %s → %s", po_file, mo_file)
    po = polib.pofile(po_file)
    po.save_as_mofile(str(mo_file))
