import fnmatch
import os
from pathlib import Path

from django.db import migrations, models  # noqa F401


class Migration(migrations.Migration):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dependencies = [("token_blacklist", "0010_fix_migrate_to_bigautofield")]

    operations = []
