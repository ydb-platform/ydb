import os

TEST_PROJECT_DISTRIBUTION_DATA = {
    "name": "TestProject",
    "version": "0.1",
    "packages": ["project"],
}
import yatest.common as yc
this_dir = os.path.abspath(os.path.dirname(yc.source_path(__file__)))
data_dir = os.path.join(this_dir, 'data')
project_dir = os.path.join(data_dir, 'project')
i18n_dir = os.path.join(project_dir, 'i18n')


def get_po_file_path(locale):
    return os.path.join(i18n_dir, locale, 'LC_MESSAGES', 'messages.po')
