import os

from openhtf import util
from openhtf.util import data

from openhtf.core.test_record import Outcome
from openhtf.output.callbacks.json_factory import OutputToJSON


class LocalStorageOutput(OutputToJSON):
    def __init__(self, folder_pattern, indent=4):
        self.folder_pattern = folder_pattern
        super().__init__(indent=indent)

    def __call__(self, test_record):
        if test_record.is_started():
            record_dict = data.convert_to_base_types(
                test_record, ignore_keys=('code_info', 'phases', 'log_records'))

            output_folder = util.format_string(self.folder_pattern, record_dict)

            if not os.path.exists(output_folder):
                os.makedirs(output_folder)

            with open(os.path.join(output_folder, 'result.json'), 'w+') as outfile:
                outfile.write(self.serialize_test_record(test_record))
            
            for phase in test_record.phases:
                for attachment_name, attachment in phase.attachments.items():
                    with open(os.path.join(output_folder, attachment_name), 'wb+') as outfile:
                        outfile.write(attachment.data)
