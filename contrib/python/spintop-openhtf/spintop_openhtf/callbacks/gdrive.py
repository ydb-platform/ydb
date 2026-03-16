import logging
import gspread
import datetime

from ..util.gdrive import FileNotFound, load_credentials_file, GoogleDrive, Worksheet, GoogleFolder
from openhtf.core.test_record import Outcome
from openhtf import conf


SCOPE = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
            
INDEX_FILE = 'index'
TESTS_TAB = 'Tests'
MEASUREMENTS_TAB = 'Measurements'
CONFIG_TAB = 'Config'
LOGS_TAB = 'Logs'

def load_credentials_file_with_scope(credentials_file):
    return load_credentials_file(credentials_file, SCOPE)

def create_google_drive_outputs(credentials, folder_name=None, share_with=[]):

    def on_output(test_record):
        if (test_record.outcome == Outcome.ABORTED
                and test_record.dut_id == conf.default_dut_id):
            # skip export
            pass
        else:
            gwrap = GoogleDrive(credentials)
            folder_id = gwrap.get_or_create_folder(folder_name, share_with=share_with)
            folder = GoogleFolder(gwrap, folder_id)

            outputs = [
                TestRecordToSheetOutput(folder),
                TestRecordToRowOutput(folder, INDEX_FILE)
            ]

            for output in outputs:
                output(test_record)

    return [on_output]

class TestRecordToSheetOutput(object):
    """ Creates a new google sheet in the constructor-argument folder.
    The file is named '{dut_id}-{timestamp}'.
    There are four tabs:
        - Tests
        - Measurements
        - Config
        - Logs
    """
    TEST_PHASE_COLUMNS = [
        ('name', lambda phase: phase.name),
        ('start_time', lambda phase: str(millis_to_datetime(phase.start_time_millis))),
        ('end_time', lambda phase: str(millis_to_datetime(phase.end_time_millis))),
        ('outcome', lambda phase: phase.outcome.name),
    ]

    def __init__(self, folder):
        self.folder = folder

    def __call__(self, test_record):
        filename = '{dut_id}-{timestamp}'.format(
            dut_id=test_record.dut_id, 
            timestamp=str(millis_to_datetime(test_record.start_time_millis))
        )
        
        spreadsheet = self.folder.get_or_create_spreadsheet(filename)
        file_id = spreadsheet.id
        file_url = 'https://docs.google.com/spreadsheets/d/%s/edit?usp=drivesdk' % file_id
        test_record.metadata['google_sheet_result_file_url'] = file_url
        self.write_test_phases(spreadsheet, test_record)
        self.write_test_measurements(spreadsheet, test_record)
        self.write_test_config(spreadsheet, test_record)
        self.write_logs(spreadsheet, test_record)

    def write_test_phases(self, spreadsheet, test_record):
        """ Writes the 'Tests' tab. """
        tab = Worksheet(spreadsheet, TESTS_TAB, headers=self.get_phase_header(), first=True)
        tab.append_rows([self.get_phase_values(phase) for phase in test_record.phases])

    def get_phase_header(self):
        return [col[0] for col in self.TEST_PHASE_COLUMNS]

    def get_phase_values(self, phase):
        return {key: fn(phase) for key, fn in self.TEST_PHASE_COLUMNS}

    def write_test_measurements(self, spreadsheet, test_record):
        """ Writes the 'Measurements' tab. """
        all_measures = []
        for phase in test_record.phases:
            for measure_name, measure in phase.measurements.items():
                measure_values = self.get_measurements_values(measure)
                measure_values['phase'] = phase.name
                all_measures.append(measure_values)

        tab = Worksheet(spreadsheet, MEASUREMENTS_TAB, headers=self.get_measurements_header())
        tab.append_rows(all_measures)

    def get_measurements_header(self):
        return ['phase', 'name', 'measured_value', 'outcome', 'validators']

    def get_measurements_values(self, measurement):
        measure = measurement.as_base_types()
        measure['validators'] = ' AND '.join(measure['validators'])
        return measure

    def write_test_config(self, spreadsheet, test_record):
        """ Writes the 'Config' tab. """
        header = ['name', 'value']
        
        tab = Worksheet(spreadsheet, CONFIG_TAB, headers=header)
        tab.append_rows([{'name': key, 'value': value} for key, value in test_record.metadata['config'].items()])

    def write_logs(self, spreadsheet, test_record):
        """ Writes the 'Logs' tab. """
        header = ['timestamp', 'timestamp_delta', 'logger_name', 'message']
        logs = []
        previous = None

        for lr in test_record.log_records:
            timestamp = millis_to_datetime(lr.timestamp_millis)
            if previous is None:
                delta = 0
            else:
                delta = (lr.timestamp_millis - previous)/1000
            
            previous = lr.timestamp_millis
            
            logs.append(
                {
                    'timestamp': str(timestamp),
                    'timestamp_delta': delta,
                    'logger_name': lr.logger_name,
                    'message': lr.message
                }
            )
        
        tab = Worksheet(spreadsheet, LOGS_TAB, headers=header)
        tab.append_rows(logs)
        
class TestRecordToRowOutput(object):
    """ Maintains an index file of all occured tests in a specific folder. """

    COLUMNS = [
        ('url', lambda tr: get_sheet_url(tr)),
        ('station_id', lambda tr: tr.station_id),
        ('dut_id', lambda tr: tr.dut_id),
        ('start_time', lambda tr: str(millis_to_datetime(tr.start_time_millis))),
        ('end_time', lambda tr: str(millis_to_datetime(tr.end_time_millis))),
        ('outcome', lambda tr: tr.outcome.name),
        ('error', lambda tr: format_error(tr))
    ]

    def __init__(self, folder, sheet_name):
        sheet = folder.get_or_create_spreadsheet(sheet_name)

        self.sheet = Worksheet(sheet, TESTS_TAB, headers=self.get_header(), first=True)
        self.measurements_sheet = Worksheet(sheet, MEASUREMENTS_TAB, headers=self.get_header())

    def __call__(self, test_record):
        self.add_test_row(test_record)
    
    def add_test_row(self, test_record):
        self.sheet.append_row(self.get_values(test_record))

    def get_header(self):
        return [col[0] for col in self.COLUMNS]

    def get_values(self, tr):
        return {key: fn(tr) for key, fn in self.COLUMNS}
        
def millis_to_datetime(millis):
    return datetime.datetime.utcfromtimestamp(millis/1000.0)

def next_available_row(worksheet):
    str_list = list(filter(lambda x: bool(x), worksheet.col_values(1)))
    return str(len(str_list)+1)

def format_error(test_record):
    errors = [':'.join(details) for details in test_record.outcome_details]
    return ' AND '.join(errors)

def get_sheet_url(test_record):
    file_url = test_record.metadata['google_sheet_result_file_url']
    return '=HYPERLINK("%s", "Link")' % file_url