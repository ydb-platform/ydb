import pytz
from datetime import datetime

from openhtf.core.test_record import Outcome, PhaseOutcome
from openhtf.core.measurements import Outcome as MeasureOutcome
from openhtf.util.data import convert_to_base_types

from spintop.transforms import Transformer
from spintop.models import (
    SpintopTreeTestRecordBuilder
)

def duration_of(openhtf_record):
    return (openhtf_record['end_time_millis'] - openhtf_record['start_time_millis'])/1000.0

def create_outcome_validator(openhtf_record, outcome_cls=Outcome):
    return lambda expected_outcome: outcome_cls[openhtf_record['outcome']] == expected_outcome

class OpenHTFTestRecordTransformer(Transformer):
    
    def __call__(self, test_record):
        test_record = convert_to_base_types(test_record)
        builder = SpintopTreeTestRecordBuilder()
        
        outcome_is = create_outcome_validator(test_record)
        builder.set_top_level_information(
            start_datetime=datetime.utcfromtimestamp(test_record['start_time_millis']/1000.0).replace(tzinfo=pytz.timezone('UTC')),
            dut=test_record['dut_id'],
            testbench=test_record['metadata']['test_name'],
            duration=duration_of(test_record),
            outcome=dict(
                is_pass=outcome_is(Outcome.PASS),
                is_abort=outcome_is(Outcome.ABORTED)
            )
        )
        for phase in test_record['phases']:
            phase_outcome_is = create_outcome_validator(phase, PhaseOutcome)
            with builder.new_phase(
                name=phase['name'],
                outcome=phase_outcome_is(PhaseOutcome.PASS),
                duration=duration_of(phase),
            ) as phase_builder:
                for measure, value in phase['measurements'].items():
                    phase_builder.new_measure(
                        name=measure,
                        outcome=MeasureOutcome[value['outcome']] == MeasureOutcome.PASS,
                        value=value['measured_value']
                    )
        record = builder.build()
        return record