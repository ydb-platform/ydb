import random
import time

import openhtf as htf
from openhtf.output.callbacks import json_factory

from openhtf.util import validators

from spintop.generators import Generator

def normal(mu, sigma):
    return random.normalvariate(mu, sigma)

@htf.measures('first', 'second', 'third', 'fourth')
def lots_of_measurements(test, base_mu, base_sigma):
    for index, measurement in enumerate(('first', 'second', 'third', 'fourth')):
        test.measurements[measurement] = normal(base_mu - ((5*index)%13), base_sigma+(index))

@htf.PhaseOptions()
def sleep_sometime(test, sleep_length):
    if sleep_length:
        time.sleep(sleep_length)

@htf.PhaseOptions()
def random_failure(test, failure_odd):
    if random.uniform(0, 1.0) < failure_odd:
        return htf.PhaseResult.STOP
        
@htf.measures('something_else')
def singular_test(test):
    test.measurements.something_else = 6.2
    

base_mu_gen = lambda index: 5+(index*0.25)
base_sigma_gen = lambda index: 2
test_name_gen = lambda index: 'basicgen_test'
dut_id_gen = lambda index: 'dut%d' % ((index%25) if 17 < (index%25) < 20 else index % 17)
additionnal_phases_gen = lambda index: [singular_test] if (index%25) == 7 else []
sleep_gen = lambda index: 1 if (index%25) == 10 or (index%25) == 17 else None


class OpenHTFTestsGenerator(Generator):
    def __call__(self, count=25):
        # We instantiate our OpenHTF test with the phases we want to run as args.
        self.tests = []
        def callback(test_record):
            self.tests.append(test_record)
        
        for index in range(count):
            
            phases = [
                lots_of_measurements.with_args(base_mu=base_mu_gen(index), base_sigma=base_sigma_gen(index)),
                random_failure.with_args(failure_odd=0.1),
                sleep_sometime.with_args(sleep_length=sleep_gen(index))
            ] + additionnal_phases_gen(index)
            
            test = htf.Test(*phases, test_name=test_name_gen(index))

            test.add_output_callbacks(callback)

            dut_id = dut_id_gen(index)
            test.execute(test_start=lambda: dut_id)
        
        for test in self.tests:
            # Requires a generator
            yield test
    
    
  
    