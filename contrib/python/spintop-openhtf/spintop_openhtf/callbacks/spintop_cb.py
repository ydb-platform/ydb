from functools import lru_cache

from openhtf import conf

from spintop import Spintop

from spintop_openhtf.transforms.openhtf_fmt import OpenHTFTestRecordTransformer

conf.declare(
    'spintop_api_url', 
    description='The spintop API URL to use. Overrides the default value from env (SPINTOP_API_URI).', 
    default_value=None
)

conf.declare(
    'spintop_org_id', 
    description='The spintop organization which will receive test results.', 
    default_value=None
)

@lru_cache()
def get_spintop(spintop=None):
    if spintop is None:
        spintop = Spintop(api_url=conf['spintop_api_url'], org_id=conf['spintop_org_id'])
    return spintop

def spintop_callback_factory(spintop=None):
    transform = OpenHTFTestRecordTransformer()
    def spintop_callback(test):
        record = transform(test)
        get_spintop(spintop).records.update([record])
    
    return spintop_callback