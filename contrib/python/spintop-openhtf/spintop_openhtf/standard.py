""" Standardized additions to openhtf """
import enum

#
# STANDARD CONF
#

def init_conf():
    pass


#
# ENVIRONMENT
#

EnvironmentType = enum.Enum('EnvironmentType', [
    'DEVELOPMENT',
    'PRODUCTION'
])

def get_env():
    # TODO : something more advanced
    return EnvironmentType.DEVELOPMENT

def is_development_env():
    return get_env() == EnvironmentType.DEVELOPMENT

def is_production_env():
    return get_env() == EnvironmentType.PRODUCTION
    
