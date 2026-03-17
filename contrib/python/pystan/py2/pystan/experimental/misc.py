import logging
import re
import os

logger = logging.getLogger('pystan')

def fix_include(model_code):
    """Reformat `model_code` (remove whitespace) around the #include statements.
    
    Note
    ----
        A modified `model_code` is returned.

    Parameters
    ----------
    model_code : str
        Model code

    Returns
    -------
    str
        Reformatted model code
    
    Example
    -------
        
    >>> from pystan.experimental import fix_include
    >>> model_code = "parameters { #include myfile.stan \n..."
    >>> model_code_reformatted = fix_include(model_code)
    # "parameters {#include myfile.stan\n..."

    """
    pattern = r"(?<=\n)\s*(#include)\s*(\S+)\s*(?=\n)"
    model_code, n = re.subn(pattern, r"\1 \2", model_code)
    if n == 1:
        msg = "Made {} substitution for the model_code"
    else:
        msg = "Made {} substitutions for the model_code"
    logger.info(msg.format(n))
    return model_code
