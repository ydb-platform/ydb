"""Activate coverage at python startup if appropriate.

The python site initialisation will ensure that anything we import
will be removed and not visible at the end of python startup.  However
we minimise all work by putting these init actions in this separate
module and only importing what is needed when needed.

For normal python startup when coverage should not be activated the pth
file checks a single env var and does not import or call the init fn
here.

For python startup when an ancestor process has set the env indicating
that code coverage is being collected we activate coverage based on
info passed via env vars.
"""


UNIQUE_SEP = '084031f3d2994d40a88c8b699b69e148'

import cov_core  # noqa: register multiprocessing handler


def init():

    # Any errors encountered should only prevent coverage from
    # starting, it should not cause python to complain that importing
    # of site failed.
    try:

        # Only continue if ancestor process has set everything needed in
        # the env.
        import os
        cov_source = os.environ.get('COV_CORE_SOURCE')
        cov_data_file = os.environ.get('COV_CORE_DATA_FILE')
        cov_config = os.environ.get('COV_CORE_CONFIG')
        if cov_data_file and cov_config:

            # Import what we need to activate coverage.
            import socket
            import random
            import coverage

            # Determine all source roots.
            if cov_source == '':
                cov_source = None
            else:
                cov_source = cov_source.split(UNIQUE_SEP)

            # Produce a unique suffix for this process in the same
            # manner as coverage.
            data_suffix = '%s.%s.%s' % (socket.gethostname(),
                                        os.getpid(),
                                        random.randint(0, 999999))

            # Activate coverage for this process.
            cov = coverage.coverage(source=cov_source,
                                    data_file=cov_data_file,
                                    data_suffix=data_suffix,
                                    config_file=cov_config,
                                    auto_data=True)
            cov.erase()
            cov.start()
            return cov

    except Exception:
        pass
