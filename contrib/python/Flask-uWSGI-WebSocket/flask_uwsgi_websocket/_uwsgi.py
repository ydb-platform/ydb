import os
import sys

try:
    import uwsgi
except ImportError:
    uwsgi = None


def find_uwsgi():
    # Use environmental variable if set
    bin = os.environ.get('FLASK_UWSGI_BINARY')

    if not bin:
        # Try to find alongside Python executable, this is generally the one we want
        bin = "{0}/uwsgi".format(os.path.dirname(sys.executable))

        if not os.path.exists(bin):
            # Fallback to $PATH in case it's not in an obvious place
            bin = 'uwsgi'

    return bin

def run_uwsgi(app, debug=False, host='localhost', port=5000, uwsgi_binary=None, **kwargs):
    # Default to master = True
    if kwargs.get('master') is None:
        kwargs['master'] = True

    # Detect virtualenv
    if hasattr(sys, 'real_prefix'):
        # Make sure not otherwise specified
        if not any(k in kwargs for k in ['home', 'virtualenv', 'venv', 'pyhome']):
            # Pass along location of virtualenv
            kwargs['virtualenv'] = os.path.abspath(sys.prefix)

    # Booleans should be treated as empty values
    for k,v in kwargs.items():
        if v is True:
            kwargs[k] = ''

    uwsgi = uwsgi_binary or find_uwsgi()
    args = ' '.join(['--{0} {1}'.format(k,v) for k,v in kwargs.items()])
    cmd = '{0} --http {1}:{2} --http-websockets {3} --wsgi {4}'.format(uwsgi, host, port, args, app)

    # Set enviromental variable to trigger adding debug middleware
    if debug:
        cmd = 'FLASK_UWSGI_DEBUG=true {0} --python-autoreload 1'.format(cmd)

    # Run uwsgi with our args
    print('Running: {0}'.format(cmd))
    sys.exit(os.system(cmd))
