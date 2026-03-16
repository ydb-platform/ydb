import os
import sys
from setuptools import find_packages, setup

# Hack to silence atexit traceback in newer python versions
try:
    import multiprocessing
except ImportError:
    pass

DESCRIPTION = (
    'MongoEngine is a Python Object-Document '
    'Mapper for working with MongoDB.'
)

try:
    with open('README.rst') as fin:
        LONG_DESCRIPTION = fin.read()
except Exception:
    LONG_DESCRIPTION = None


def get_version(version_tuple):
    """Return the version tuple as a string, e.g. for (0, 10, 7),
    return '0.10.7'.
    """
    return '.'.join(map(str, version_tuple))


# Dirty hack to get version number from monogengine/__init__.py - we can't
# import it as it depends on PyMongo and PyMongo isn't installed until this
# file is read
init = os.path.join(os.path.dirname(__file__), 'mongoengine', '__init__.py')
version_line = list(filter(lambda l: l.startswith('VERSION'), open(init)))[0]

VERSION = get_version(eval(version_line.split('=')[-1]))

CLASSIFIERS = [
    'Development Status :: 4 - Beta',
    'Intended Audience :: Developers',
    'License :: OSI Approved :: MIT License',
    'Operating System :: OS Independent',
    'Programming Language :: Python',
    "Programming Language :: Python :: 2",
    "Programming Language :: Python :: 2.7",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.5",
    "Programming Language :: Python :: 3.6",
    "Programming Language :: Python :: Implementation :: CPython",
    "Programming Language :: Python :: Implementation :: PyPy",
    'Topic :: Database',
    'Topic :: Software Development :: Libraries :: Python Modules',
]

extra_opts = {
    'packages': find_packages(exclude=['tests', 'tests.*']),
    'tests_require': ['nose', 'coverage==4.2', 'blinker', 'Pillow>=2.0.0']
}
if sys.version_info[0] == 3:
    extra_opts['use_2to3'] = True
    if 'test' in sys.argv or 'nosetests' in sys.argv:
        extra_opts['packages'] = find_packages()
        extra_opts['package_data'] = {
            'tests': ['fields/mongoengine.png', 'fields/mongodb_leaf.png']}
else:
    extra_opts['tests_require'] += ['python-dateutil']

setup(
    name='mongoengine',
    version=VERSION,
    author='Harry Marr',
    author_email='harry.marr@gmail.com',
    maintainer="Stefan Wojcik",
    maintainer_email="wojcikstefan@gmail.com",
    url='http://mongoengine.org/',
    download_url='https://github.com/MongoEngine/mongoengine/tarball/master',
    license='MIT',
    include_package_data=True,
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    platforms=['any'],
    classifiers=CLASSIFIERS,
    install_requires=['pymongo>=2.7.1', 'six'],
    test_suite='nose.collector',
    **extra_opts
)
