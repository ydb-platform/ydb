"""A setup module for the google apis common protos

See:
https://packaging.python.org/en/latest/distributing.html
https://github.com/pypa/sampleproject
"""

import setuptools

from setuptools import setup, find_packages

install_requires = [
    'protobuf >= 3.6.0'
]

extras_require = {
    'grpc': ['grpcio >= 1.0.0']
}

setuptools.setup(
    name='googleapis-common-protos',
    version='{PROTOS_VERSION}',

    author='Google LLC',
    author_email='googleapis-packages@google.com',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: Implementation :: CPython',
    ],
    description='Common protobufs used in Google APIs',
    long_description=open('README.rst').read(),
    install_requires=install_requires,
    extras_require=extras_require,
    license='Apache-2.0',
    packages=find_packages(),
    namespace_packages=['google', 'google.logging', ],
    url='https://github.com/googleapis/googleapis'
)
