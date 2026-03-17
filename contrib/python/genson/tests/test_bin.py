import unittest
import json
import os
from subprocess import Popen, PIPE
from genson import SchemaBuilder

BASE_SCHEMA = {"$schema": SchemaBuilder.DEFAULT_URI}
FIXTURE_PATH = os.path.join(os.path.dirname(__file__), 'fixtures')
SHORT_USAGE = """\
usage: genson [-h] [--version] [-d DELIM] [-e ENCODING] [-i SPACES]
              [-s SCHEMA] [-$ SCHEMA_URI]
              ..."""


def fixture(filename):
    return os.path.join(FIXTURE_PATH, filename)


def stderr_message(message):
    return '{}\ngenson: error: {}\n'.format(SHORT_USAGE, message)


def run(args=tuple(), stdin_data=None):
    """
    Run the ``genson`` executable as a subprocess and return
    (stdout, stderr).
    """
    full_args = ['python', '-m', 'genson']
    full_args.extend(args)
    env = os.environ.copy()
    env['COLUMNS'] = '80'  # set width for deterministic text wrapping

    genson_process = Popen(
        full_args, env=env, stdout=PIPE, stderr=PIPE,
        stdin=PIPE if stdin_data is not None else None)
    if stdin_data is not None:
        stdin_data = stdin_data.encode('utf-8')
    (stdout, stderr) = genson_process.communicate(stdin_data)
    genson_process.wait()

    if isinstance(stdout, bytes):
        stdout = stdout.decode('utf-8')
    if isinstance(stderr, bytes):
        stderr = stderr.decode('utf-8')
    return (stdout, stderr)


class TestBasic(unittest.TestCase):

    def test_empty_input(self):
        (stdout, stderr) = run(stdin_data='')
        self.assertEqual(stderr, '')
        self.assertEqual(json.loads(stdout), BASE_SCHEMA)

    def test_empty_object_stdin(self):
        (stdout, stderr) = run(stdin_data='{}')
        self.assertEqual(stderr, '')
        self.assertEqual(
            json.loads(stdout),
            dict({"type": "object"}, **BASE_SCHEMA))

    def test_empty_object_file(self):
        (stdout, stderr) = run([fixture('empty.json')])
        self.assertEqual(stderr, '')
        self.assertEqual(
            json.loads(stdout),
            BASE_SCHEMA)

    def test_basic_schema_file(self):
        (stdout, stderr) = run(['-s', fixture('base_schema.json')])
        self.assertEqual(stderr, '')
        self.assertEqual(
            json.loads(stdout),
            BASE_SCHEMA)


class TestError(unittest.TestCase):
    maxDiff = 1000
    BAD_JSON_FILE = fixture('not_json.txt')
    BAD_JSON_MESSAGE = stderr_message(
        'invalid JSON in %s: Expecting value: line 1 column 1 (char 0)'
        % BAD_JSON_FILE)

    def test_no_input(self):
        (stdout, stderr) = run()
        self.assertEqual(stderr, stderr_message(
            'noting to do - no schemas or objects given'))
        self.assertEqual(stdout, '')

    def test_object_not_json(self):
        (stdout, stderr) = run([self.BAD_JSON_FILE])
        self.assertEqual(stderr, self.BAD_JSON_MESSAGE)
        self.assertEqual(stdout, '')

    def test_schema_not_json(self):
        (stdout, stderr) = run(['-s', self.BAD_JSON_FILE])
        self.assertEqual(stderr, self.BAD_JSON_MESSAGE)
        self.assertEqual(stdout, '')


class TestDelimiter(unittest.TestCase):

    def test_delim_newline(self):
        (stdout, stderr) = run(['-d', 'newline'],
                               stdin_data='{"hi":"there"}\n{"hi":5}')
        self.assertEqual(stderr, '')
        self.assertEqual(
            json.loads(stdout),
            dict({"required": ["hi"], "type": "object", "properties": {
                "hi": {"type": ["integer", "string"]}}}, **BASE_SCHEMA))

    def test_delim_auto_empty(self):
        (stdout, stderr) = run(['-d', ''], stdin_data='{"hi":"there"}{"hi":5}')
        self.assertEqual(stderr, '')
        self.assertEqual(
            json.loads(stdout),
            dict({"required": ["hi"], "type": "object", "properties": {
                "hi": {"type": ["integer", "string"]}}}, **BASE_SCHEMA))

    def test_delim_auto_whitespace(self):
        (stdout, stderr) = run(['-d', ''],
                               stdin_data='{"hi":"there"} \n\t{"hi":5}')
        self.assertEqual(stderr, '')
        self.assertEqual(
            json.loads(stdout),
            dict({"required": ["hi"], "type": "object", "properties": {
                "hi": {"type": ["integer", "string"]}}}, **BASE_SCHEMA))


class TestEncoding(unittest.TestCase):

    def test_encoding_unicode(self):
        (stdout, stderr) = run(
            ['-e', 'utf-8', fixture('utf-8.json')])
        self.assertEqual(stderr, '')
        self.assertEqual(
            json.loads(stdout),
            dict({"type": "string"}, **BASE_SCHEMA))

    def test_encoding_cp1252(self):
        (stdout, stderr) = run(
            ['-e', 'cp1252', fixture('cp1252.json')])
        self.assertEqual(stderr, '')
        self.assertEqual(
            json.loads(stdout),
            dict({"type": "string"}, **BASE_SCHEMA))
