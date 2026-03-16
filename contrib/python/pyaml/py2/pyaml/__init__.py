# -*- coding: utf-8 -*-
from __future__ import unicode_literals, print_function

import itertools as it, operator as op, functools as ft
from collections import defaultdict, OrderedDict, namedtuple
import os, sys, io, re

import yaml

if sys.version_info.major > 2: unicode = str


class PrettyYAMLDumper(yaml.dumper.SafeDumper):

	def __init__(self, *args, **kws):
		self.pyaml_force_embed = kws.pop('force_embed', False)
		self.pyaml_string_val_style = kws.pop('string_val_style', None)
		self.pyaml_sort_dicts = kws.pop('sort_dicts', True)
		return super(PrettyYAMLDumper, self).__init__(*args, **kws)

	def represent_odict(dumper, data):
		value = list()
		node = yaml.nodes.MappingNode(
			'tag:yaml.org,2002:map', value, flow_style=None )
		if dumper.alias_key is not None:
			dumper.represented_objects[dumper.alias_key] = node
		for item_key, item_value in data.items():
			node_key = dumper.represent_data(item_key)
			node_value = dumper.represent_data(item_value)
			value.append((node_key, node_value))
		node.flow_style = False
		return node

	def represent_undefined(dumper, data):
		if isinstance(data, tuple) and hasattr(data, '_make') and hasattr(data, '_asdict'):
			return dumper.represent_odict(data._asdict()) # assuming namedtuple
		elif isinstance(data, OrderedDict): return dumper.represent_odict(data)
		elif isinstance(data, dict): return dumper.represent_dict(data)
		elif callable(getattr(data, 'tolist', None)): return dumper.represent_data(data.tolist())
		return super(PrettyYAMLDumper, dumper).represent_undefined(data)

	def represent_dict(dumper, data):
		if not dumper.pyaml_sort_dicts: return dumper.represent_odict(data)
		return super(PrettyYAMLDumper, dumper).represent_dict(data)

	def serialize_node(self, node, parent, index):
		if self.pyaml_force_embed: self.serialized_nodes.clear()
		return super(PrettyYAMLDumper, self).serialize_node(node, parent, index)

	@staticmethod
	def pyaml_transliterate(string):
		if not all(ord(c) < 128 for c in string):
			from unidecode import unidecode
			string = unidecode(string)
		string_new = ''
		for ch in string:
			if '0' <= ch <= '9' or 'A' <= ch <= 'Z' or 'a' <= ch <= 'z' or ch in '-_': string_new += ch
			else: string_new += '_'
		return string_new.lower()

	def anchor_node(self, node, hint=list()):
		if node in self.anchors:
			if self.anchors[node] is None and not self.pyaml_force_embed:
				self.anchors[node] = self.generate_anchor(node)\
					if not hint else '{}'.format(
						self.pyaml_transliterate(
							'_-_'.join(map(op.attrgetter('value'), hint)) ) )
		else:
			self.anchors[node] = None
			if isinstance(node, yaml.nodes.SequenceNode):
				for item in node.value:
					self.anchor_node(item)
			elif isinstance(node, yaml.nodes.MappingNode):
				for key, value in node.value:
					self.anchor_node(key)
					self.anchor_node(value, hint=hint+[key])

PrettyYAMLDumper.add_representer(dict, PrettyYAMLDumper.represent_dict)
PrettyYAMLDumper.add_representer(defaultdict, PrettyYAMLDumper.represent_dict)
PrettyYAMLDumper.add_representer(OrderedDict, PrettyYAMLDumper.represent_odict)
PrettyYAMLDumper.add_representer(set, PrettyYAMLDumper.represent_list)
PrettyYAMLDumper.add_representer(None, PrettyYAMLDumper.represent_undefined)

if sys.version_info.major >= 3:
	try: import pathlib
	except ImportError: pass
	else:
		PrettyYAMLDumper.add_representer(
			type(pathlib.Path('')), lambda cls,o: cls.represent_data(str(o)) )


class UnsafePrettyYAMLDumper(PrettyYAMLDumper):

	def expect_block_sequence(self):
		self.increase_indent(flow=False, indentless=False)
		self.state = self.expect_first_block_sequence_item

	def expect_block_sequence_item(self, first=False):
		if not first and isinstance(self.event, yaml.events.SequenceEndEvent):
			self.indent = self.indents.pop()
			self.state = self.states.pop()
		else:
			self.write_indent()
			self.write_indicator('-', True, indention=True)
			self.states.append(self.expect_block_sequence_item)
			self.expect_node(sequence=True)

	def check_simple_key(self):
		res = super(UnsafePrettyYAMLDumper, self).check_simple_key()
		self.analysis.allow_flow_plain = False # not always-set for keys in newer pyyaml
		return res

	def choose_scalar_style(self, _re1=re.compile(':(\s|$)')):
		is_dict_key = self.states[-1] == self.expect_block_mapping_simple_value
		if is_dict_key:
			# Don't mess-up (replace) styles for dict keys, if possible
			if self.pyaml_string_val_style: self.event.style = 'plain'
		else:
			# Make sure we don't create "key: null" mapping accidentally
			if self.event.value.endswith(':'): self.event.style = "'"
		if self.event.style != 'plain':
			return super(UnsafePrettyYAMLDumper, self).choose_scalar_style()
		s = self.event.value
		if s.startswith('- ') or _re1.search(s): return "'"
		if self.analysis and not self.analysis.allow_flow_plain:
			# Can be a mapping key - disallow spaces in those
			if ' ' in s: return "'"

	def represent_stringish(dumper, data):
		# Will crash on bytestrings with weird chars in them,
		#  because we can't tell if it's supposed to be e.g. utf-8 readable string
		#  or an arbitrary binary buffer, and former one *must* be pretty-printed
		# PyYAML's Representer.represent_str does the guesswork and !!binary or !!python/str
		# Explicit crash on any bytes object might be more sane, but also annoying
		# Use something like base64 to encode such buffer values instead
		# Having such binary stuff pretty much everywhere on unix (e.g. paths) kinda sucks
		data = unicode(data) # read the comment above

		# Try to use '|' style for multiline data,
		#  quoting it with 'literal' if lines are too long anyway,
		#  not sure if Emitter.analyze_scalar can also provide useful info here
		style = dumper.pyaml_string_val_style
		if not style:
			style = 'plain'
			if '\n' in data or not data or data == '-' or data[0] in '!&*[' or '#' in data:
				style = 'literal'
				if '\n' in data[:-1]:
					for line in data.splitlines():
						if len(line) > dumper.best_width: break
					else: style = '|'

		return yaml.representer.ScalarNode('tag:yaml.org,2002:str', data, style=style)

for str_type in {bytes, unicode}:
	UnsafePrettyYAMLDumper.add_representer(
		str_type, UnsafePrettyYAMLDumper.represent_stringish )

UnsafePrettyYAMLDumper.add_representer(
	type(None), lambda s,o: s.represent_scalar('tag:yaml.org,2002:null', '') )

def add_representer(*args, **kws):
	PrettyYAMLDumper.add_representer(*args, **kws)
	UnsafePrettyYAMLDumper.add_representer(*args, **kws)


def dump_add_vspacing(buff, vspacing):
	'Post-processing to add some nice-ish spacing for deeper map/list levels.'
	if isinstance(vspacing, int):
		vspacing = ['\n']*(vspacing+1)
	buff.seek(0)
	result = list()
	for line in buff:
		level = 0
		line = line.decode('utf-8')
		result.append(line)
		if ':' in line or re.search(r'---(\s*$|\s)', line):
			while line.startswith('  '):
				level, line = level + 1, line[2:]
			if len(vspacing) > level and len(result) != 1:
				vspace = vspacing[level]
				result.insert( -1, vspace
					if not isinstance(vspace, int) else '\n'*vspace )
	buff.seek(0), buff.truncate()
	buff.write(''.join(result).encode('utf-8'))


def dump_all(data, *dump_args, **dump_kws):
	return dump(data, *dump_args, multiple_docs=True, **dump_kws)

def dump( data, dst=unicode, safe=False, force_embed=False, vspacing=None,
		string_val_style=None, sort_dicts=True, multiple_docs=False, **pyyaml_kws ):
	buff = io.BytesIO()
	Dumper = PrettyYAMLDumper if safe else UnsafePrettyYAMLDumper
	Dumper = ft.partial( Dumper,
		force_embed=force_embed, string_val_style=string_val_style, sort_dicts=sort_dicts )
	if not multiple_docs: data = [data]
	else: pyyaml_kws.setdefault('explicit_start', True)
	yaml.dump_all( data, buff, Dumper=Dumper,
		default_flow_style=False, allow_unicode=True, encoding='utf-8', **pyyaml_kws )

	if vspacing is not None:
		dump_add_vspacing(buff, vspacing)

	buff = buff.getvalue()
	if dst is bytes: return buff
	elif dst is unicode: return buff.decode('utf-8')
	else:
		try: dst.write(b'') # tests if dst is unicode- or bytestream
		except: dst.write(buff.decode('utf-8'))
		else: dst.write(buff)

def dumps(data, **dump_kws):
	return dump(data, dst=bytes, **dump_kws)

def pprint(*data, **dump_kws):
	dst = dump_kws.pop('file', dump_kws.pop('dst', sys.stdout))
	if len(data) == 1: data, = data
	dump(data, dst=dst, **dump_kws)

p, _p = pprint, print
print = pprint # pyaml.print() won't work without "from __future__ import print_function"
