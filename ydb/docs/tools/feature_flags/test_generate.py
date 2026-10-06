import json
from pathlib import Path
import tempfile
import unittest

from google.protobuf import descriptor_pb2 as pb
import yaml

import generate as gen


class DefaultsTests(unittest.TestCase):
    def schema(self):
        schema = pb.FileDescriptorSet()
        file = schema.file.add(name='flags.proto', package='NKikimrConfig', syntax='proto2')
        canonical = file.message_type.add(name='TFeatureFlags')
        tribool = canonical.enum_type.add(name='Tribool')
        for name, number in [('UNSET', 0), ('VALUE_TRUE', 1), ('VALUE_FALSE', 2)]:
            tribool.value.add(name=name, number=number)
        for name, default in [('EnableDisabled', 'false'), ('EnableEnabled', 'true'), ('EnableImplicit', None)]:
            field = canonical.field.add(name=name, number=len(canonical.field) + 1, type=pb.FieldDescriptorProto.TYPE_BOOL,
                                        label=pb.FieldDescriptorProto.LABEL_OPTIONAL)
            if default is not None:
                field.default_value = default
        canonical.field.add(name='EnableUnknown', number=4, type=pb.FieldDescriptorProto.TYPE_ENUM,
                            type_name=gen.TRIBOOL, label=pb.FieldDescriptorProto.LABEL_OPTIONAL)
        canonical.field.add(name='RepeatedBool', number=5, type=pb.FieldDescriptorProto.TYPE_BOOL,
                            label=pb.FieldDescriptorProto.LABEL_REPEATED)
        canonical.reserved_name.append('EnableRemoved')
        nested = canonical.nested_type.add(name='Other')
        nested.field.add(name='EnableEnabled', number=1, type=pb.FieldDescriptorProto.TYPE_BOOL,
                         label=pb.FieldDescriptorProto.LABEL_OPTIONAL, default_value='false')
        public = schema.file.add(name='public.proto', package='Ydb', syntax='proto3')
        wrapper = public.message_type.add(name='FeatureFlag')
        status = wrapper.enum_type.add(name='Status')
        for name, number in [('STATUS_UNSPECIFIED', 0), ('ENABLED', 1), ('DISABLED', 2)]:
            status.value.add(name=name, number=number)
        table = public.message_type.add(name='Table')
        table.field.add(name='UseSnapshot', number=1, type=pb.FieldDescriptorProto.TYPE_ENUM,
                        type_name=gen.API_STATUS, label=pb.FieldDescriptorProto.LABEL_OPTIONAL)
        return schema

    def test_defaults_nested_identity_and_tristate(self):
        variables, catalog = gen.variables_from_schema(self.schema(), {'flags.proto', 'public.proto'})
        canonical = variables['feature_flags']
        self.assertIs(canonical['enable_disabled'], False)
        self.assertIs(canonical['enable_enabled'], True)
        self.assertIs(canonical['enable_implicit'], False)
        self.assertIsNone(canonical['enable_unknown'])
        self.assertIsNone(canonical['enable_removed'])
        self.assertNotIn('repeated_bool', canonical)
        self.assertIs(variables['proto_flags']['NKikimrConfig']['TFeatureFlags']['Other']['enable_enabled'], False)
        self.assertEqual(variables['proto_flags']['Ydb']['Table']['use_snapshot'], 'STATUS_UNSPECIFIED')
        self.assertEqual(len(catalog), 6)

    def test_name_collisions_fail(self):
        schema = self.schema()
        schema.file[0].message_type[0].field.add(name='enable_enabled', number=9, type=pb.FieldDescriptorProto.TYPE_BOOL,
                                              label=pb.FieldDescriptorProto.LABEL_OPTIONAL)
        with self.assertRaisesRegex(gen.Error, 'collision'):
            gen.variables_from_schema(schema, {'flags.proto'})

    def test_missing_canonical_fails(self):
        with self.assertRaisesRegex(gen.Error, 'Required message'):
            gen.variables_from_schema(self.schema(), {'public.proto'})

    def test_real_protoc_defaults_comments_and_custom_options(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / 'fixture.proto').write_text('''syntax = "proto2";
import "google/protobuf/descriptor.proto";
package NKikimrConfig;
extend google.protobuf.FieldOptions { optional bool RequireRestart = 56681; }
message TFeatureFlags {
  // optional bool Fake = 20 [default = true];
  optional bool EnableTrue = 1 [default = true, (RequireRestart) = true];
  optional bool EnableImplicit = 2;
  reserved "EnableRetired";
}
''')
            variables, _ = gen.variables_from_schema(gen.compile_schema(root, ['fixture.proto']), {'fixture.proto'})
            self.assertEqual(variables['feature_flags'], {'enable_true': True, 'enable_implicit': False, 'enable_retired': None})

    def test_cpp_literals_and_unknown_expression(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / 'flags.h').write_text('''// static constexpr bool Fake = true;
namespace N { struct Flags { static constexpr bool EnableTrue = true; static constexpr bool EnableFalse = false;
static constexpr bool EnableComputed = SOME_BUILD_OPTION; }; }''')
            values, catalog, missing = gen.cpp_variables(root, [{'path': 'flags.h', 'namespace': 'N', 'structs': ['Flags']}])
            self.assertEqual(values['N']['Flags'], {'enable_true': True, 'enable_false': False, 'enable_computed': None})
            self.assertFalse(missing)
            self.assertEqual(len(catalog), 3)

    def test_missing_cpp_structure_fails(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / 'flags.h').write_text('namespace N { struct Other {}; }')
            with self.assertRaisesRegex(gen.Error, r'registered C\+\+ struct'):
                gen.cpp_variables(root, [{'path': 'flags.h', 'namespace': 'N', 'structs': ['Flags']}])

    def test_explicit_removed_messages_and_cpp_fields(self):
        variables = {'feature_flags': {}, 'proto_flags': {}, 'compile_time_flags': {}}
        gen.retired_variables(variables, [
            {'namespace': 'feature_flags', 'field': 'enable_removed'},
            {'namespace': 'proto_flags', 'message': 'Old.Type', 'field': 'enable_removed'},
            {'namespace': 'compile_time_flags', 'message': 'N::Flags', 'field': 'old_field'},
        ])
        self.assertIsNone(variables['feature_flags']['enable_removed'])
        self.assertIsNone(variables['proto_flags']['Old']['Type']['enable_removed'])
        self.assertIsNone(variables['compile_time_flags']['N']['Flags']['old_field'])

    def test_active_field_cannot_be_marked_removed(self):
        variables = {'feature_flags': {'enable_known': False}, 'proto_flags': {}, 'compile_time_flags': {}}
        with self.assertRaisesRegex(gen.Error, 'active field'):
            gen.retired_variables(variables, [{'namespace': 'feature_flags', 'field': 'enable_known'}])

    def test_unset_active_field_cannot_be_marked_removed(self):
        variables = {'feature_flags': {'enable_known': None}, 'proto_flags': {}, 'compile_time_flags': {}}
        with self.assertRaisesRegex(gen.Error, 'active field'):
            gen.retired_variables(variables, [{'namespace': 'feature_flags', 'field': 'enable_known'}],
                                  {('feature_flags', '', 'enable_known')})

    def test_branch_local_sources_change_the_default(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / 'flags.proto'
            for expected, literal in [(False, 'false'), (True, 'true')]:
                source.write_text('syntax = "proto2"; package NKikimrConfig; message TFeatureFlags {'
                                  f'optional bool EnableBranchLocal = 1 [default = {literal}];' + '}')
                values, _ = gen.variables_from_schema(gen.compile_schema(root, ['flags.proto']), {'flags.proto'})
                self.assertIs(values['feature_flags']['enable_branch_local'], expected)

    def test_missing_version_sources_and_required_schema(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            path = root / 'ydb/core/protos/feature_flags.proto'
            path.parent.mkdir(parents=True)
            path.write_text('')
            config = {'proto_sources': ['ydb/core/protos/feature_flags.proto', 'new-version-only.proto']}
            present, missing = gen.source_files(root, config)
            self.assertEqual(present, ['ydb/core/protos/feature_flags.proto'])
            self.assertEqual(missing, ['new-version-only.proto'])
            path.unlink()
            with self.assertRaisesRegex(gen.Error, 'Missing required'):
                gen.source_files(root, config)


class PresetTests(unittest.TestCase):
    def test_preserves_other_presets_comments_and_is_idempotent(self):
        text = 'default:\n  existing: true # keep this comment\n\ninternal:\n  existing: false\n'
        variables = {'feature_flags': {'enabled': True, 'disabled': False, 'removed': None},
                     'proto_flags': {'A.B': {'enabled': False}}, 'compile_time_flags': {}}
        updated = gen.inject_presets(text, variables)
        self.assertIn('existing: true # keep this comment', updated)
        loaded = yaml.safe_load(updated)
        self.assertEqual(loaded['default']['feature_flags'], variables['feature_flags'])
        self.assertEqual(loaded['internal'], {'existing': False})
        self.assertEqual(gen.inject_presets(updated, variables), updated)

    def test_unterminated_block_fails(self):
        with self.assertRaisesRegex(gen.Error, 'Malformed'):
            gen.inject_presets('default:\n  x: true\n' + gen.BEGIN, {'feature_flags': {}})

    def test_existing_manual_namespace_fails(self):
        with self.assertRaisesRegex(gen.Error, 'Manually defined'):
            gen.inject_presets('default:\n  feature_flags: {}\n', {'feature_flags': {}})

    def test_duplicate_keys_fail(self):
        with self.assertRaisesRegex(gen.Error, 'Duplicate'):
            gen.inject_presets('default:\n  x: true\n  x: false\n', {'feature_flags': {}})


class ReferenceTests(unittest.TestCase):
    def validate(self, text):
        variables = {'feature_flags': {'enable_known': False, 'enable_removed': None},
                     'proto_flags': {'A': {'B': {'enable_known': True}}},
                     'compile_time_flags': {'N': {'Flags': {'enable_known': False}}}}
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / 'page.md').write_text(text)
            return gen.validate_references(root, variables)

    def test_all_namespaces_and_removed_flag(self):
        self.assertEqual(self.validate('''{% if feature_flags.enable_known == false %}warning{% endif %}
{% if feature_flags.enable_removed == false %}hidden{% endif %}
{% if proto_flags.A.B.enable_known == false %}hidden{% endif %}
{{ compile_time_flags.N.Flags.enable_known }}
'''), 4)

    def test_unknown_flag_rejected(self):
        with self.assertRaisesRegex(gen.Error, 'unknown flag'):
            self.validate('{% if feature_flags.enable_typo == false %}warning{% endif %}')

    def test_qualified_unknown_type_rejected(self):
        with self.assertRaisesRegex(gen.Error, 'unknown flag'):
            self.validate('{% if proto_flags.Unknown.Type.enable_known == false %}warning{% endif %}')

    def test_examples_ignored_and_actual_template_checked(self):
        self.assertEqual(self.validate('''```markdown
{% if feature_flags.enable_xxx == false %}
```
~~~markdown
{{ feature_flags.enable_example }}
~~~
{% if feature_flags.enable_known == false %}actual{% endif %}
'''), 1)

    def test_dynamic_lookup_rejected(self):
        with self.assertRaisesRegex(gen.Error, 'static, qualified'):
            self.validate('{{ feature_flags[variable] }}')


if __name__ == '__main__':
    unittest.main()
