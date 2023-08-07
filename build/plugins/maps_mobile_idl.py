import os
import re
from collections import namedtuple

from _common import sort_by_keywords

Framework = namedtuple(
    'Framework', ['cpp_namespace_tokens', 'java_class_path', 'objc_framework_name', 'objc_framework_prefix']
)


def _get_proto_header_file(proto_file_name):
    return proto_file_name.split('.')[0] + '.pb.h'


def _get_appended_values(unit, key):
    value = []
    raw_value = unit.get(key)
    if raw_value:
        value = filter(lambda x: len(x) > 0, raw_value.split(' '))
        assert len(value) == 0 or value[0] == '$' + key
    return value[1:] if len(value) > 0 else value


def _load_framework_file_list(unit):
    frameworks = [
        unit.resolve(unit.resolve_arc_path(os.sep.join(path.split(os.sep)[1:])))
        for path in unit.get('MAPKIT_IDL_FRAMEWORK_FILES').split(' ')
    ]
    return frameworks


def _require_framework_entry(entry, framework):
    if entry not in framework:
        raise Exception('No {} entry in {} framework'.format(entry, framework))


def _read_framework(unit, framework_file):
    file_path = unit.resolve(framework_file)
    result = {}
    with open(file_path, 'r') as f:
        lineId = 0
        for line in f:
            lineId += 1
            tokens = line.split('=')
            if len(tokens) != 2:
                raise Exception('Malformed idl framework file {} line {}'.format(framework_file, lineId))
            result[tokens[0].strip()] = tokens[1].strip()

    _require_framework_entry('CPP_NAMESPACE', result)
    _require_framework_entry('JAVA_PACKAGE', result)
    _require_framework_entry('OBJC_FRAMEWORK', result)
    _require_framework_entry('OBJC_FRAMEWORK_PREFIX', result)
    return Framework(
        result['CPP_NAMESPACE'].split('.'),
        result['JAVA_PACKAGE'],
        result['OBJC_FRAMEWORK'],
        result['OBJC_FRAMEWORK_PREFIX'],
    )


def _read_frameworks(unit):
    framework_file_list = _load_framework_file_list(unit)
    result = {}
    for file_name in framework_file_list:
        name = file_name.split(os.sep)[-1].split('.')[0]
        result[name] = _read_framework(unit, file_name)
    return result


def _extract_by_regexp(line, regexp):
    re_match = regexp.search(line)
    if not re_match:
        return None
    return re_match.group(1)


class RegExp:
    OBJC_INFIX = r'\bobjc_infix\s*([^\s]+);'

    IMPORT = r'^import\s+"([^"]+)"'

    WEAK_INTERFACE = r'\bweak_ref\s+interface\b'
    SHARED_INTERFACE = r'\bshared_ref\s+interface\b'
    STRONG_INTERFACE = r'^\s*interface\b'
    NATIVE_LISTENER = r'\bnative\s+listener\b'
    STATIC_INTERFACE = r'\bstatic\s+interface\b'
    VIEW_DELEGATE = r'\bview_delegate\b'

    CUSTOM_PROTO_HEADER = r'^\s*protoconv\s+"([^"]+)"\s*$'
    BASED_ON_PROTO_START = r'\bbased\s+on(\s|$)'
    BASED_ON_PROTO = r'\bbased\s+on\s+"([^"]+)"\s*:'

    CUSTOM_CPP_HEADER = r'^\s*cpp\s+"([^"]+)"\s*$'
    STRUCT = r'\bstruct\b'

    LITE_STRUCT = r'\blite\s+struct\b'
    BRIDGED_STRUCT = r'^(\s*options|\s*(navi_)?serializable|\s*abstract)*\s*struct\s+'

    LAMBDA_LISTENER = r'\blambda\s+listener\b'
    LISTENER = r'^\s*listener\s+'
    PLATFORM_INTERFACE = r'platform\s+interface'

    VARIANT = r'\bvariant\b'

    OPTIONAL = r'\boptional\b'
    INT_64 = r'\bint64\b'
    STRING = r'\bstring\b'
    POINT = r'\bpoint\b'
    BYTES = r'\bbytes\b'
    VECTOR = r'\bvector\b'
    DICTIONARY = r'\bdictionary\b'
    ANY = r'\bany[^_]'
    ENUM = r'\benum\b'
    TIME = r'\b(time_interval|abs_timestamp|rel_timestamp)\b'
    BITMAP = r'\bbitmap\b'
    VIEW_PROVIDER = r'\bview_provider\b'
    IMAGE_PROVIDER = r'\bimage_provider\b'
    ANIMATED_IMAGE_PROVIDER = r'\banimated_image_provider\b'
    MODEL_PROVIDER = r'\bmodel_provider\b'
    ANIMATED_MODEL_PROVIDER = r'\banimated_model_provider\b'
    COLOR = r'\bcolor\b'
    PLATFORM_VIEW = r'\bplatform_view\b'
    ERROR = r'\b(runtime\.)?Error\b'
    TYPE_DICTIONARY = r'\btype_dictionary\b'

    SERIALIZABLE = r'\bserializable\b'
    NAVI_SERIALIZABLE = r'\bnavi_serializable\b'


class OutputType:
    BASE_HEADER = 1
    STRUCT_SOURCE = 2
    PROTOCONV_HEADER = 3
    PROTOCONV_SOURCE = 4
    ANDROID_HEADER = 5
    ANDROID_SOURCE = 6
    IOS_HEADER = 7
    IOS_SOURCE = 8
    IOS_PRIVATE_HEADER = 9
    IOS_BINDING_SOURCE = 10
    DART_CPP_HEADER = 11
    DART_CPP_SOURCE = 12
    DART_SOURCE = 13
    DART_SOURCE_PRIVATE = 14


class OutputNameGenerator:
    def __init__(self, file_path, frameworks):
        path_tokens = file_path.split(os.sep)
        framework_name = path_tokens[0]
        self._framework = frameworks[framework_name]
        self._cpp_namespace_tokens = self._framework.cpp_namespace_tokens + path_tokens[1:-1]
        file_name = path_tokens[-1]
        self._cpp_name = file_name.split('.')[0]

        name_tokens = self._cpp_name.split('_')
        self._objc_name_core = ''.join((self._capitalize(token) for token in name_tokens))
        self._objc_name = self._framework.objc_framework_prefix + self._objc_name_core

    def set_objc_infix(self, objc_infix):
        if self._objc_name_core.startswith(objc_infix):
            self._objc_name = self._framework.objc_framework_prefix + self._objc_name_core
        else:
            self._objc_name = self._framework.objc_framework_prefix + objc_infix + self._objc_name_core

    def is_header(self, output_type):
        return output_type in [
            OutputType.BASE_HEADER,
            OutputType.PROTOCONV_HEADER,
            OutputType.ANDROID_HEADER,
            OutputType.IOS_HEADER,
            OutputType.IOS_PRIVATE_HEADER,
            OutputType.DART_CPP_HEADER
        ]

    def _cpp_file_name(self, extension, additional_tokens=[]):
        path_tokens = self._cpp_namespace_tokens + additional_tokens + [self._cpp_name + extension]
        return os.path.join(*path_tokens)

    def _dart_public_file_name(self, extension):
        return self._cpp_file_name(extension)

    def _dart_private_file_name(self, extension):
        path_tokens = ['src'] + self._cpp_namespace_tokens + [self._cpp_name + '_private' + extension]
        return os.path.join(*path_tokens)

    def _objc_file_name(self, extension, additional_tokens=[]):
        path_tokens = [self._framework.objc_framework_name] + additional_tokens + [self._objc_name + extension]
        return os.path.join(*path_tokens)

    def _capitalize(self, word):
        return word[:1].upper() + word[1:]

    def generate_name(self, output_type):
        if output_type is OutputType.BASE_HEADER:
            return self._cpp_file_name('.h')

        if output_type is OutputType.STRUCT_SOURCE:
            return self._cpp_file_name('.cpp')

        if output_type is OutputType.PROTOCONV_HEADER:
            return self._cpp_file_name('.conv.h')

        if output_type is OutputType.PROTOCONV_SOURCE:
            return self._cpp_file_name('.conv.cpp')

        if output_type is OutputType.ANDROID_HEADER:
            return self._cpp_file_name('_binding.h', ['internal', 'android'])

        if output_type is OutputType.ANDROID_SOURCE:
            return self._cpp_file_name('_binding.cpp', ['internal', 'android'])

        if output_type is OutputType.IOS_HEADER:
            return self._objc_file_name('.h')

        if output_type is OutputType.IOS_SOURCE:
            return self._objc_file_name('.m')

        if output_type is OutputType.IOS_PRIVATE_HEADER:
            return self._objc_file_name('_Private.h', ['Internal'])

        if output_type is OutputType.IOS_BINDING_SOURCE:
            return self._objc_file_name('_Binding.mm')

        if output_type is OutputType.DART_CPP_SOURCE:
            return self._cpp_file_name('_dart_binding.cpp')

        if output_type is OutputType.DART_SOURCE:
            return self._dart_public_file_name('.dart')

        if output_type is OutputType.DART_CPP_HEADER:
            return self._cpp_file_name('_dart_binding.h')

        if output_type is OutputType.DART_SOURCE_PRIVATE:
            return self._dart_private_file_name('.dart')

    def generate_path(self, output_type):
        name = self.generate_name(output_type)

        if self.is_header(output_type):
            return os.path.join('include', name)

        return os.path.join('impl', name)


class ProcessContext:
    def __init__(self, unit, frameworks, file_paths):
        self.unit = unit
        self.frameworks = frameworks
        self.file_paths = file_paths
        self.is_dart = unit.enabled("MAPKIT_DART_IDL")
        self.is_ios = unit.enabled("OS_IOS")
        self.is_android = unit.enabled("OS_ANDROID")
        self.output_name_generator = None
        self.add_generated_output_includes = unit.enabled("H_CPP_IDL")

    def runtime_include(self, include_name):
        name_tokens = self.frameworks['runtime'].cpp_namespace_tokens + [include_name]
        return os.path.join(*name_tokens)

    def runtime_objc_import(self, import_name):
        return os.path.join(
            self.frameworks['runtime'].objc_framework_name,
            self.frameworks['runtime'].objc_framework_prefix + import_name,
        )


class BaseRule:
    def __init__(self, context):
        self.context = context

    def start_file(self, file_path):
        pass

    def process_line(self, line):
        pass

    def get_output_types(self):
        return set()

    def get_output_includes(self):
        return set()


class ObjcInfixRule(BaseRule):
    def __init__(self, context):
        BaseRule.__init__(self, context)
        self._found_infix = False
        self._reg_exp = re.compile(RegExp.OBJC_INFIX)

    def start_file(self, file_path):
        BaseRule.start_file(self, file_path)
        self.context.output_name_generator.set_objc_infix('')
        self._found_infix = False

    def process_line(self, line):
        BaseRule.process_line(self, line)
        if self._found_infix:
            return

        infix = _extract_by_regexp(line, self._reg_exp)
        if infix:
            self._found_infix = True
            self.context.output_name_generator.set_objc_infix(infix)


class ImportRule(BaseRule):
    def __init__(self, context):
        BaseRule.__init__(self, context)
        self._imports = set()
        self._import_reg_exp = re.compile(RegExp.IMPORT)

    def start_file(self, file_path):
        self._imports = set()

    def process_line(self, line):
        BaseRule.process_line(self, line)
        idl_import = _extract_by_regexp(line, self._import_reg_exp)
        if idl_import:
            self._imports.add(idl_import)

    def get_output_includes(self):
        result = set()
        for idl_import in self._imports:
            if idl_import in self.context.file_paths:
                continue

            name_generator = OutputNameGenerator(idl_import, self.context.frameworks)
            result.add(name_generator.generate_name(OutputType.BASE_HEADER))

        return result


class DefaultRule(BaseRule):
    def __init__(self, context):
        BaseRule.__init__(self, context)

    def get_output_types(self):
        result = set()
        result.add(OutputType.BASE_HEADER)

        if self.context.is_dart:
            result.add(OutputType.DART_SOURCE)
            result.add(OutputType.DART_CPP_SOURCE)
            result.add(OutputType.DART_CPP_HEADER)
            result.add(OutputType.DART_SOURCE_PRIVATE)

        if self.context.is_ios:
            result.add(OutputType.IOS_HEADER)
            result.add(OutputType.IOS_SOURCE)

        return result

    def get_output_includes(self):
        result = set()

        if self.context.is_dart:
            result.add(self.context.runtime_include('bindings/flutter/base_types.h'))
            result.add(self.context.runtime_include('bindings/flutter/exception.h'))
            result.add(self.context.runtime_include('bindings/flutter/export.h'))
            result.add(self.context.runtime_include('bindings/flutter/to_native.h'))
            result.add(self.context.runtime_include('bindings/flutter/to_native_fwd.h'))
            result.add(self.context.runtime_include('bindings/flutter/to_platform.h'))
            result.add(self.context.runtime_include('bindings/flutter/to_platform_fwd.h'))
            result.add(self.context.runtime_include('bindings/flutter/vector.h'))
            result.add(self.context.runtime_include('bindings/flutter/string_map.h'))
            result.add(self.context.runtime_include('bindings/flutter/helper.h'))
            result.add(self.context.runtime_include('bindings/flutter/meta_type.h'))
            result.add(self.context.runtime_include('bindings/flutter/native.h'))
            return result

        result.add('yandex/maps/export.h')
        result.add(self.context.runtime_include('assert.h'))
        result.add(self.context.runtime_include('exception.h'))
        result.add(self.context.runtime_include('bindings/traits.h'))

        if self.context.is_ios:
            result.add(self.context.runtime_include('bindings/platform.h'))
            result.add('Foundation/Foundation.h')
            result.add(self.context.runtime_include('ios/object.h'))
            result.add(self.context.runtime_include('bindings/ios/to_native.h'))
            result.add(self.context.runtime_include('bindings/ios/to_platform.h'))
            result.add(self.context.runtime_include('ios/exception.h'))
            result.add(self.context.runtime_objc_import('Subscription.h'))

        if self.context.is_android:
            result.add(self.context.runtime_include('bindings/platform.h'))
            result.add(self.context.runtime_include('android/object.h'))
            result.add(self.context.runtime_include('bindings/android/to_native.h'))
            result.add(self.context.runtime_include('bindings/android/to_platform.h'))
            result.add(self.context.runtime_include('exception.h'))

        return result


class CheckRule(BaseRule):
    def __init__(
        self,
        context,
        output_types=set(),
        output_includes=set(),
        ios_output_types=set(),
        ios_output_includes=set(),
        android_output_types=set(),
        android_output_includes=set(),
    ):
        BaseRule.__init__(self, context)
        self._output_types = output_types
        self._output_includes = output_includes
        self._ios_output_types = ios_output_types
        self._ios_output_includes = ios_output_includes
        self._android_output_types = android_output_types
        self._android_output_includes = android_output_includes

    def triggered_on_file(self):
        pass

    def get_output_types(self):
        result = set()
        if self.triggered_on_file():
            result.update(self._output_types)

            if self.context.is_ios:
                result.update(self._ios_output_types)

            if self.context.is_android:
                result.update(self._android_output_types)

        return result

    def get_output_includes(self):
        result = set()

        if self.triggered_on_file():
            result.update(self._output_includes)

            if self.context.is_ios:
                result.update(self._ios_output_includes)

            if self.context.is_android:
                result.update(self._android_output_includes)

        return result


class OrRule(CheckRule):
    def __init__(self, check_rules, *args, **kwargs):
        CheckRule.__init__(self, *args, **kwargs)
        self._rules = check_rules

    def triggered_on_file(self):
        return any((rule.triggered_on_file() for rule in self._rules))


class AndRule(CheckRule):
    def __init__(self, check_rules, *args, **kwargs):
        CheckRule.__init__(self, *args, **kwargs)
        self._rules = check_rules

    def triggered_on_file(self):
        return all((rule.triggered_on_file() for rule in self._rules))


class RegExpRule(CheckRule):
    def __init__(self, reg_exp_string, *args, **kwargs):
        CheckRule.__init__(self, *args, **kwargs)
        self._reg_exp = re.compile(reg_exp_string)
        self._reg_exp_found_file = False

    def start_file(self, file_path):
        CheckRule.start_file(self, file_path)
        self._reg_exp_found_file = False

    def process_line(self, line):
        CheckRule.process_line(self, line)
        if self._reg_exp_found_file:
            return

        if self._reg_exp.search(line) is not None:
            self._reg_exp_found_file = True

    def triggered_on_file(self):
        return self._reg_exp_found_file


class ProtoRule(BaseRule):
    def __init__(self, context):
        BaseRule.__init__(self, context)
        self._file_has_non_custom_proto = False
        self._currently_custom_proto = False
        self._currently_based_on = False
        self._running_line = ''
        self._custom_proto_headers = set()
        self._proto_files = set()

        self._custom_proto_reg_exp = re.compile(RegExp.CUSTOM_PROTO_HEADER)
        self._based_on_proto_start_reg_exp = re.compile(RegExp.BASED_ON_PROTO_START)
        self._based_on_proto_reg_exp = re.compile(RegExp.BASED_ON_PROTO)

    def start_file(self, file_path):
        BaseRule.start_file(self, file_path)
        self._currently_custom_proto = False
        self._file_has_non_custom_proto = False
        self._currently_based_on = False
        self._running_line = ''

    def process_line(self, line):
        BaseRule.process_line(self, line)
        proto_header = _extract_by_regexp(line, self._custom_proto_reg_exp)
        if proto_header:
            self._custom_proto_headers.add(proto_header)
            self._currently_based_on = False
            self._running_line = ''

            self._currently_custom_proto = True
            return

        if self._based_on_proto_start_reg_exp.search(line) is not None:
            self._currently_based_on = True
            self._running_line = ''

        if self._currently_based_on:
            self._running_line += '\n' + line
            proto_file = _extract_by_regexp(self._running_line, self._based_on_proto_reg_exp)
            if proto_file:
                self._currently_based_on = False
                self._running_line = ''
                self._proto_files.add(proto_file)

                if self._currently_custom_proto:
                    self._currently_custom_proto = False
                else:
                    self._file_has_non_custom_proto = True

    def get_output_types(self):
        if self._file_has_non_custom_proto:
            return {OutputType.PROTOCONV_HEADER, OutputType.PROTOCONV_SOURCE}
        return set()

    def get_output_includes(self):
        result = set()
        result.update(self._custom_proto_headers)
        result.update((proto_file.split('.')[0] + '.pb.h' for proto_file in self._proto_files))

        if self._file_has_non_custom_proto:
            result.update({'vector'})

        return result


class StructImplementationRule(BaseRule):
    def __init__(self, context):
        BaseRule.__init__(self, context)
        self._file_has_non_custom_struct = False
        self._custom_cpp_headers = set()
        self._currently_custom_struct = False

        self._custom_cpp_header_reg_exp = re.compile(RegExp.CUSTOM_CPP_HEADER)
        self._struct_reg_exp = re.compile(RegExp.STRUCT)

    def start_file(self, file_path):
        BaseRule.start_file(self, file_path)
        self._currently_custom_struct = False
        self._file_has_non_custom_struct = False

    def process_line(self, line):
        BaseRule.process_line(self, line)

        cpp_header = _extract_by_regexp(line, self._custom_cpp_header_reg_exp)
        if cpp_header:
            self._custom_cpp_headers.add(cpp_header)
            self._currently_custom_struct = True
            return

        if not self._file_has_non_custom_struct:
            if self._struct_reg_exp.search(line) is not None:
                if self._currently_custom_struct:
                    self._currently_custom_struct = False
                else:
                    self._file_has_non_custom_struct = True

    def get_output_types(self):
        result = set()
        if self._file_has_non_custom_struct:
            result.add(OutputType.STRUCT_SOURCE)
            if self.context.is_ios:
                result.add(OutputType.IOS_BINDING_SOURCE)

        return result

    def get_output_includes(self):
        return self._custom_cpp_headers


class IdlFileProcessor:
    def __init__(self, unit, frameworks, file_paths):
        self._context = ProcessContext(unit, frameworks, file_paths)
        self._resolved_idl_dir = unit.resolve(unit.resolve_arc_path(unit.path()))
        self._outputs = set()
        self._output_includes = set()

        self._rules = set()

        self._rules.add(ObjcInfixRule(self._context))
        self._rules.add(DefaultRule(self._context))
        self._rules.add(ImportRule(self._context))
        self._rules.add(ProtoRule(self._context))
        self._rules.add(StructImplementationRule(self._context))

        view_delegate_rule = self._create_reg_exp_rule(
            RegExp.VIEW_DELEGATE, output_includes={self._context.runtime_include('view/view_delegate.h')}
        )

        weak_interface_rule = self._create_or_rule(
            rules={
                self._create_reg_exp_rule(RegExp.WEAK_INTERFACE),
                view_delegate_rule,
            },
            output_includes={'boost/any.hpp', 'memory', self._context.runtime_include('platform_holder.h')},
        )

        strong_interface_rule = self._create_or_rule(
            rules={
                self._create_reg_exp_rule(RegExp.STRONG_INTERFACE),
                self._create_reg_exp_rule(RegExp.NATIVE_LISTENER),
            }
        )

        non_static_interface_rule = self._create_or_rule(
            rules={self._create_reg_exp_rule(RegExp.SHARED_INTERFACE), strong_interface_rule, weak_interface_rule},
            ios_output_types={OutputType.IOS_PRIVATE_HEADER},
        )

        # interface rule
        self._create_or_rule(
            rules={self._create_reg_exp_rule(RegExp.STATIC_INTERFACE), non_static_interface_rule},
            ios_output_types={OutputType.IOS_BINDING_SOURCE},
            android_output_types={OutputType.ANDROID_SOURCE},
            ios_output_includes={'memory'},
        )

        bridged_struct_rule = self._create_reg_exp_rule(
            RegExp.BRIDGED_STRUCT,
            output_includes={'memory', self._context.runtime_include('bindings/platform.h')},
            android_output_includes={self._context.runtime_include('bindings/android/internal/new_serialization.h')},
        )

        # struct rule
        self._create_or_rule(
            rules={self._create_reg_exp_rule(RegExp.LITE_STRUCT), bridged_struct_rule},
            ios_output_types={OutputType.IOS_PRIVATE_HEADER},
            android_output_types={OutputType.ANDROID_HEADER, OutputType.ANDROID_SOURCE},
            ios_output_includes={self._context.runtime_objc_import('NativeObject.h')},
        )

        lambda_listener_rule = self._create_reg_exp_rule(
            RegExp.LAMBDA_LISTENER,
            output_includes={'functional'},
            android_output_includes={self._context.runtime_include('verify_and_run.h')},
            ios_output_includes={self._context.runtime_include('verify_and_run.h')},
        )

        # listener rule
        self._create_or_rule(
            rules={
                self._create_reg_exp_rule(RegExp.PLATFORM_INTERFACE),
                self._create_reg_exp_rule(RegExp.LISTENER),
                lambda_listener_rule,
            },
            ios_output_types={OutputType.IOS_PRIVATE_HEADER, OutputType.IOS_BINDING_SOURCE},
            android_output_types={OutputType.ANDROID_HEADER, OutputType.ANDROID_SOURCE},
            output_includes={'memory'},
            android_output_includes={'string', self._context.runtime_include('verify_and_run.h')},
            ios_output_includes={self._context.runtime_include('verify_and_run.h')},
        )

        if self._context.unit.enabled("MAPS_MOBILE_USE_STD_VARIANT"):
            variant_header = 'variant'
            variant_serialization_header = self.context.runtime_include('serialization/variant.hpp')
        else:
            variant_header = 'boost/variant.hpp'
            variant_serialization_header = 'boost/serialization/variant.hpp'

        variant_rule = self._create_reg_exp_rule(
            RegExp.VARIANT,
            ios_output_types={OutputType.IOS_PRIVATE_HEADER, OutputType.IOS_BINDING_SOURCE},
            output_includes={variant_header, 'boost/variant/recursive_wrapper.hpp'},
            ios_output_includes={
                self._context.runtime_include('bindings/ios/to_platform_fwd.h'),
                self._context.runtime_include('bindings/ios/to_native_fwd.h'),
                'type_traits',
            },
        )

        optional_rule = self._create_reg_exp_rule(RegExp.OPTIONAL, output_includes={'optional'})
        # int64 rule
        self._create_reg_exp_rule(RegExp.INT_64, output_includes={'cstdint'})

        string_rule = self._create_reg_exp_rule(
            RegExp.STRING, output_includes={'string', self._context.runtime_include('bindings/platform.h')}
        )

        point_rule = self._create_reg_exp_rule(
            RegExp.POINT,
            output_includes={'Eigen/Geometry', self._context.runtime_include('bindings/point_traits.h')},
            android_output_includes={
                self._context.runtime_include('bindings/android/point_to_native.h'),
                self._context.runtime_include('bindings/android/point_to_platform.h'),
            },
            ios_output_includes={
                self._context.runtime_include('bindings/ios/point_to_native.h'),
                self._context.runtime_include('bindings/ios/point_to_platform.h'),
                'UIKit/UIKit.h',
            },
        )

        bytes_rule = self._create_reg_exp_rule(RegExp.BYTES, output_includes={'cstdint', 'vector'})

        vector_rule = self._create_reg_exp_rule(
            RegExp.VECTOR,
            output_includes={'memory', self._context.runtime_include('bindings/platform.h')},
            android_output_includes={
                self._context.runtime_include('bindings/android/vector_to_native.h'),
                self._context.runtime_include('bindings/android/vector_to_platform.h'),
            },
            ios_output_includes={
                self._context.runtime_include('bindings/ios/vector_to_native.h'),
                self._context.runtime_include('bindings/ios/vector_to_platform.h'),
            },
        )

        dictionary_rule = self._create_reg_exp_rule(
            RegExp.DICTIONARY,
            output_includes={'memory', self._context.runtime_include('bindings/platform.h')},
            android_output_includes={
                self._context.runtime_include('bindings/android/dictionary_to_native.h'),
                self._context.runtime_include('bindings/android/dictionary_to_platform.h'),
            },
            ios_output_includes={
                self._context.runtime_include('bindings/ios/dictionary_to_native.h'),
                self._context.runtime_include('bindings/ios/dictionary_to_platform.h'),
            },
        )

        # any rule
        self._create_reg_exp_rule(
            RegExp.ANY, output_includes={'boost/any.hpp', self._context.runtime_include('bindings/platform.h')}
        )

        time_rule = self._create_reg_exp_rule(RegExp.TIME, output_includes={self._context.runtime_include('time.h')})

        # bitmap rule
        self._create_reg_exp_rule(
            RegExp.BITMAP,
            output_includes={self._context.runtime_include('platform_bitmap.h')},
            ios_output_includes={'UIKit/UIKit.h'},
        )

        # image_provider rule
        self._create_reg_exp_rule(
            RegExp.IMAGE_PROVIDER,
            output_includes={self._context.runtime_include('image/image_provider.h')},
            android_output_includes={self._context.runtime_include('image/android/image_provider_binding.h')},
            ios_output_includes={self._context.runtime_include('image/ios/image_provider_binding.h'), 'UIKit/UIKit.h'},
        )

        # animated_image_provider rule
        self._create_reg_exp_rule(
            RegExp.ANIMATED_IMAGE_PROVIDER,
            output_includes={self._context.runtime_include('image/animated_image_provider.h')},
            android_output_includes={self._context.runtime_include('image/android/animated_image_provider_binding.h')},
            ios_output_includes={
                self._context.runtime_include('image/ios/animated_image_provider_binding.h'),
                self._context.runtime_objc_import('AnimatedImageProvider.h'),
            },
        )

        # model_provider and animated_model_provider rules
        model_provider_rule = self._create_reg_exp_rule(
            RegExp.MODEL_PROVIDER, output_includes={self._context.runtime_include('model/model_provider.h')}
        )
        animated_model_provider_rule = self._create_reg_exp_rule(
            RegExp.ANIMATED_MODEL_PROVIDER,
            output_includes={self._context.runtime_include('model/animated_model_provider.h')},
        )
        if not unit.enabled('MAPS_MOBILE_PUBLIC_API'):
            self._create_or_rule(
                rules={model_provider_rule},
                android_output_includes={self._context.runtime_include('model/android/model_provider_binding.h')},
                ios_output_includes={
                    self._context.runtime_include('model/ios/model_provider_binding.h'),
                    self._context.runtime_objc_import('ModelProvider.h'),
                },
            )

            self._create_or_rule(
                rules={animated_model_provider_rule},
                android_output_includes={
                    self._context.runtime_include('model/android/animated_model_provider_binding.h')
                },
                ios_output_includes={
                    self._context.runtime_include('model/ios/animated_model_provider_binding.h'),
                    self._context.runtime_objc_import('AnimatedModelProvider.h'),
                },
            )

        # view_provider rule
        self._create_reg_exp_rule(
            RegExp.VIEW_PROVIDER,
            output_includes={self._context.runtime_include('ui_view/view_provider.h')},
            android_output_includes={self._context.runtime_include('ui_view/android/view_provider_binding.h')},
            ios_output_includes={
                self._context.runtime_include('ui_view/ios/view_provider_binding.h'),
                self._context.runtime_objc_import('ViewProvider.h'),
            },
        )

        # platform_view rule
        self._create_reg_exp_rule(
            RegExp.PLATFORM_VIEW,
            output_includes={self._context.runtime_include('view/platform_view.h')},
            android_output_includes={self._context.runtime_include('view/android/to_native.h')},
            ios_output_includes={
                self._context.runtime_include('view/ios/to_native.h'),
                self._context.runtime_objc_import('PlatformView_Fwd.h'),
                self._context.runtime_objc_import('PlatformView_Private.h'),
            },
        )

        # type_dictionary rule
        self._create_reg_exp_rule(
            RegExp.TYPE_DICTIONARY,
            output_includes={
                self._context.runtime_include('bindings/platform.h'),
                self._context.runtime_include('bindings/type_dictionary.h'),
            },
            android_output_includes={
                self._context.runtime_include('bindings/android/type_dictionary_to_native.h'),
                self._context.runtime_include('bindings/android/type_dictionary_to_platform.h'),
            },
            ios_output_includes={
                self._context.runtime_include('bindings/ios/type_dictionary_to_native.h'),
                self._context.runtime_include('bindings/ios/type_dictionary_to_platform.h'),
                self._context.runtime_objc_import('TypeDictionary.h'),
            },
        )

        # color rule
        self._create_reg_exp_rule(
            RegExp.COLOR,
            output_includes={self._context.runtime_include('color.h')},
            ios_output_includes={'UIKit/UIKit.h'},
        )

        # error rule
        self._create_reg_exp_rule(
            RegExp.ERROR,
            android_output_includes={self._context.runtime_include('android/make_error.h')},
            ios_output_includes={self._context.runtime_include('ios/make_error.h')},
        )

        navi_serialization = self._context.unit.enabled('MAPS_MOBILE_ENABLE_NAVI_SERIALIZATION')
        if navi_serialization:
            serialization_rule = self._create_or_rule(
                {self._create_reg_exp_rule(RegExp.SERIALIZABLE), self._create_reg_exp_rule(RegExp.NAVI_SERIALIZABLE)}
            )
        else:
            serialization_rule = self._create_reg_exp_rule(RegExp.SERIALIZABLE)

        self._serialization_rule = self._create_or_rule(
            rules={serialization_rule, variant_rule},
            output_includes={
                'boost/serialization/nvp.hpp',
                self._context.runtime_include('serialization/ptr.h'),
                self._context.runtime_include('bindings/internal/archive_generator.h'),
                self._context.runtime_include('bindings/internal/archive_reader.h'),
                self._context.runtime_include('bindings/internal/archive_writer.h'),
            },
        )

        # point serialization rule
        self._create_serialization_rule(point_rule, self._context.runtime_include('serialization/math.h'))

        # optional serialization rule
        self._create_serialization_rule(
            optional_rule, self._context.runtime_include('serialization/serialization_std.h')
        )

        # bridged struct serialization rule
        self._create_serialization_rule(bridged_struct_rule, self._context.runtime_include('bindings/export.h'))

        # time serialization rule
        self._create_serialization_rule(time_rule, self._context.runtime_include('serialization/chrono.h'))

        # string serialization rule
        self._create_serialization_rule(string_rule, 'boost/serialization/string.hpp')

        # bytes serialization rule
        self._create_serialization_rule(bytes_rule, 'boost/serialization/vector.hpp')

        # vector serialization rule
        self._create_serialization_rule(vector_rule, 'boost/serialization/vector.hpp')

        # dictionary serialization rule
        self._create_serialization_rule(dictionary_rule, 'boost/serialization/map.hpp')

        # variant serialization rule
        self._create_serialization_rule(variant_rule, variant_serialization_header)

    def _create_reg_exp_rule(self, reg_exp_string, *args, **kwargs):
        rule = RegExpRule(reg_exp_string, self._context, *args, **kwargs)
        self._rules.add(rule)
        return rule

    def _create_or_rule(self, rules, *args, **kwargs):
        rule = OrRule(rules, self._context, *args, **kwargs)
        self._rules.add(rule)
        return rule

    def _create_and_rule(self, rules, *args, **kwargs):
        rule = AndRule(rules, self._context, *args, **kwargs)
        self._rules.add(rule)
        return rule

    def _create_serialization_rule(self, additional_rule, serialization_header):
        rule = self._create_and_rule(
            rules={self._serialization_rule, additional_rule}, output_includes={serialization_header}
        )
        return rule

    def _split_and_remove_comments(self, input_file):
        inside_comment = False
        for line in input_file:
            current_line = line

            if inside_comment:
                closing_index = current_line.find("*/")
                if closing_index == -1:
                    continue
                current_line = current_line[closing_index + 2 :]
                inside_comment = False

            oneline_index = current_line.find("//")
            if oneline_index != -1:
                current_line = current_line[:oneline_index]

            opening_index = current_line.find("/*")
            while opening_index != -1:
                closing_index = current_line.find("*/")
                if closing_index == -1:
                    current_line = current_line[:opening_index]
                    inside_comment = True
                else:
                    current_line = current_line[:opening_index] + current_line[closing_index + 2 :]
                opening_index = current_line.find("/*")

            yield current_line

    def _should_add_to_output_includes(self, output_type):
        return self._context.add_generated_output_includes and self._context.output_name_generator.is_header(
            output_type
        )

    def process_files(self):
        for file_path in self._context.file_paths:
            self._context.output_name_generator = OutputNameGenerator(file_path, self._context.frameworks)

            for rule in self._rules:
                rule.start_file(file_path)

            with open(os.path.join(self._resolved_idl_dir, file_path), 'r') as f:
                for line in self._split_and_remove_comments(f):
                    for rule in self._rules:
                        rule.process_line(line)

            for rule in self._rules:
                for output_type in rule.get_output_types():
                    self._outputs.add(self._context.output_name_generator.generate_path(output_type))

                    if self._should_add_to_output_includes(output_type):
                        self._output_includes.add(self._context.output_name_generator.generate_name(output_type))

                self._output_includes.update(rule.get_output_includes())

    def get_outputs(self):
        return self._outputs

    def get_output_includes(self):
        return self._output_includes


def process_files(unit, file_paths):
    frameworks = _read_frameworks(unit)

    processor = IdlFileProcessor(unit, frameworks, file_paths)
    processor.process_files()
    outputs = processor.get_outputs()
    output_includes = processor.get_output_includes()

    return (outputs, output_includes)


def on_process_maps_mobile_idl(unit, *args):
    if not unit.enabled('MAPSMOBI_BUILD_TARGET'):
        return

    idl_files, kwds = sort_by_keywords({'FILTER': -1, 'FILTER_OUT': -1, 'GLOBAL_OUTPUTS': 0}, args)

    if len(idl_files) == 0:
        return

    is_global_outputs = 'GLOBAL_OUTPUTS' in kwds
    filter_in = kwds.get('FILTER', [])
    filter_out = kwds.get('FILTER_OUT', [])

    is_java_idl = unit.enabled("JAVA_IDL")
    is_dart_idl = unit.enabled("MAPKIT_DART_IDL")

    outputs, output_includes = process_files(unit, idl_files)

    if filter_in:
        outputs = [o for o in outputs if any([o.endswith(x) for x in filter_in])]
    if filter_out:
        outputs = [o for o in outputs if not any([o.endswith(x) for x in filter_out])]

    if len(outputs) == 0 and not is_java_idl:
        return

    base_out_dir = '${{ARCADIA_BUILD_ROOT}}/{}'.format(unit.path()[3:])
    unit.onaddincl(['GLOBAL', '{}/include'.format(base_out_dir)])

    include_dirs = _get_appended_values(unit, 'MAPKIT_IDL_INCLUDES')
    include_dirs.append(unit.path()[3:])

    framework_dir = unit.get('MAPKIT_IDL_FRAMEWORK')

    extra_inputs = unit.get('MAPKIT_IDL_EXTRA_INPUTS').split(' ')

    idl_args = []
    idl_args.extend(['OUT_BASE_ROOT', base_out_dir, 'OUT_ANDROID_ROOT', base_out_dir, 'OUT_IOS_ROOT', base_out_dir])

    if framework_dir:
        idl_args.extend(['FRAMEWORK_DIRS', framework_dir])

    if include_dirs:
        idl_args.append('INCLUDES')
        idl_args.extend(include_dirs)

    idl_args.append('IN')
    idl_args.extend(idl_files)
    if extra_inputs:
        idl_args.extend(extra_inputs)

    sorted_outputs = sorted(outputs)
    dart_outputs = []
    global_outputs = []

    if is_dart_idl:
        dart_outputs = [x for x in sorted_outputs if x.endswith('.dart')]
        if is_global_outputs:
            global_outputs = [x for x in sorted_outputs if x.endswith('.cpp')]
    elif not is_java_idl:
        if is_global_outputs:
            global_outputs = [x for x in sorted_outputs if x.endswith(('.cpp', '.m', '.mm'))]

    if not is_java_idl:
        non_global_outputs = sorted(set(outputs) - set(global_outputs) - set(dart_outputs))

        if global_outputs:
            idl_args.append('OUT_NOAUTO')
            idl_args.extend(global_outputs + dart_outputs)
            unit.onglobal_srcs(global_outputs)

        if non_global_outputs:
            idl_args.append('OUT')
            idl_args.extend(non_global_outputs)

        idl_args.append('OUTPUT_INCLUDES')
        idl_args.extend(sorted(set(output_includes) - set(outputs)))

    idl_args.append('IDLS')
    idl_args.extend(idl_files)

    if is_java_idl:
        unit.on_run_idl_tool_java(idl_args)
    else:
        unit.on_run_idl_tool(idl_args)
