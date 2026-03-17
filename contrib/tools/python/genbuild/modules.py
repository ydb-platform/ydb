def list_split(iterable, separator):
    result = []
    for value in iterable:
        if value == separator and result:
            yield result
            result = []

        result.append(value)

    if result:
        yield result


def dict_split(iterable, separators):
    result = { }
    key = None

    for value in iterable:
        if value in separators:
            key = value
        elif key:
            if key not in result:
                result[key] = [ value ]
            else:
                result[key].append(value)

    return result


def parse_tokens(tokens):
    for module_tokens in list_split(tokens, "MODULE"):
        module = dict_split(module_tokens, [
            "MODULE", "FILES", "FLAGS", "LIBS", "INCLUDE", "PEERDIR", "PLATFORM", "PLATFORM_VER"
        ])
        (m_name,) = module["MODULE"]
        m_files = module["FILES"]
        m_flags = module.get("FLAGS", [])
        m_libs = module.get("LIBS", [])
        m_include = module.get("INCLUDE", [])
        m_peerdir = module.get("PEERDIR", [])
        (m_platform,) = module.get("PLATFORM", [""])
        m_platform_ver = " ".join(module.get("PLATFORM_VER", []))

        yield m_name, m_files, m_flags, m_libs, m_include, m_peerdir, m_platform, m_platform_ver


def load_modules_file(input_file):
    with open(input_file) as file_modules_list:
        tokens = file_modules_list.read().split()

        modules = list(parse_tokens(tokens))

        platform_modules = { }
        for name, files, flags, libs, include, peerdir, platform, platform_ver in modules:
            if platform not in platform_modules:
                platform_modules[platform] = { }

            if platform_ver not in platform_modules[platform]:
                platform_modules[platform][platform_ver] = [], [], [], [], [], []

            m_names, m_files, m_flags, m_libs, m_includes, m_peerdirs = platform_modules[platform][platform_ver]

            m_names.append(name)
            m_files.extend(files)

            if flags: m_flags.append((flags, files))

            m_libs.extend(libs)
            m_includes.extend(include)
            m_peerdirs.extend(peerdir)

        return sorted((m_platform, [(m_version, module) for m_version, module in m_versions.iteritems()])
                      for m_platform, m_versions in platform_modules.iteritems())


def write_modules_cmake(output_file, platform_modules):
    with open(output_file, "w") as output:
        output.write("# Generated automatically by ../modules.py\n\n")

        for platform, platform_versions in platform_modules:
            if platform:
                output.write("IF(%s)\n" % platform)

            for platform_version, modules in platform_versions:
                platform_indent = "    " if platform else ""

                if platform_version:
                    output.write("%sIF(%s_VER %s)\n" % (platform_indent, platform, platform_version))

                indent = platform_indent + ("    " if platform_version else "")

                m_names, m_files, m_flags, m_libs, m_includes, m_peerdirs = modules

                m_files = sorted(set(m_files))
                m_libs = sorted(set(m_libs))
                m_includes = sorted(set(m_includes))
                m_peerdirs = sorted(set(m_peerdirs))

                for m_include in m_includes:
                    output.write("%sADDINCL(%s)\n" % (indent, m_include))

                for m_peerdir in m_peerdirs:
                    output.write("%sPEERDIR(%s)\n" % (indent, m_peerdir))

                output.write("%sSRCS(%s)\n" % (indent, ("\n%s     " % indent).join(m_files)))

                for module_libs in m_libs:
                    output.write("%sSET_APPEND(OBJADDE %s)\n" % (indent, module_libs))

                for module_flags, module_files in m_flags:
                    properties_files = " ".join(root_prefix + filename for filename in module_files)
                    properties_flags = " ".join(module_flags)

                    properties = "\n%sCFLAGS(%s)\n"
                    output.write(properties % (indent, properties_flags))

                if platform_version:
                    output.write("%sENDIF(%s_VER %s)\n" % (platform_indent, platform, platform_version))

            if platform:
                output.write("ENDIF(%s)\n" % platform)

            output.write("\n")



def write_modules_config(init_filename, map_filename, platform_modules):
    with open(init_filename, "w") as init_file, open(map_filename, "w") as map_file:
        file_header = "/* Generated automatically by ../modules.py */\n\n#include \"config_platform.h\"\n\n"
        init_file.write(file_header)
        map_file.write(file_header)

        for platform, platform_versions in platform_modules:
            if platform:
                file_if = "#ifdef _%s_\n" % platform
                init_file.write(file_if)
                map_file.write(file_if)

            for platform_version, module in platform_versions:
                m_names, m_files, m_flags, m_libs, m_includes, m_peerdirs = module

                m_names = sorted(set(m_names))

                for module_name in m_names:
                    init_file.write("extern void init%s(void);\n" % module_name)
                    map_file.write("{\"%s\", init%s},\n" % (module_name, module_name))

            if platform:
                file_endif = "#endif\n"
                init_file.write(file_endif)
                map_file.write(file_endif)

            init_file.write("\n")
            map_file.write("\n")

import sys
filename, list_file, output_file, config_init_file, config_map_file, root_prefix = sys.argv

modules = load_modules_file(list_file)
write_modules_cmake(output_file, modules)
write_modules_config(config_init_file, config_map_file, modules)
