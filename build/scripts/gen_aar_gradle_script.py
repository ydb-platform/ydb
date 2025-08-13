import argparse
import os
import sys
import tarfile

FLAT_DIRS_REPO_TEMPLATE = 'flatDir {{ dirs {dirs} }}\n'
MAVEN_REPO_TEMPLATE = '''
    maven {{
        url "{repo}"
        allowInsecureProtocol true
    }}\n
'''
KEYSTORE_TEMLATE = 'signingConfigs {{ debug {{ storeFile file("{keystore}") }} }}\n'

DO_NOT_STRIP = '''\
    packagingOptions {
        doNotStrip "*/arm64-v8a/*.so"
        doNotStrip "*/armeabi-v7a/*.so"
        doNotStrip "*/x86_64/*.so"
        doNotStrip "*/x86/*.so"
    }
'''

AAR_TEMPLATE = """\
apply plugin: 'com.android.library'
apply plugin: 'maven-publish'

ext.jniLibsDirs = [
    {jni_libs_dirs}
]

ext.resDirs = [
    {res_dirs}
]

ext.assetsDirs = [
    {assets_dirs}
]

ext.javaDirs = [
    {java_dirs}
]

def aidlDirs = [
    {aidl_dirs}
]

ext.bundles = [
    {bundles}
]

ext.androidArs = [
    {aars}
]

ext.compileOnlyAndroidArs = [
    {compile_only_aars}
]

def minVersion = 26
def compileVersion = 35
def targetVersion = 35
def buildVersion = '35.0.0'

import java.nio.file.Files
import java.nio.file.Paths
import java.util.regex.Matcher
import java.util.regex.Pattern
import java.util.zip.ZipFile

buildDir = "$projectDir/build"

if (!ext.has("packageSuffix"))
    ext.packageSuffix = ""

buildscript {{
    //repositories {{
    //     google()
    //     mavenCentral()
    //}}

    repositories {{
        {maven_repos}
    }}

    dependencies {{
        classpath 'com.android.tools.build:gradle:8.6.0'
    }}
}}

repositories {{
    //google()
    //mavenCentral()
    flatDir {{
        dirs System.env.PKG_ROOT + '/bundle'
    }}

    {flat_dirs_repo}

    {maven_repos}
}}

android {{
    namespace "{package_name}"

    {keystore}

    compileSdkVersion compileVersion
    buildToolsVersion buildVersion

    compileOptions {{
        sourceCompatibility JavaVersion.VERSION_21
        targetCompatibility JavaVersion.VERSION_21
    }}

    defaultConfig {{
        minSdkVersion minVersion
        targetSdkVersion targetVersion
        consumerProguardFiles '{proguard_rules}'
    }}

    sourceSets {{
        main  {{
            manifest.srcFile '{manifest}'
            jniLibs.srcDirs = jniLibsDirs
            res.srcDirs = resDirs
            assets.srcDirs = assetsDirs
            java.srcDirs = javaDirs
            aidl.srcDirs = aidlDirs
        }}
        // We don't use this feature, so we set it to nonexisting directory
        androidTest.setRoot('bundle/tests')
    }}


    {do_not_strip}

    dependencies {{
        for (bundle in bundles)
            implementation("$bundle") {{
                transitive = true
            }}
        for (bundle in androidArs)
            implementation(bundle) {{
                transitive = true
            }}
        for (bundle in compileOnlyAndroidArs)
            compileOnly(bundle)
    }}

    android.libraryVariants.all {{ variant ->
        def suffix = variant.buildType.name.capitalize()

        def sourcesJarTask = project.tasks.create(name: "sourcesJar${{suffix}}", type: Jar) {{
            archiveClassifier = 'sources'
            from android.sourceSets.main.java.srcDirs
            include '**/*.java'
            eachFile {{ fcd ->
                def segments = fcd.relativePath.segments
                if (segments[0] == 'impl') {{
                    fcd.relativePath = new RelativePath(true, segments.drop(1))
                }}
            }}
            includeEmptyDirs = false
        }}

        tasks["bundle${{suffix}}Aar"].dependsOn sourcesJarTask
        tasks["bundle${{suffix}}Aar"].dependsOn tasks["generatePomFileForReleasePublication"]
    }}
}}

publishing {{
    publications {{
        release(MavenPublication) {{
            def manifestFile = android.sourceSets.main.manifest.srcFile
            def manifestXml = new XmlParser().parse(manifestFile)
            def versionName = ''
            manifestXml.attributes().each {{ p ->
                if (p.key.localPart.equals("versionName")) {{
                    versionName = p.value
                }}
            }}

            groupId android.namespace.tokenize('.')[0..-2].join('.')
            version versionName
            artifact("$buildDir/outputs/aar/maps.mobile.aar")
            pom.withXml {{
                def dependenciesNode = asNode().appendNode('dependencies')

                configurations.implementation.allDependencies.each {{
                    def dependencyNode = dependenciesNode.appendNode('dependency')
                    dependencyNode.appendNode('groupId', it.group)
                    dependencyNode.appendNode('artifactId', it.name)
                    dependencyNode.appendNode('version', it.version)
                }}
            }}
        }}
    }}
}}

tasks.withType(GenerateMavenPom).all {{
    destination = layout.buildDirectory.file("$buildDir/${{rootProject.name}}$packageSuffix-pom.xml").get().asFile
}}

"""


def gen_build_script(args):
    def wrap(items):
        return ',\n    '.join('"{}"'.format(x) for x in items)

    bundles = []
    bundles_dirs = set(args.flat_repos)
    for bundle in args.bundles:
        dir_name, base_name = os.path.split(bundle)
        assert len(dir_name) > 0 and len(base_name) > 0
        name, ext = os.path.splitext(base_name)
        assert len(name) > 0 and ext == '.aar'
        bundles_dirs.add(dir_name)
        bundles.append('com.yandex:{}@aar'.format(name))

    if len(bundles_dirs) > 0:
        flat_dirs_repo = FLAT_DIRS_REPO_TEMPLATE.format(dirs=wrap(bundles_dirs))
    else:
        flat_dirs_repo = ''

    maven_repos = ''.join(MAVEN_REPO_TEMPLATE.format(repo=repo) for repo in args.maven_repos)

    if args.keystore:
        keystore = KEYSTORE_TEMLATE.format(keystore=args.keystore)
    else:
        keystore = ''

    if args.do_not_strip:
        do_not_strip = DO_NOT_STRIP
    else:
        do_not_strip = ''

    return AAR_TEMPLATE.format(
        aars=wrap(args.aars),
        compile_only_aars=wrap(args.compile_only_aars),
        aidl_dirs=wrap(args.aidl_dirs),
        assets_dirs=wrap(args.assets_dirs),
        bundles=wrap(bundles),
        do_not_strip=do_not_strip,
        flat_dirs_repo=flat_dirs_repo,
        java_dirs=wrap(args.java_dirs),
        jni_libs_dirs=wrap(args.jni_libs_dirs),
        keystore=keystore,
        manifest=args.manifest,
        maven_repos=maven_repos,
        proguard_rules=args.proguard_rules,
        res_dirs=wrap(args.res_dirs),
        package_name=args.package_name
    )


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--aars', nargs='*', default=[])
    parser.add_argument('--compile-only-aars', nargs='*', default=[])
    parser.add_argument('--aidl-dirs', nargs='*', default=[])
    parser.add_argument('--assets-dirs', nargs='*', default=[])
    parser.add_argument('--bundle-name', nargs='?', default='default-bundle-name')
    parser.add_argument('--bundles', nargs='*', default=[])
    parser.add_argument('--do-not-strip', action='store_true')
    parser.add_argument('--flat-repos', nargs='*', default=[])
    parser.add_argument('--java-dirs', nargs='*', default=[])
    parser.add_argument('--jni-libs-dirs', nargs='*', default=[])
    parser.add_argument('--keystore', default=None)
    parser.add_argument('--manifest', required=True)
    parser.add_argument('--maven-repos', nargs='*', default=[])
    parser.add_argument('--output-dir', required=True)
    parser.add_argument('--peers', nargs='*', default=[])
    parser.add_argument('--proguard-rules', nargs='?', default=None)
    parser.add_argument('--res-dirs', nargs='*', default=[])
    parser.add_argument('--package-name', default="com.yandex.maps.mobile")
    args = parser.parse_args()

    if args.proguard_rules is None:
        args.proguard_rules = os.path.join(args.output_dir, 'proguard-rules.txt')
        with open(args.proguard_rules, 'w') as f:
            pass

    for index, jsrc in enumerate(filter(lambda x: x.endswith('.jsrc'), args.peers)):
        jsrc_dir = os.path.join(args.output_dir, 'jsrc_{}'.format(str(index)))
        os.makedirs(jsrc_dir)
        with tarfile.open(jsrc, 'r') as tar:
            if sys.version_info >= (3, 12):
                tar.extractall(path=jsrc_dir, filter='data')
            else:
                tar.extractall(path=jsrc_dir)
            args.java_dirs.append(jsrc_dir)

    args.build_gradle = os.path.join(args.output_dir, 'build.gradle')
    args.settings_gradle = os.path.join(args.output_dir, 'settings.gradle')
    args.gradle_properties = os.path.join(args.output_dir, 'gradle.properties')

    content = gen_build_script(args)
    with open(args.build_gradle, 'w') as f:
        f.write(content)

    with open(args.gradle_properties, 'w') as f:
        f.write('android.useAndroidX=true')

    if args.bundle_name:
        with open(args.settings_gradle, 'w') as f:
            f.write('rootProject.name = "{}"'.format(args.bundle_name))
