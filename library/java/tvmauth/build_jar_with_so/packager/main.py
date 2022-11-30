import os
import shutil
import sys
import zipfile


def main():
    jar_path = sys.argv[1]
    dlls_dir = sys.argv[2]
    assert os.path.exists(jar_path)
    assert os.path.isdir(dlls_dir)

    tmppath = jar_path + ".tmp"
    shutil.move(jar_path, tmppath)

    with zipfile.ZipFile(jar_path, "w") as zout:
        with zipfile.ZipFile(tmppath, "r") as zin:
            for item in zin.infolist():
                # to avoid duplicating of 'libtvmauth_java.so'
                if 'tvmauth_java' not in item.filename:
                    buffer = zin.read(item.filename)
                    zout.writestr(item, buffer)

        for root, _, files in os.walk(dlls_dir):
            for f in files:
                zout.write(os.path.join(root, f), f)

    os.remove(tmppath)
    shutil.rmtree(dlls_dir)


if __name__ == '__main__':
    main()
