import sys
import platform
import subprocess
from argparse import ArgumentParser


def main():
    parser = ArgumentParser(description='YDB Distributed Storage Administration Tool')

    # common options
    parser.add_argument('--install', '-i', action='store_true', help='Install ydb-dstool')
    
    args = parser.parse_args()

    if args.install:
        if platform.system() in ['Linux', 'Darwin']:  # Darwin - это macOS
            command = "curl -sSL 'https://install.ydb.tech/dstool' | bash"
        elif platform.system() == 'Windows':
            command = "iex (New-Object System.Net.WebClient).DownloadString('https://install.ydb.tech/dstool-windows')"
        else:
            print("Unsupported OS")
            return
        
        print(f"Executing installation command: {command}")
        result = subprocess.run(command, shell=True)
        if result.returncode != 0:
            print("Installation failed. Please check the error messages above.")
            return
        print("Installation completed successfully.")
        print("You may now uninstall the pypi package by running:")
        print("pip uninstall ydb-dstool")
        
        return
    
    print("The distribution method for ydb-dstool has changed.")
    
    if platform.system() not in ['Linux', 'Darwin', 'Windows']:
        print("Unsupported operating system. Please refer to the documentation for installation instructions.")
        return
    
    if platform.system() in ['Linux', 'Darwin']:
        print("To install ydb-dstool, please run the following command:")
        print(f"curl -sSL 'https://install.ydb.tech/dstool' | bash")
    elif platform.system() == 'Windows':
        print("To install ydb-dstool, please run the following command:")
        print(f"iex (New-Object System.Net.WebClient).DownloadString('https://install.ydb.tech/dstool-windows')")

    print()
    print("Before installing, you should uninstall the pypi package:")
    print("pip uninstall ydb-dstool")

if __name__ == '__main__':
    main()
