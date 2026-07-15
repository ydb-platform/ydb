self: super: with self; {
  yamaker-emscripten = llvmPackages_20.libcxxStdenv.mkDerivation rec {
    pname = "yamaker-emscripten";
    version = "4.0.12";

    src = fetchFromGitHub {
      owner = "emscripten-core";
      repo = "emscripten";
      rev = version;
      hash = "sha256-MwCUilfyum1yJb6nHEViYiYWufXlz2+krHZmXw2NAck=";
    };

    llvmEnv = symlinkJoin {
      name = "emscripten-llvm-${version}";
      paths = with llvmPackages_20; [
        clang-unwrapped
        (lib.getLib clang-unwrapped)
        lld
        llvm
      ];
    };

    patches = [
      ./0001-emulate-clang-sysroot-include-logic.patch
    ];

    nativeBuildInputs = [ which mktemp python3 wget gnutar nodejs ];
  };
}
