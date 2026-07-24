self: super: with self; {
  wavm = stdenv.mkDerivation rec {
    name = "wavm";
    version = "2022-05-14";
    revision = "3f9a150cac7faf28eab357a2c5b83d2ec740c7d9";

    src = fetchFromGitHub {
      owner = "WAVM";
      repo = "WAVM";
      rev = "${revision}";

      hash = "sha256-SHz+oOOkwvVZucJYFSyZc3MnOAy1VatspmZmOAXYAWA=";
    };

    nativeBuildInputs = [ cmake ninja llvm ];
    patches = [
        ./0001-remove-object-cache.patch
        ./0002-initialize-only-native-llvm-target.patch
    ];
  };
}
