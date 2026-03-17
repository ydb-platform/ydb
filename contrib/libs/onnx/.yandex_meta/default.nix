self: super: with self; {
  onnx = stdenv.mkDerivation rec {

    name = "onnx";
    version = "1.18.0";

    src = fetchFromGitHub {
      owner = "onnx";
      repo = "onnx";
      rev = "v${version}";
      hash = "sha256-UhtF+CWuyv5/Pq/5agLL4Y95YNP63W2BraprhRqJOag=";
    };

    nativeBuildInputs = (with python3Packages; [
      pip
      pybind11
      setuptools
      wheel
    ]) ++ [ cmake python3 protobuf ];

    postUnpack = ''(
      cd source
    )'';
  };
}
