self: super: with self; {
  boost_math = stdenv.mkDerivation rec {
    pname = "boost_math";
    version = "1.92.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "math";
      rev = "boost-${version}";
      hash = "sha256-d5a1tCuqovcvYPUQcd+avTpAGLhH7In42MAjs/YSpDQ=";
    };
  };
}
