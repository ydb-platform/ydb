self: super: with self; {
  boost_describe = stdenv.mkDerivation rec {
    pname = "boost_describe";
    version = "1.92.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "describe";
      rev = "boost-${version}";
      hash = "sha256-GOUwSDBRtaROE6UWrMOBVK0N8zLiqcrA97u9QOu3zn0=";
    };
  };
}
