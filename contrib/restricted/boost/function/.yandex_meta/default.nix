self: super: with self; {
  boost_function = stdenv.mkDerivation rec {
    pname = "boost_function";
    version = "1.88.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "function";
      rev = "boost-${version}";
      hash = "sha256-ZfEd50J1Lq1W2X90QYLgkudvofmNHlwgVmIgpQWqdtE=";
    };
  };
}
