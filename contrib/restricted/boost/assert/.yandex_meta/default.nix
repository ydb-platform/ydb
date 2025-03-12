self: super: with self; {
  boost_assert = stdenv.mkDerivation rec {
    pname = "boost_assert";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "assert";
      rev = "boost-${version}";
      hash = "sha256-gxA+/HvbCn2Eq5q+Jp8aX58N0f9TJdNPd4ZJfWKP3sI=";
    };
  };
}
