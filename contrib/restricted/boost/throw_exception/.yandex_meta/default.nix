self: super: with self; {
  boost_throw_exception = stdenv.mkDerivation rec {
    pname = "boost_throw_exception";
    version = "1.90.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "throw_exception";
      rev = "boost-${version}";
      hash = "sha256-kqpcApJQv9vbhRF0YyVuHJtv6J+hvlve9KuzWHyxLT8=";
    };
  };
}
