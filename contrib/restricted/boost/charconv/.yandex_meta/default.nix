self: super: with self; {
  boost_charconv = stdenv.mkDerivation rec {
    pname = "boost_charconv";
    version = "1.89.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "charconv";
      rev = "boost-${version}";
      hash = "sha256-/2gydYQoeCVqoy21f4hgUZ6YqgSVDuyEW8qKyYJrN0M=";
    };
  };
}
