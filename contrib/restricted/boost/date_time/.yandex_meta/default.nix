self: super: with self; {
  boost_date_time = stdenv.mkDerivation rec {
    pname = "boost_date_time";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "date_time";
      rev = "boost-${version}";
      hash = "sha256-I1Oo/3MyHnsBSEjQ7OieCVn3B0ImSYDaPqoMMW0kPjg=";
    };
  };
}
