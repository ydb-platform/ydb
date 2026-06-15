self: super: with self; {
  boost_container = stdenv.mkDerivation rec {
    pname = "boost_container";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "container";
      rev = "boost-${version}";
      hash = "sha256-lk+hPbbXVcWh6Mnin40Q5KPwb/CvBillxZYsR7JXn7w=";
    };
  };
}
