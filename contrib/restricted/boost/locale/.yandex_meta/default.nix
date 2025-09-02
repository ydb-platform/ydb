self: super: with self; {
  boost_locale = stdenv.mkDerivation rec {
    pname = "boost_locale";
    version = "1.89.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "locale";
      rev = "boost-${version}";
      hash = "sha256-zAMYVfR1CL5mCgTYV9ujW0Fl+8tVndX/OK/ZVJImdQo=";
    };
  };
}
