self: super: with self; {
  boost_units = stdenv.mkDerivation rec {
    pname = "boost_units";
    version = "1.90.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "units";
      rev = "boost-${version}";
      hash = "sha256-fjiO9rnljUqM0LZtKbILxvj+UHL0eNGmLQeCn6yk/bo=";
    };
  };
}
