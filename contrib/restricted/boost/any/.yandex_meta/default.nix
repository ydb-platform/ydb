self: super: with self; {
  boost_any = stdenv.mkDerivation rec {
    pname = "boost_any";
    version = "1.88.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "any";
      rev = "boost-${version}";
      hash = "sha256-hcJ7q6nkExmg8i1cjPA0eqrVJ5u4vluvtdrUwzUV+5c=";
    };
  };
}
