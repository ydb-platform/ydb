self: super: with self; {
  boost_date_time = stdenv.mkDerivation rec {
    pname = "boost_date_time";
    version = "1.92.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "date_time";
      rev = "boost-${version}";
      hash = "sha256-N1rifX8ANF0Ewa8pNTMXrneQdTiazkUzoP5N6uF1zNo=";
    };
  };
}
