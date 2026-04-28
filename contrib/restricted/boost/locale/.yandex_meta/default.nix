self: super: with self; {
  boost_locale = stdenv.mkDerivation rec {
    pname = "boost_locale";
    version = "1.90.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "locale";
      rev = "boost-${version}";
      hash = "sha256-upelF8VheXf/1JufbNpb/9Pratl6gGXbHwoatJNNJh8=";
    };
  };
}
