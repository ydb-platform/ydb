self: super: with self; {
  boost_utility = stdenv.mkDerivation rec {
    pname = "boost_utility";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "utility";
      rev = "boost-${version}";
      hash = "sha256-514BYSu5ZrDNmqMOITsxU9WLqhpGXH3uac9kWxUx8TA=";
    };
  };
}
