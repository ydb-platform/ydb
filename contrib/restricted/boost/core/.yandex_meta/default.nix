self: super: with self; {
  boost_core = stdenv.mkDerivation rec {
    pname = "boost_core";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "core";
      rev = "boost-${version}";
      hash = "sha256-Y9xNI7tI6LIW4WuHIKcR7Ippo7IVal4D9vr5q7MiUY8=";
    };
  };
}
