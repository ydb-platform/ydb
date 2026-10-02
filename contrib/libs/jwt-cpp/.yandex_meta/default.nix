self: super: with self; {
  jwt-cpp = stdenv.mkDerivation rec {
    pname = "jwt-cpp";
    version = "0.4.0";

    src = fetchFromGitHub {
      owner = "Thalhammer";
      repo = "jwt-cpp";
      rev = "v${version}";
      sha256 = "1nmdyfv0qsam9qv43yrsryfywm4pfpwd4ind4yqdcnq0pv0qlss3";
    };
 };
}
