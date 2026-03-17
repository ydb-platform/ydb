self: super: with self; {
  nsync = stdenv.mkDerivation rec {
    pname = "nsync";
    version = "1.30.0";

    src = fetchFromGitHub {
      owner = "google";
      repo = pname;
      rev = version;
      hash = "sha256-DKUKYQEnA79sznY4gLdE/8/Fs5VngC5o5Gkc3saS/bM=";
    };

    nativeBuildInputs = [ cmake ];
  };
}
