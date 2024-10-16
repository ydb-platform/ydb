self: super: with self; rec {
  pname = "uriparser";
  version = "0.9.8";

  src = fetchFromGitHub {
    owner = pname;
    repo = pname;
    rev = "${pname}-${version}";
    hash = "sha256-U/AM8ULKGDfL3t+VUcn+t9sn4z/uc+pDjf2HHwHLI2M=";
  };

  patches = [];
}
