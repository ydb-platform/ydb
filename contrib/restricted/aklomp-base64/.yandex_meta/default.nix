self: super: with self; {
  aklomp_base64 = stdenv.mkDerivation rec {
    name = "aklomp-base64";
    version = "0.5.2";
    src = fetchFromGitHub {
      owner = "aklomp";
      repo = "base64";
      rev = "e77bd70bdd860c52c561568cffb251d88bba064c";
      sha256 = "sha256-rJggyY125gjFAMJFa8Ui53cbyk4B/Qat3Y61+MAq70Y=";
    };

    nativeBuildInputs = [
      cmake
      ninja
    ];

  };
}
