self: super: with self; {
  yamaker-morton-nd = stdenv.mkDerivation rec {
    name = "morton-nd";
    version = "4.0.0";

    src = fetchFromGitHub {
      owner = "morton-nd";
      repo = "morton-nd";
      rev = "v${version}";

      hash = "sha256-WoHlrSMQURwA67vSCpPl50OZIJrxpcFIlfkfnktaFKE=";
    };
  };
}
